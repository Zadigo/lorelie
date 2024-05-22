import unittest

from lorelie.constraints import CheckConstraint
from lorelie.database.base import Database
from lorelie.exceptions import (ConnectionExistsError, FieldExistsError,
                                ImproperlyConfiguredError)
from lorelie.expressions import Q
from lorelie.fields.base import CharField, Field, IntegerField
from lorelie.tables import Table
from lorelie.test.testcases import LorelieTestCase


class TestTable(LorelieTestCase):
    def test_structure(self):
        table = self.create_table()
        self.assertTrue(table == table)
        self.assertTrue('name' in table)

        with self.assertRaises(ValueError):
            table.validate_table_name('objects')

        table.backend = self.create_connection()
        talent = IntegerField('talent')

        with self.assertRaises(ValueError):
            table._add_field('other', talent)

        table._add_field('talent', talent)
        self.assertIn('talent', table)

    def test_cannot_load_connection(self):
        table = self.create_table()
        with self.assertRaises(ConnectionExistsError):
            table.load_current_connection()

    def test_fields_map(self):
        table = self.create_table()
        self.assertIn('id', table.fields_map)
        self.assertIsInstance(table.fields_map, dict)
        self.assertTrue(table.has_field('id'))
        self.assertTrue(table.has_field('name'))

        field = table.get_field('name')
        self.assertIsInstance(field, Field)
        self.assertIsInstance(field, CharField)

        with self.assertRaises(KeyError):
            table.get_field('firstname')

        # self.assertIsNone(field.table)

    def test_field_types(self):
        table = self.create_table()
        self.assertDictEqual(
            table.field_types,
            {'name': 'text', 'height': 'integer'}
        )

        # When we have mixed type fields, we have to determine
        # how to resovle the resulting value from these mixed
        # elements so that we can get a consistent result
        state = table.compare_field_types(
            CharField('firstname'),
            CharField('lastname')
        )
        self.assertFalse(state)

        state = table.compare_field_types(
            CharField('firstname'),
            IntegerField('age')
        )
        self.assertTrue(state)

    def test_build_field_parameters(self):
        # The field parameters are the parameters that
        # are used by "create table" when creating a
        # new table in a database
        table = self.create_table()

        # We should not be able to use a table outside
        # of the Database class without having called
        # prepare()
        with self.assertRaises((ImproperlyConfiguredError, AttributeError)):
            table.build_field_parameters()

        table.backend = self.create_connection()
        parameters = table.build_field_parameters()
        self.assertListEqual(
            parameters,
            [
                ['name', 'text', 'not null'],
                ['height', 'integer', 'default', 150, 'not null'],
                ['id', 'integer', 'primary key', 'autoincrement', 'not null']
            ]
        )

    def test_value_validation(self):
        table = self.create_table()
        table.backend = self.create_connection()
        items = table.validate_values(['name'], ['Kendall'])
        self.assertListEqual(items, ["'Kendall'"])

        with self.assertRaises((FieldExistsError, KeyError)):
            # Validating a field that does not exist
            # on the table
            table.validate_values(['age'], [23])

    def test_table_management(self):
        table = self.create_table()
        table.backend = self.create_connection()
        field_params = table.build_field_parameters()

        field_params = (
            table.backend.simple_join(params)
            for params in field_params
        )

        create_table_sql = table.create_table_sql(
            table.backend.comma_join(field_params)
        )
        self.assertListEqual(
            create_table_sql,
            ['create table if not exists celebrities (name text not null, height integer default 150 not null, id integer primary key autoincrement not null)']
        )

        drop_table_sql = table.drop_table_sql()
        self.assertListEqual(
            drop_table_sql,
            ['drop table if exists celebrities']
        )

    def test_table_level_constraints(self):
        constraint = CheckConstraint('my_constraint', Q(name__eq='Kendall'))
        table = Table(
            'my_table', 
            fields=[CharField('name')], 
            constraints=[constraint]
        )
        db = Database(table)
        table.prepare(db)


if __name__ == '__main__':
    unittest.main()
