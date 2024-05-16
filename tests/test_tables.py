import unittest

from lorelie.exceptions import ConnectionExistsError, FieldExistsError
from lorelie.fields import base
from lorelie.backends import BaseRow, SQLiteBackend
from lorelie.fields.base import CharField, Field, IntegerField
from lorelie.tables import Table


class TestTable(unittest.TestCase):
    def setUp(self):
        table = Table('celebrities', fields=[
            CharField('firstname'),
            CharField('lastname'),
            IntegerField('followers')
        ])
        self.table = table

    def test_cannot_load_connection(self):
        table = Table('celebrities')
        with self.assertRaises(ConnectionExistsError):
            table.load_current_connection()

    def test_fields_map(self):
        self.table.backend = SQLiteBackend()
        self.assertIn('id', self.table.fields_map)
        self.assertIsInstance(self.table.fields_map, dict)
        self.assertTrue(self.table.has_field('id'))
        self.assertTrue(self.table.has_field('firstname'))
        self.assertIsInstance(self.table.backend, SQLiteBackend)

    def test_get_field(self):
        field = self.table.get_field('firstname')
        self.assertIsInstance(field, Field)

    def test_fields_constraints(self):
        pass

    def test_field_types(self):
        self.assertDictEqual(
            self.table.field_types, 
            {'firstname': 'text', 'lastname': 'text', 'followers': 'integer'}
        )

        # When we have mixed type fields, we have to determine
        # how to resovle the resulting value from these mixed
        # elements so that we can get a consistent result
        state = self.table.compare_field_types(
            CharField('firstname'),
            CharField('lastname')
        )
        self.assertFalse(state)

        state = self.table.compare_field_types(
            CharField('firstname'),
            IntegerField('age')
        )
        self.assertTrue(state)

    def test_build_field_parameters(self):
        parameters = self.table.build_field_parameters()
        self.assertListEqual(
            parameters,
            [
                ['firstname', 'text', 'not null'],
                ['lastname', 'text', 'not null'],
                ['followers', 'integer', 'not null'],
                ['id', 'integer', 'primary key', 'autoincrement', 'not null']
            ]
        )

    def test_sqls(self):
        self.table.backend = SQLiteBackend()
        field_params = self.table.build_field_parameters()
        field_params = (
            self.table.backend.simple_join(params)
            for params in field_params
        )

        create_table_sql = self.table.create_table_sql(
            self.table.backend.comma_join(field_params)
        )
        self.assertListEqual(
            create_table_sql,
            ['create table if not exists celebrities (firstname text not null, lastname text not null, followers integer not null, id integer primary key autoincrement not null)']
        )

        drop_table_sql = self.table.drop_table_sql()
        self.assertListEqual(
            drop_table_sql,
            ['drop table if exists celebrities']
        )

    def test_value_validation(self):
        self.table.backend = SQLiteBackend()
        items = self.table.validate_values(['firstname'], ['Kendall'])
        self.assertListEqual(items, ["'Kendall'"])

        with self.assertRaises(FieldExistsError):
            # Field that does not exist on the table
            self.table.validate_values(['height'], [189])

    def test_equality(self):
        self.assertTrue(self.table == self.table)
        self.assertTrue('firstname' in self.table)
        self.assertTrue(self.table.has_field('firstname'))


if __name__ == '__main__':
    unittest.main()
