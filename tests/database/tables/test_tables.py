import random
import sqlite3
import unittest
from typing import Generator
from unittest.mock import MagicMock, Mock, patch

from dataclasses import dataclass

from lorelie.constraints import CheckConstraint
from lorelie.database.base import Database
from lorelie.queries import log_queries
from lorelie.database.functions.text import Lower
from lorelie.database.tables.base import Table
from lorelie.exceptions import (ConnectionExistsError, FieldExistsError,
                                ValidationError)
from lorelie.expressions import Q
from lorelie.fields.base import CharField, Field, IntegerField
from lorelie.test.testcases import LorelieTestCase


class TestTable(LorelieTestCase):
    @patch.object(sqlite3, 'connect')
    def test_structure(self, msqlite: Mock):
        table = self.create_table()

        self.assertTrue(table == table)
        self.assertTrue('name' in table)

        # Using a wrong name
        with self.assertRaises(ValueError):
            table.validate_table_name('objects')

        table.backend = self.create_connection()
        talent = IntegerField('talent')

        with self.assertRaises(ValueError):
            table._add_field('other', talent)

        mdatabase = MagicMock(spec=Database)
        mdatabase.relationships = {}

        table.prepare(mdatabase)
        table._add_field('talent', talent)

        self.assertIn('talent', table)
        self.assertTrue(len(table.field_names), 5)
        self.assertTrue(len(table.fields_map.keys()) > 0)
        self.assertTrue(len(table.auto_update_fields) > 0)
        self.assertTrue(len(table.field_names) > 0)

        msqlite.assert_called_once()

    def test_eq(self):
        pass

    def test_contains(self):
        pass

    def test_is_mixed_type_fields(self):
        field1 = CharField('name')
        field2 = IntegerField('age')
        result = Table.is_mixed_type_fields(field1, field2)
        self.assertTrue(result)

    def test_has_field(self):
        table = self.create_table()
        self.assertTrue(table.has_field('name'))
        self.assertFalse(table.has_field('age'))

    @patch.object(sqlite3, 'connect', autospec=True)
    def test_prepare(self, mconnection):
        table = self.create_table()
        db = Database(table)

        # Do not prepare the database because we
        # want to test the preparation of the table
        # outside of that database context
        table.prepare(db)
        self.assertTrue(table.is_prepared)

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

        self.assertIsNotNone(field.table)

    def test_field_types(self):
        table = self.create_table()
        self.assertDictEqual(
            table.field_types,
            {
                'name': 'text',
                'height': 'integer',
                'created_on': 'datetime',
                'id': 'integer'
            }
        )

        # When we have mixed type fields, we have to determine
        # how to resovle the resulting value from these mixed
        # elements so that we can get a consistent result
        state = table.is_mixed_type_fields(
            CharField('firstname'),
            CharField('lastname')
        )
        self.assertFalse(state)

        state = table.is_mixed_type_fields(
            CharField('firstname'),
            IntegerField('age')
        )
        self.assertTrue(state)

    @patch.object(sqlite3, 'connect')
    def test_build_all_field_parameters(self, mock_connect):
        # The field parameters are the parameters that
        # are used by "create table" when creating a
        # new table in a database
        table = self.create_table()

        # OLD: We should not be able to use a table outside
        # of the Database class without having called
        # prepare()
        # with self.assertRaises((ImproperlyConfiguredError, AttributeError)):
        #     table.build_all_field_parameters()

        table.backend = self.create_connection()
        parameters = list(table.build_all_field_parameters())

        self.assertListEqual(
            parameters,
            [
                ['name', 'text', 'not null'],
                ['height', 'integer', 'default', 152,
                    'not null', 'check(height>150)'],
                ['created_on', 'datetime', 'null'],
                ['id', 'integer', 'primary key', 'autoincrement', 'not null']
            ]
        )

    @patch.object(sqlite3, 'connect')
    def test_value_validation(self, mock_connect):
        table = self.create_table()
        table.backend = self.create_connection()

        items = table.validate_values(['name'], ['Kendall'])
        self.assertIsInstance(items, tuple)
        self.assertIsInstance(items[0], list)
        self.assertIsInstance(items[1], dict)
        self.assertTupleEqual(items, (["'Kendall'"], {'name': "'Kendall'"}))

        with self.assertRaises((FieldExistsError, KeyError)):
            # Validating a field that does not exist
            # on the table should raise an error
            table.validate_values(['age'], [23])

    @patch.object(sqlite3, 'connect')
    def test_validate_values_from_list(self, mock_connect):
        table = self.create_table()
        table.backend = self.create_connection()

        items = table.validate_values_from_list([{'name': 'Kendall'}])
        self.assertIsInstance(items, Generator)
        self.assertIsInstance(list(items)[0], tuple)

    @patch.object(sqlite3, 'connect')
    def test_table_management(self, mock_connect):
        table = self.create_table()
        table.backend = self.create_connection()
        field_params = table.build_all_field_parameters()

        field_params = (
            table.backend.simple_join(params)
            for params in field_params
        )

        create_table_sql = table.create_table_sql(
            table.backend.comma_join(field_params)
        )

        self.assertListEqual(
            create_table_sql,
            [
                'create table if not exists celebrities (name text not null, height integer default 152 '
                'not null check(height>150), created_on datetime null, id integer primary key autoincrement not null)'
            ]
        )

        drop_table_sql = table.drop_table_sql()
        self.assertListEqual(
            drop_table_sql,
            ['drop table if exists celebrities']
        )

    @patch.object(sqlite3, 'connect')
    def test_table_level_constraints(self, mock_connect):
        constraint = CheckConstraint('my_constraint', Q(name__eq='Kendall'))

        table = Table(
            'my_table',
            fields=[CharField('name')],
            constraints=[constraint]
        )

        db = Database(table)
        table.prepare(db)

        self.assertIn(constraint, table.table_constraints)

    def test_adding_an_existing_constraint_to_the_table(self):
        # TODO: Prevent the user from being able to create two
        # similar constraints on a given table
        constraint1 = CheckConstraint('my_constraint', Q(name__eq='Kendall'))
        constraint2 = CheckConstraint('my_constraint', Q(name__eq='Kendall'))

        table = Table(
            'my_table',
            fields=[CharField('name')],
            constraints=[constraint1, constraint2]
        )

        db = Database(table, log_queries=True)
        db.migrate()

    @unittest.skip
    @patch.object(sqlite3, 'connect')
    def test_database_field_creation_validation(self, mock_connect):
        # FIXME: What is this test supposed to check ???????

        db = self.create_database(
            using=self.create_complex_table(),
            log_queries=True
        )

        with self.assertRaises(sqlite3.IntegrityError):
            db.stars.objects.create(height=145)

        with self.assertRaises(sqlite3.IntegrityError):
            db.stars.objects.create(
                name='Kendall Jenner',
                height=0
            )

        with self.assertRaises(ValidationError):
            db.stars.objects.create(name='Taylor Swift')

        # TODO: Should not_null be False if we have a field
        # with a default value set
        db.stars.objects.create(
            name='Lucie Safarova',
            height=165
        )

        with self.assertRaises(sqlite3.IntegrityError):
            db.stars.objects.create(
                name='Lucie Safarova',
                height=165
            )

    def test_list_contains_table(self):
        table = self.create_table()
        tables = [table]
        self.assertIn(table, tables)
        self.assertIn(table.name, tables)

    def test_add_field(self):
        table = Table('celebrities')
        db = Database(table)
        table.prepare(db)

        field = CharField('name')
        table._add_field('name', field)

        # TODO: The index field does not
        # get updated correctly
        f1 = table.fields_map['id']
        f2 = table.fields_map['name']
        print(f1.index, f2.index)
        # self.assertEqual(f1.index, 1)

    def test_manager(self):
        table = self.create_table()
        db = Database(table, name='db_celebrities')
        db.migrate()

        # table.objects.create(name='Kendall Jenner', height=175)
        row = table.objects.get_or_create(
            create_defaults={'height': 171},
            name='Kylie Jenner'
        )
        self.assertIsNotNone(row)

        row = table.objects.get_or_create(
            create_defaults={'height': 175},
            name=f'Kendall {random.randint(1, 100)}'
        )

        row = table.objects.update_or_create(
            create_defaults={'height': 172},
            name='Kylie Jenner'
        )

        qs = table.objects.all()
        self.assertTrue(len(qs) > 0)

        row = table.objects.first()
        self.assertIsNotNone(row)

        row = table.objects.last()
        self.assertIsNotNone(row)

        row = table.objects.get(id=1)
        self.assertIsNotNone(row)

        row = table.objects.filter(name='Kendall Jenner')
        self.assertIsNotNone(row)

        qs = table.objects.exclude(id=1)
        self.assertTrue(len(qs) > 0)

        qs = table.objects.annotate(lowered_name=Lower('name'))
        self.assertTrue(len(qs) > 0)
        for row in qs:
            with self.subTest(row=row):
                self.assertIsNotNone(row.lowered_name)

        @dataclass
        class Celebrity:
            name: str
            height: int

        table.objects.bulk_create([
            Celebrity(name='Celebrity A', height=180),
            Celebrity(name='Celebrity B', height=165),
            Celebrity(name='Celebrity C', height=170),
        ])

        print(list(log_queries))
        # print(row)
        # print(qs)
