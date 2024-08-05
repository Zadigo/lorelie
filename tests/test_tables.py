import sqlite3

from lorelie.constraints import CheckConstraint
from lorelie.database.base import Database
from lorelie.database.manager import BackwardForeignTableManager, ForeignTablesManager, ForwardForeignTableManager
from lorelie.exceptions import (ConnectionExistsError, FieldExistsError,
                                ValidationError)
from lorelie.expressions import Q
from lorelie.fields.base import CharField, Field, IntegerField
from lorelie.tables import Column, RelationshipMap, Table
from lorelie.test.testcases import LorelieTestCase


class TestTable(LorelieTestCase):
    maxDiff = None

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

        self.assertIsNotNone(field.table)

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

    def test_build_all_field_parameters(self):
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
                    'not null', 'check(height>150)'
                 ],
                ['created_on', 'datetime', 'null'],
                ['id', 'integer', 'primary key', 'autoincrement', 'not null']
            ]
        )

    def test_value_validation(self):
        table = self.create_table()
        table.backend = self.create_connection()
        items = table.validate_values(['name'], ['Kendall'])
        self.assertTupleEqual(items, (["'Kendall'"], {'name': "'Kendall'"}))

        with self.assertRaises((FieldExistsError, KeyError)):
            # Validating a field that does not exist
            # on the table should raise an error
            table.validate_values(['age'], [23])

    def test_table_management(self):
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
                'create table if not exists celebrities (name text not null, ',
                'height integer default 152 not null check(height>150), created_on datetime null, '
                'id integer primary key autoincrement not null)'
            ]
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
        self.assertIn(constraint, table.table_constraints)

    def test_adding_an_existing_constraint_to_the_table(self):
        # TODO: Prevent the user from being able to create two
        # similar constraints in a given table
        constraint1 = CheckConstraint('my_constraint', Q(name__eq='Kendall'))
        constraint2 = CheckConstraint('my_constraint', Q(name__eq='Kendall'))

        table = Table(
            'my_table',
            fields=[CharField('name')],
            constraints=[constraint1, constraint2]
        )
        db = Database(table, log_queries=True)
        db.migrate()

    def test_database_field_creation_validation(self):
        db = self.create_database(
            using=self.create_complex_table(),
            log_queries=True
        )
        table = db.get_table('stars')

        with self.assertRaises(sqlite3.IntegrityError):
            table.objects.create(height=145)

        with self.assertRaises(sqlite3.IntegrityError):
            table.objects.create(name='Kendall Jenner', height=0)

        with self.assertRaises(ValidationError):
            table.objects.create(name='Taylor Swift')

        # TODO: Should not_null be False if we have a field
        # with a default value set
        table.objects.create(name='Lucie Safarova', height=165)
        with self.assertRaises(sqlite3.IntegrityError):
            table.objects.create(name='Lucie Safarova', height=165)

    def test_list_contains_table(self):
        table = self.create_table()
        tables = [table]
        self.assertIn(table, tables)
        self.assertIn(table.name, tables)

    def test_foreign_key_tables(self):
        db = self.create_foreign_key_database()

        t1 = db.get_table('celebrity')
        t2 = db.get_table('follower')

        # We can create on both tables
        t1.objects.create(name='Kendall', age=25)
        t2.objects.create(name='Julie Margot')

        # We can query both tables
        qs1 = t1.objects.all()
        qs2 = t2.objects.all()

        self.assertTrue(qs1.exists())
        self.assertTrue(qs2.exists())

        celebrity = qs1[0]
        self.assertEqual(qs1.first().linked_to_table, 'celebrity')
        self.assertIsInstance(
            celebrity.celebrity_follower_set,
            BackwardForeignTableManager
        )

        follower = qs2[0]
        self.assertEqual(qs2.first().linked_to_table, 'follower')
        self.assertIsInstance(
            follower.celebrity_follower,
            ForwardForeignTableManager
        )

        # We can expand the foreign field
        # t1.objects.filter(celebrities__number_of_followers=1000)


class TestRelationshipMap(LorelieTestCase):
    def test_structure(self):
        db = self.create_foreign_key_database()

        t1 = db.get_table('celebrity')
        t2 = db.get_table('follower')

        relationship_map = RelationshipMap(t1, t2)
        relationship_map.relationship_field = t2.get_field('celebrity')
        self.assertEqual(relationship_map.forward_field_name, 'celebrity')
        self.assertEqual(relationship_map.backward_field_name, 'celebrity_set')

        print(relationship_map.get_relationship_condition(t1))
        print(relationship_map.get_relationship_condition(t2))


class TestColumn(LorelieTestCase):
    def test_stucture(self):
        table = self.create_table()
        field = table.get_field('name')
        column = Column(field, table)

        self.assertEqual('name', column)
