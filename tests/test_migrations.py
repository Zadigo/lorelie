
import json
import pathlib
from unittest.mock import patch

from lorelie.database.base import Database
from lorelie.database.indexes import Index
from lorelie.database.migrations import JsonMigrationsSchema, Migrations
from lorelie.database.tables.base import Table
from lorelie.test.testcases import LorelieTestCase
from lorelie.fields.base import CharField


class TestSchemaDataclass(LorelieTestCase):
    @classmethod
    def setUpClass(cls):
        path = pathlib.Path(__file__).parent / 'migration.json'
        with open(path, 'r') as f:
            data = json.load(f)
        cls.TEST_MIGRATION = data

    def test_structure(self):
        instance = JsonMigrationsSchema(**self.TEST_MIGRATION)

        json_table = instance.get_table('company')
        self.assertIsInstance(json_table, dict)
        self.assertEqual(json_table.get('name'), 'company')

        json_fields = instance.get_table_fields('company')
        self.assertIsInstance(json_fields, list)
        self.assertIsInstance(json_fields[0], list)

        json_field = instance.get_table_field('company', 'name')
        field_type, name, params = json_field
        self.assertIsInstance(json_field, list)
        self.assertEqual(field_type, 'CharField')
        self.assertEqual(name, 'name')
        self.assertIsInstance(params, dict)


@patch.object(Migrations, 'blank_migration')
class TestMigrations(LorelieTestCase):
    @classmethod
    def setUpClass(cls):
        cls.path = pathlib.Path(__file__).parent

    def _load_file(self, name):
        with open(self.path / f'{name}.json', 'r') as f:
            return json.load(f)

    def test_structure(self, mblank):
        with patch.object(json, 'dump') as mdump:
            data = self._load_file('migration_empty')
            schema = JsonMigrationsSchema(**data)
            mblank.return_value = schema

            db = Database()
            migrations = Migrations(db)

            self.assertFalse(migrations.for_update)
            self.assertFalse(migrations.migrated)
            self.assertTrue(len(migrations.existing_tables) == 0)

    def test_migrate_creation_mode(self, mblank):
        # Creation mode: no existing tables
        with patch.object(json, 'dump') as mdump:
            data = self._load_file('migration_empty')

            schema = JsonMigrationsSchema(**data)
            mblank.return_value = schema

            table1 = Table('company', fields=[CharField('name')])

            db = Database(table1)
            migration = Migrations(db)

            self.assertTrue(len(migration.existing_tables) == 0)

            state = migration.migrate(db.table_map, dry_run=True)
            self.assertTrue(state)

    def test_migrate_table_deletion_mode(self, mblank):
        # Deletion mode: existing tables not in migration
        with patch.object(json, 'dump') as mdump:
            data = self._load_file('migration')

            schema = JsonMigrationsSchema(**data)
            mblank.return_value = schema

            table1 = Table('company', fields=[CharField('name')])

            db = Database(table1)
            migration = Migrations(db)

            self.assertSetEqual(
                migration.existing_tables,
                {'company', 'migrations'}
            )

            # Expected: company table to be deleted
            table2 = Table('employee', fields=[CharField('name')])
            table2.backend = db.get_table('company').backend

            migration.migrate(
                {
                    'employee': table2
                },
                dry_run=True
            )
            state = migration.migrate({})
            self.assertTrue(state)

    def test_migrate_addition_mode(self, mblank):
        # Addition mode: add new tables to existing migration
        with patch.object(json, 'dump') as mdump:
            data = self._load_file('migration')

            schema = JsonMigrationsSchema(**data)
            mblank.return_value = schema

            table1 = Table('company', fields=[CharField('name')])

            db = Database(table1)
            migration = Migrations(db)

            self.assertSetEqual(
                migration.existing_tables,
                {'migrations', 'company'}
            )

            # Expected: employee table to be added
            table2 = Table('employee', fields=[CharField('name')])
            table2.backend = db.get_table('company').backend
            db._add_table(table2)

            state = migration.migrate(db.table_map, dry_run=True)
            self.assertTrue(state)

    def test_migrate_index_check_mode(self, mblank):
        # Addition mode: migrate new index to a table that
        # does not have one
        with patch.object(json, 'dump') as mdump:
            data = self._load_file('migration')

            schema = JsonMigrationsSchema(**data)
            schema.migrated = False
            mblank.return_value = schema

            table1 = Table(
                'company',
                fields=[CharField('name')],
                indexes=[Index('unique_name', ['name'])]
            )

            db = Database(table1)
            migration = Migrations(db)

            self.assertSetEqual(
                migration.existing_tables,
                {'migrations', 'company'}
            )

            self.assertTrue(
                len(
                    migration.JSON_MIGRATIONS_SCHEMA.get_table_indexes(
                        table1.name
                    )
                ) == 0
            )

            # First table has one single index
            state = migration.migrate(db.table_map, dry_run=True)
            self.assertTrue(state)

            migration.migrated = False

            # Remove the index and try again
            table1.indexes = []
            state = migration.migrate(db.table_map, dry_run=True)
            self.assertTrue(state)

    def test_migration_field_deletion_mode(self):
        pass

    def test_migration_constraint_deletion_mode(self):
        pass
