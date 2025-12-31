
import json
import pathlib
from unittest.mock import patch

from lorelie.database.base import Database
from lorelie.database.migrations import JsonMigrationsSchema, Migrations
from lorelie.database.tables.base import Table
from lorelie.queries import Query
from lorelie.test.testcases import LorelieTestCase


class TestSchemaDataclass(LorelieTestCase):
    @classmethod
    def setUpClass(cls):
        path = pathlib.Path(__file__).parent / 'test_migration.json'
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


class TestMigrations(LorelieTestCase):
    def test_structure(self):
        db = Database()
        migrations = Migrations(db)
        self.assertFalse(migrations.migrated)

    def test_migrate_creation_mode(self):
        table = self.create_table()
        table.backend = self.create_connection()

        db = Database(table)
        migrations = Migrations(db)
        migrations.migrate({'celebrities': table})

    def test_migrate_table_deletion_mode(self):
        pass

    def test_migrate_index_deletion_mode(self):
        pass

    def test_migration_field_deletion_mode(self):
        pass

    def test_migration_constraint_deletion_mode(self):
        pass

    def test_write_sql_file(self):
        with patch.object(Migrations, 'read_content') as mocked_read_content:
            mocked_read_content.return_value = JsonMigrationsSchema(
                **EMPTY_MIGRATION
            )

            self.create_empty_database.migrations.write_to_sql_file(
                'CREATE TABLE test (id INTEGER);'
            )

    @patch('lorelie.backends.QuerySet', autospec=True)
    def test_check_fields(self, mqueryset):
        data = [{'name': 'id'}, {'name': 'name'}]
        _queryset = mqueryset.return_value
        type(_queryset).__iter__ = lambda _: iter(data)
        _queryset.load_cache.return_value = data

        table = self.create_table()
        backend = self.create_connection()
        table.backend = backend

        db = self.create_empty_database

        db.migrations.schemas[table.name] = Schema(table, db)
        with patch.object(Query, 'run') as mrun:
            mrun.return_value = True
            db.migrations.check_fields(table, backend)

    @patch('lorelie.database.migrations.connections', autospec=True)
    @patch('lorelie.database.migrations.SQLiteBackend', autospec=True)
    def test_migrate_with_blank_migration(self, msqlite, mconnections):
        _sqlite = msqlite.return_value

        _connections = mconnections.return_value
        _connections.get_last_connection.return_value = _sqlite
        _connections.list_database_indexes.return_value = []
        _connections.list_database_constraints.return_value = []

        # Simulate no existing tables
        _connections.list_all_tables.return_value = []

        table = self.create_table()
        table.backend = _sqlite
        table_instances = {'celebrities': table}
        db = self.create_empty_database

        with patch.object(Table, 'prepare') as mprepare:
            mprepare.return_value = None

            db.migrations.migrate(table_instances)

            self.assertTrue(db.migrations.migrated)
            self.assertTrue(len(db.migrations.tables_for_creation) == 0)
            self.assertTrue(len(db.migrations.tables_for_deletion) == 0)

    def test_migrate_with_existing_migration(self):
        table_instances = [self.create_table()]
