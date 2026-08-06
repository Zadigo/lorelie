import json
import pathlib
from unittest.mock import Mock, patch

from lorelie.database.base import Database
from lorelie.database.migrations.base import Migrations
from lorelie.database.migrations.validation import JsonMigrationSchema
from lorelie.test.testcases import LorelieTestCase

EMPTY_MIGRATION = {
    'id': '79f47320e4',
    'date': '2026-08-06T15:58:36.165224+00:00',
    'number': 1,
    'migrated': False,
    'schema': {}
}

@patch.object(Migrations, 'create_blank_migration')
class TestMigrationsExistingFile(LorelieTestCase):
    @classmethod
    def setUpClass(cls):
        cls.path = pathlib.Path(__file__).parent
        cls.mdb = Mock(
            spec=Database, 
            path=cls.path, 
            database_name='existingdb',
            in_memory=True
        )

    def test_structure(self, mcreate):
        # Skip the creation of the blank migration on the disk
        mcreate.return_value = JsonMigrationSchema(**EMPTY_MIGRATION)

        migrations = Migrations(self.mdb)
    
        self.assertFalse(migrations.for_update)
        self.assertFalse(migrations.migrated)
        self.assertTrue(len(migrations.existing_tables) == 0)
        


    # def test_migrate_creation_mode(self, mblank):
    #     # Creation mode: no existing tables
    #     with patch.object(json, 'dump') as mdump:
    #         data = self._load_file('migration_empty')

    #         schema = JsonMigrationSchema(**data)
    #         mblank.return_value = schema

    #         table1 = Table('company', fields=[CharField('name')])

    #         db = Database(table1)
    #         migration = Migrations(db)

    #         self.assertTrue(len(migration.existing_tables) == 0)

    #         state = migration.migrate(db.table_map, dry_run=True)
    #         self.assertTrue(state)

    # def test_migrate_table_deletion_mode(self, mblank):
    #     # Deletion mode: existing tables not in migration
    #     with patch.object(json, 'dump') as mdump:
    #         data = self._load_file('migration')

    #         schema = JsonMigrationSchema(**data)
    #         mblank.return_value = schema

    #         table1 = Table('company', fields=[CharField('name')])

    #         db = Database(table1)
    #         migration = Migrations(db)

    #         self.assertSetEqual(
    #             migration.existing_tables,
    #             {'company', 'migrations'}
    #         )

    #         # Expected: company table to be deleted
    #         table2 = Table('employee', fields=[CharField('name')])
    #         table2.backend = db.get_table('company').backend

    #         migration.migrate(
    #             {
    #                 'employee': table2
    #             },
    #             dry_run=True
    #         )
    #         state = migration.migrate({})
    #         self.assertTrue(state)

    # def test_migrate_addition_mode(self, mblank):
    #     # Addition mode: add new tables to existing migration
    #     with patch.object(json, 'dump') as mdump:
    #         data = self._load_file('migration')

    #         schema = JsonMigrationSchema(**data)
    #         mblank.return_value = schema

    #         table1 = Table('company', fields=[CharField('name')])

    #         db = Database(table1)
    #         migration = Migrations(db)

    #         self.assertSetEqual(
    #             migration.existing_tables,
    #             {'migrations', 'company'}
    #         )

    #         # Expected: employee table to be added
    #         table2 = Table('employee', fields=[CharField('name')])
    #         table2.backend = db.get_table('company').backend
    #         db._add_table(table2)

    #         state = migration.migrate(db.table_map, dry_run=True)
    #         self.assertTrue(state)

    # def test_migrate_index_check_mode(self, mblank):
    #     # Addition mode: migrate new index to a table that
    #     # does not have one
    #     with patch.object(json, 'dump') as mdump:
    #         data = self._load_file('migration')

    #         schema = JsonMigrationSchema(**data)
    #         schema.migrated = False
    #         mblank.return_value = schema

    #         table1 = Table(
    #             'company',
    #             fields=[CharField('name')],
    #             indexes=[Index('unique_name', ['name'])]
    #         )

    #         db = Database(table1)
    #         migration = Migrations(db)

    #         self.assertSetEqual(
    #             migration.existing_tables,
    #             {'migrations', 'company'}
    #         )

    #         self.assertTrue(
    #             len(
    #                 migration.JSON_MIGRATIONS_SCHEMA.get_table_indexes(
    #                     table1.name
    #                 )
    #             ) == 0
    #         )

    #         # First table has one single index
    #         state = migration.migrate(db.table_map, dry_run=True)
    #         self.assertTrue(state)

    #         migration.migrated = False

    #         # Remove the index and try again
    #         table1.indexes = []
    #         state = migration.migrate(db.table_map, dry_run=True)
    #         self.assertTrue(state)

    def test_migration_field_deletion_mode(self):
        pass

    def test_migration_constraint_deletion_mode(self):
        pass


class TestMigrationsNoneExistingFile(LorelieTestCase):
    @classmethod
    def setUpClass(cls):
        cls.path = pathlib.Path(__file__).parent
        cls.database_name = 'missingdb'
        cls.mdb = Mock(
            spec=Database, 
            path=cls.path, 
            database_name=cls.database_name, 
            in_memory=True
        )

    @classmethod
    def tearDownClass(cls):
        # Remove the test migration files if they exist so that we can
        # run these tests in the condition where the files do not exist
        cls.path.joinpath(f'{cls.database_name}_migrations.json').unlink(missing_ok=True)
        cls.path.joinpath(f'{cls.database_name}_migrations.sql').unlink(missing_ok=True)

    def test_structure(self):
        migrations = Migrations(self.mdb)
        
        self.assertFalse(migrations.for_update)
        self.assertFalse(migrations.migrated)
        self.assertTrue(len(migrations.existing_tables) == 0)
