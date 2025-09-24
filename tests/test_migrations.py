import unittest

from lorelie.database.base import Database
from lorelie.database.migrations import Migrations
from lorelie.exceptions import ImproperlyConfiguredError
from lorelie.fields.base import CharField
from lorelie.database.tables.base import Table
from lorelie.test.testcases import LorelieTestCase


class TestMigrations(LorelieTestCase):
    def test_structure(self):
        db = Database()
        migrations = Migrations(db)
        self.assertFalse(migrations.migrated)

    def test_migrate(self):
        # Check the migration class on
        # its very own by destructuring the
        # building process
        table = self.create_table()
        table.backend = self.create_connection()

        db = Database(table)
        migrations = Migrations(db)
        migrations.migrate({'celebrities': table})

    def test_make_migrations(self):
        db = self.create_database(using=self.create_full_table())
        db.make_migrations()


#     @unittest.expectedFailure
#     def test_structure(self):
#         self.assertFalse(self.instance.file.exists())

#     def test_check_without_tables(self):
#         db = Database()
#         db.migrations.check({})
#         self.assertFalse(db.migrations.migrated)

#     @unittest.expectedFailure
#     def test_no_migrations_exists(self):
#         # Trying to call a query function on
#         # a none existing migration should
#         # raise MigrationsExistsError
#         table = Table('test_lorelie', fields=[
#             CharField('name', null=True)
#         ])
#         db = Database(table)
#         db.celebrities.objects.all('celebrities')

#     def test_check_with_tables(self):
#         table = Table('celebrities', fields=[
#             CharField('name', null=True)
#         ])

#         db = Database(table)
#         db.migrate()

#         self.assertTrue(db.migrations.migrated)
#         db.celebrities.objects.all('celebrities')

#         self.assertEqual(db.migrations.database_name, 'memory')
#         db.celebrities.objects.all('lorelie_migrations')
