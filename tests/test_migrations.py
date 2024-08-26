import unittest

from lorelie.database import migrations
from lorelie.database.base import Database
from lorelie.database.migrations import Migrations, Schema
from lorelie.exceptions import ImproperlyConfiguredError
from lorelie.fields.base import CharField
from lorelie.database.tables.base import Table
from lorelie.test.testcases import LorelieTestCase


class TestSchema(LorelieTestCase):
    def test_structure(self):
        db = self.create_database(using=self.create_full_table())
        schema = Schema(
            table=db.get_table('celebrities'),
            database=db
        )
        result = dict(schema)
        self.assertEqual(result['table'], 'celebrities')


class TestMigrations(LorelieTestCase):
    def test_structure(self):
        db = Database()
        migrations = Migrations(db)
        self.assertFalse(migrations.migrated)

        table = Table('products')

    def test_migrate(self):
        db = self.create_database(using=self.create_table())
        migrations = Migrations(db)
        migrations.migrate(db.table_instances)

    def test_make_migrations(self):
        db = self.create_database(using=self.create_full_table())
        migrations = Migrations(db)
        migrations.make_migrations(db.table_instances)


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
