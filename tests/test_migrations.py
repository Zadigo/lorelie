import unittest

from lorelie.database.base import Database
from lorelie.database.migrations import Migrations
from lorelie.exceptions import ImproperlyConfiguredError
from lorelie.fields.base import CharField
from lorelie.tables import Table
from lorelie.test.testcases import LorelieTestCase


class TestMigrations(LorelieTestCase):
    def test_structure(self):
        db = Database()
        migrations = Migrations(db)
        self.assertFalse(migrations.migrated)

        table = Table('products')
        with self.assertRaises(ImproperlyConfiguredError):
            migrations.check({'products': table})

    def test_check_function(self):
        # Check the migration class on
        # its very own by destructuring the
        # building process
        table = self.create_table()
        table.backend = self.create_connection()

        db = Database(table)
        migrations = Migrations(db)
        migrations.check({'celebrities': table})
        self.assertTrue(migrations.migrated)
        self.assertTrue(db.get_table('celebrities').is_prepared)



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
#         db.objects.all('celebrities')

#     def test_check_with_tables(self):
#         table = Table('celebrities', fields=[
#             CharField('name', null=True)
#         ])

#         db = Database(table)
#         db.migrate()

#         self.assertTrue(db.migrations.migrated)
#         db.objects.all('celebrities')

#         self.assertEqual(db.migrations.database_name, 'memory')
#         db.objects.all('lorelie_migrations')


# if __name__ == '__main__':
#     unittest.main()
