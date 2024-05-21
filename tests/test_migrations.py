# import unittest

# from lorelie.database.base import Database
# from lorelie.database.migrations import Migrations
# from lorelie.fields.base import CharField
# from lorelie.tables import Table

# TEST_MIGRATION = {
#     "id": "fe81zef5",
#     "date": None,
#     "number": 1,
#     "tables": [
#         {
#             "name": "urls_seen",
#             "fields": [
#                 {
#                     "name": "id",
#                     "verbose_name": "id",
#                     "params": [
#                         "integer",
#                         "primary key"
#                     ]
#                 },
#                 {
#                     "name": "url",
#                     "verbose_name": None,
#                     "params": [
#                         "text",
#                         "null"
#                     ]
#                 },
#                 {
#                     "name": "date",
#                     "verbose_name": None,
#                     "params": [
#                         "text",
#                         "not null"
#                     ]
#                 }
#             ],
#             "indexes": [
#                 "url"
#             ]
#         }
#     ]
# }


# # table = Table('movies', 'celebrities', )

# class TestMigrations(unittest.TestCase):
#     def setUp(self):
#         db = Database()
#         self.instance = Migrations(db)

#     def test_is_migrated(self):
#         self.assertFalse(self.instance.migrated)

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
