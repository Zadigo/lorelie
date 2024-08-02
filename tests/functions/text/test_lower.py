# import unittest

# from lorelie.database.base import Database
# from lorelie.database.functions.text import Lower
# from lorelie.fields.base import CharField, IntegerField
# from lorelie.tables import Table


# def create_mock_data():
#     table = Table('celebrities', fields=[
#         CharField('name'),
#         IntegerField('height', default=150, min_value=150)
#     ])
#     db = Database(table)
#     db.migrate()
#     db.celebrities.objects.( name='Julie', height=176)
#     db.celebrities.objects.( name='Pauline', height=190)
#     db.celebrities.objects.( name='Aurélie', height=210)
#     return db, table


# class TestLower(unittest.TestCase):
#     def setUp(self):
#         db, table = create_mock_data()
#         self.db = db
#         self.table = table

#     def test_structure(self):
#         instance = Lower('name')
#         expected_sql = instance.as_sql(self.table.backend)
#         self.assertEqual('lower(name)', expected_sql)

#     def test_transform(self):
#         qs = self.db.celebrities.objects.annotate(
#             'celebrities',
#             lowered=Lower('name')
#         )
#         self.assertListEqual(
#             qs.values('lowered'),
#             [
#                 {'lowered': 'julie'},
#                 {'lowered': 'pauline'},
#                 {'lowered': 'aurélie'}
#             ]
#         )


# if __name__ == '__main__':
#     unittest.main()
