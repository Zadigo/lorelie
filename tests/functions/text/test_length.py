# import unittest

# from lorelie.database.base import Database
# from lorelie.database.functions.text import Length
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


# class TestLength(unittest.TestCase):
#     def setUp(self):
#         db, table = create_mock_data()
#         self.db = db
#         self.table = table

#     def test_structure(self):
#         instance = Length('name')
#         expected_sql = instance.as_sql(self.table.backend)
#         self.assertEqual('length(name)', expected_sql)

#     def test_transform(self):
#         qs = self.db.celebrities.objects.annotate(
#             'celebrities',
#             name_length=Length('name')
#         )
#         self.assertListEqual(
#             qs.values('name_length'),
#             [
#                 {'name_length': 5},
#                 {'name_length': 7},
#                 {'name_length': 7}
#             ]
#         )


# if __name__ == '__main__':
#     unittest.main()
