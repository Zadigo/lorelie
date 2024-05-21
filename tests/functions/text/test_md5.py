# import unittest

# from lorelie.database.base import Database
# from lorelie.database.functions.text import MD5Hash
# from lorelie.fields.base import CharField, IntegerField
# from lorelie.tables import Table


# def create_mock_data():
#     table = Table('celebrities', fields=[
#         CharField('name'),
#         IntegerField('height', default=150, min_value=150)
#     ])
#     db = Database(table)
#     db.migrate()
#     db.objects.create('celebrities', name='Julie', height=176)
#     db.objects.create('celebrities', name='Pauline', height=190)
#     db.objects.create('celebrities', name='Aurélie', height=210)
#     return db, table


# class TestMD5Hash(unittest.TestCase):
#     def setUp(self):
#         db, table = create_mock_data()
#         self.db = db
#         self.table = table

#     def test_structure(self):
#         instance = MD5Hash('name')
#         expected_sql = instance.as_sql(self.table.backend)
#         self.assertEqual('hash(name)', expected_sql)

#     def test_transform(self):
#         qs = self.db.objects.annotate(
#             'celebrities',
#             hash=MD5Hash('name')
#         )
#         self.assertListEqual(
#             qs.values('hash'),
#             [
#                 {'hash': '2964815d03a032c8ca37ac5d557647dd'},
#                 {'hash': 'e7d31845480111fdba3316129e166860'},
#                 {'hash': '7acdec8b181aa8cac7a5f454629c62b8'}
#             ]
#         )


# if __name__ == '__main__':
#     unittest.main()
