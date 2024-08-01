# import unittest

# from lorelie.database.base import Database
# from lorelie.database.functions.text import Upper
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


# class TestUpper(unittest.TestCase):
#     def setUp(self):
#         db, table = create_mock_data()
#         self.db = db
#         self.table = table

#     def test_structure(self):
#         instance = Upper('name')
#         expected_sql = instance.as_sql(self.table.backend)
#         self.assertEqual('upper(name)', expected_sql)

#     def test_transform(self):
#         qs = self.db.objects.annotate(
#             'celebrities',
#             uppered=Upper('name')
#         )
#         self.assertListEqual(
#             qs.values('uppered'),
#             [
#                 {'uppered': 'JULIE'},
#                 {'uppered': 'PAULINE'},
#                 {'uppered': 'AURéLIE'}
#             ]
#         )


# if __name__ == '__main__':
#     unittest.main()
