import unittest

from lorelie.fields import base
from lorelie.backends import BaseRow, SQLiteBackend
from lorelie.fields.base import CharField, Field, IntegerField
from lorelie.tables import Table


# class TestTable(unittest.TestCase):
#     def setUp(self):
#         table = Table('celebrities', fields=[
#             CharField('firstname'),
#             CharField('lastname'),
#             IntegerField('followers')
#         ])
#         table.backend = SQLiteBackend()
#         table.prepare()
#         self.table = table

#     def test_fields_map(self):
#         self.assertIn('id', self.table.fields_map)
#         self.assertIsInstance(self.table.fields_map, dict)
#         self.assertTrue(self.table.has_field('id'))
#         self.assertTrue(self.table.has_field('firstname'))
#         self.assertIsInstance(self.table.backend, SQLiteBackend)

#     def test_get_field(self):
#         field = self.table.get_field('firstname')
#         self.assertIsInstance(field, Field)

#     def test_fields_constraints(self):
#         print(self.table.field_constraints)

#     def test_build_field_parameters(self):
#         parameters = self.table.build_field_parameters()
#         self.assertListEqual(
#             parameters,
#             [
#                 ['firstname', 'text', 'not null'],
#                 ['lastname', 'text', 'not null'],
#                 ['followers', 'integer', 'not null'],
#                 ['id', 'integer', 'primary key', 'autoincrement', 'not null']
#             ]
#         )

#     def test_sqls(self):
#         field_params = self.table.build_field_parameters()
#         field_params = (
#             self.table.backend.simple_join(params)
#             for params in field_params
#         )

#         create_table_sql = self.table.create_table_sql(
#             self.table.backend.comma_join(field_params)
#         )
#         self.assertListEqual(
#             create_table_sql,
#             ['create table if not exists celebrities (firstname text not null, lastname text not null, followers integer not null, id integer primary key autoincrement not null)']
#         )

#         drop_table_sql = self.table.drop_table_sql('')
#         self.assertListEqual(
#             drop_table_sql,
#             ['drop table if exists celebrities']
#         )


# if __name__ == '__main__':
#     unittest.main()

# FIXME: When using max_length constraint, all the fields
# share exactly the same MaxLengthConstraint which is not
# normal
# table = Table('celebrities', fields=[
#     CharField('firstname', max_length=150, unique=True),
#     CharField('lastname')
# ])
# table.backend = SQLiteBackend()
# table.prepare()
# field = table.get_field('firstname')
# print(table.fields_map)
