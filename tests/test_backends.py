import dataclasses
import sqlite3
import unittest
from collections import defaultdict

from lorelie.backends import SQLiteBackend
from lorelie.database.base import Database
from lorelie.database.functions.aggregation import Count
from lorelie.fields.base import CharField
from lorelie.tables import Table
from lorelie.test.testcases import LorelieTestCase


class TestSQLiteBackend(LorelieTestCase):
    def test_in_memory(self):
        connection = self.create_connection()
        self.assertTrue(connection.database_name == ':memory:')

    def test_connection(self):
        connection = self.create_connection()
        self.assertIsInstance(connection.connection, sqlite3.Connection)

    def test_quote_value(self):
        connection = self.create_connection()
        values = ['Kendall', 'Great', 'Tall', "j'ai", "l'abbaye"]
        for value in values:
            with self.subTest(value=value):
                result = connection.quote_value(value)
                self.assertRegex(result, r"\'\w+\'")

    def test_comma_join(self):
        connection = self.create_connection()
        values = ['Kendall', 'Great', 'Tall']
        result = connection.comma_join(values)
        self.assertRegex(result, r'(\w+\,)+')
        self.assertEqual("Kendall, Great, Tall", result)

    def test_operator_join(self):
        connection = self.create_connection()
        values = ['Kendall', 'Great', 'Tall']
        result = connection.operator_join(values)
        self.assertRegex(result, r'^(\w+\sand\s)+(?:\w+)?$')
        self.assertEqual(result, "Kendall and Great and Tall")

    def test_simple_join(self):
        connection = self.create_connection()
        values = ['Kendall', 'Great', 'Tall']
        result = connection.simple_join(values)
        self.assertRegex(result, r'(\w+\s?)+')
        self.assertEqual(result, "Kendall Great Tall")

        result = connection.simple_join(values, space_characters=False)
        self.assertEqual(result, 'KendallGreatTall')

    def test_finalize_sql(self):
        connection = self.create_connection()
        result = connection.finalize_sql('sql statement')
        self.assertTrue(result.endswith(';'))

    def test_de_sqlize_statement(self):
        connection = self.create_connection()
        result = connection.de_sqlize_statement('sql statement;')
        self.assertFalse(result.endswith(';'))

    def test_wrap_parenthesis(self):
        connection = self.create_connection()
        result = connection.wrap_parenthentis('Kendall')
        self.assertRegex(result, r'\(Kendall\)')
        self.assertEqual(result, "(Kendall)")

    def test_build_alias(self):
        connection = self.create_connection()
        result = connection.build_alias('count(name)', 'name_count')
        self.assertRegex(result, r'^count\(name\) as name_count$')
        self.assertEqual(result, "count(name) as name_count")

    def test_quote_startswith(self):
        connection = self.create_connection()
        result = connection.quote_startswith('Ken')
        self.assertRegex(result, r'^\'Ken\%\'$')
        self.assertEqual(result, "Ken%")

    def test_quote_endswith(self):
        connection = self.create_connection()
        result = connection.quote_endswith('all')
        self.assertRegex(result, r'^\'\%all\'$')

    def test_quote_like(self):
        connection = self.create_connection()
        result = connection.quote_like('all')
        self.assertRegex(result, r'^\'\%all%\'$')

    def test_dict_to_sql(self):
        connection = self.create_connection()
        result = connection.dict_to_sql({'name__eq': 'Kendall'})
        self.assertIsInstance(result, (list, tuple))
        self.assertTupleEqual(result, (['name__eq'], ["'Kendall'"]))

#     def test_build_script(self):
#         pass

    def test_decompose_filters_from_string(self):
        connection = self.create_connection()
        filters = [
            # expression - expected result
            ('rowid=1', [('rowid', '=', '1')]),
            ('rowid__eq=1', [('rowid', '=', '1')]),
            ('rowid__lt=1', [('rowid', '<', '1')]),
            ('rowid__gt=1', [('rowid', '>', '1')]),
            ('rowid__lte=1', [('rowid', '<=', '1')]),
            ('rowid__gte=1', [('rowid', '>=', '1')]),
            ('rowid__contains=1', [('rowid', 'like', '1')]),
            ('rowid__startswith=1', [('rowid', 'startswith', '1')]),
            ('rowid__endswith=1', [('rowid', 'endswith', '1')]),
            # ('rowid__range=[1, 2]', [('rowid', 'between', '1')]),
            ('rowid__ne=1', [('rowid', '!=', '1')]),
            # ('rowid__in=1', [('rowid', 'in', '1')]),
            # ('rowid__isnull=True', [('rowid', 'isnull', 'True')]),
            # ('rowid__regex=True', [('rowid', 'regex', r'\w+')]),
        ]

        for lhv, rhv in filters:
            with self.subTest(lhv=lhv, rhv=rhv):
                result = connection.decompose_filters_from_string(lhv)
                self.assertIsInstance(result, list)
                self.assertListEqual(result, rhv)

    @unittest.expectedFailure
    def test_failed_decompose_filters_from_string(self):
        connection = self.create_connection()
        connection.decompose_filters_from_string('rowid__google=1')

    def test_decompose_foreign_key_filters_from_string(self):
        connection = self.create_connection()
        filters = [
            ('followers__id__eq=1', [('followers', 'id', '=', 1)]),
            ('followers__users__id__eq=1', [
             ('followers', 'users', 'id', '=', 1)]),
        ]
        for lhv, rhv in filters:
            with self.subTest(lhv=lhv, rhv=rhv):
                result = connection.decompose_filters_from_string(lhv)

    def test_decompose_filters_from_dict(self):
        connection = self.create_connection()
        filters = [
            ({'rowid__eq': 1}, [('rowid', '=', 1)]),
            ({'rowid__gt': 1}, [('rowid', '>', 1)]),
            ({'rowid__lt': 1}, [('rowid', '<', 1)]),
            ({'rowid__gte': 1}, [('rowid', '>=', 1)]),
            ({'rowid__lte': 1}, [('rowid', '<=', 1)])
        ]

        for lhv, rhv in filters:
            with self.subTest(lhv=lhv, rhv=rhv):
                result = connection.decompose_filters(**lhv)
                self.assertIsInstance(result, list)
                self.assertListEqual(result, rhv)

    @unittest.expectedFailure
    def test_failed_decompose_filters_from_dict(self):
        connection = self.create_connection()
        connection.decompose_filters_from_string({'rowid__google': 1})

    def test_build_filters(self):
        connection = self.create_connection()
        filters = [
            ([('rowid', '=', '1')], ["rowid = '1'"]),
            ([('rowid', '>', '1')], ["rowid > '1'"]),
            ([('rowid', '<', '1')], ["rowid < '1'"]),
            ([('rowid', '>=', '1')], ["rowid >= '1'"]),
            ([('rowid', '<=', '1')], ["rowid <= '1'"]),
        ]
        for lhv, rhv in filters:
            with self.subTest(lhv=lhv, rhv=rhv):
                result = connection.build_filters(lhv)
                self.assertIsInstance(result, list)
                self.assertListEqual(result, rhv)

    def test_decompose_foreign_key_filters(self):
        connection = self.create_connection()
        result = connection.decompose_filters(celebrities__name__eq='Kendall')
        self.assertListEqual(result, [('celebrities', 'name', '=', 'Kendall')])

        result = connection.decompose_filters_from_string(
            "celebrities__name__eq=Kendall"
        )
        self.assertListEqual(result, [('celebrities', 'name', '=', 'Kendall')])

#     @unittest.expectedFailure
#     def test_failed_build_filters(self):
#         # TODO: If an operator is not found we should not
#         # be able to build the filter
#         self.backend.build_filters([('rowid', '<=>', '1')])

    def test_build_annotation(self):
        connection = self.create_connection()
        connection.current_table = self.create_table()
        connection.current_table.backend = connection

        annotation_map = connection.build_annotation({'name': Count('name')})
        self.assertTrue(dataclasses.is_dataclass(annotation_map))

        self.assertDictEqual(
            annotation_map.sql_statements_dict,
            {'name': 'count(name)'}
        )
        self.assertIn('name', annotation_map.alias_fields)
        # self.assertIn('name', annotation_map.field_names)
        self.assertDictEqual(
            annotation_map.annotation_type_map,
            {'name': 'Count'}
        )

        self.assertListEqual(
            annotation_map.joined_final_sql_fields,
            ['count(name) as name']
        )
        self.assertTrue(annotation_map.requires_grouping)

    def test_build_dot_notation(self):
        connection = self.create_connection()
        values = [('followers', 'id', '=', '1')]
        result = connection.build_dot_notation(values)
        self.assertListEqual(result, ["followers.id='1'"])

#     def test_decompose_sql(self):
#         bits = self.backend.decompose_sql_statement(
#             'select *, name from celebrities'
#         )
#         self.assertIsInstance(bits, defaultdict)
#         select_map = bits['select']
#         column, values = select_map[0]
#         self.assertEqual(column, 'columns')
#         self.assertListEqual(values, ['*', 'name'])

#         bits = self.backend.decompose_sql_statement(
#             "select *, name from celebrities where name like 'kend%'"
#         )
#         print(bits)


# class TestCore(unittest.TestCase):
#     def setUp(self):
#         table = Table('celebrities')
#         db = Database(table)
#         db.migrate()
#         self.db = db

#     def test_list_table_columns_sql(self):
#         table = self.db.get_table('celebrities')
#         query = table.backend.list_table_columns_sql(table)
#         print(query)

#     @unittest.expectedFailure
#     def test_drop_indexes_sql(self):
#         table = self.db.get_table('celebrities')
#         sql = table.backend.drop_indexes_sql()

#     def test_create_table_fields(self):
#         table = self.db.get_table('celebrities')
#         table._add_field('firstname', CharField('firstname'))
#         table.backend.create_table_fields(table, ['firstname'])
#         self.db.objects.all('celebrities')

#     def test_list_tables_sql(self):
#         table = self.db.get_table('celebrities')
#         result = table.backend.list_tables_sql()
#         print(result)

#     def test_list_database_indexes(self):
#         table = self.db.get_table('celebrities')
#         result = table.backend.list_database_indexes()
#         print(result)

#     def test_list_table_indexes(self):
#         table = self.db.get_table('celebrities')
#         result = table.backend.list_table_indexes(table)
#         print(result)

#     def test_save_row_object(self):
#         row = self.db.objects.create('celebrities', firstname='Kendall')
#         row['firstname'] = 'Kylie'
#         table = self.db.get_table('celebrities')
#         query = table.backend.save_row_object(row)
#         print(query)

#     def test_save_row_object(self):
#         row = self.db.objects.create('celebrities', firstname='Kendall')
#         table = self.db.get_table('celebrities')
#         query = table.backend.delete_row_object(row)
#         print(query)


# if __name__ == '__main__':
#     unittest.main()
