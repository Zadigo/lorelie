import dataclasses
import sqlite3
import unittest
from lorelie.backends import BaseRow, SQLiteBackend
from lorelie.aggregation import Count
from lorelie.fields.base import CharField
from lorelie.tables import Table
from lorelie.database.base import Database


class TestSQLiteBackend(unittest.TestCase):
    def setUp(self):
        self.backend = SQLiteBackend()

    def test_in_memory(self):
        self.assertTrue(self.backend.database_name == ':memory:')

    def test_connection(self):
        self.assertIsInstance(self.backend.connection, sqlite3.Connection)

    def test_quote_value(self):
        # TODO: Test with texts like: "j'ai", "l'abbye"
        values = ['Kendall', 'Great', 'Tall']
        for value in values:
            with self.subTest(value=value):
                result = self.backend.quote_value(value)
                self.assertRegex(
                    result,
                    r"\'\w+\'"
                )

    def test_comma_join(self):
        values = ['Kendall', 'Great', 'Tall']
        result = self.backend.comma_join(values)
        self.assertRegex(
            result,
            r'(\w+\,)+'
        )

    def test_operator_join(self):
        values = ['Kendall', 'Great', 'Tall']
        result = self.backend.operator_join(values)
        self.assertRegex(
            result,
            r'^(\w+\sand\s)+(?:\w+)?$'
        )

    def test_simple_join(self):
        values = ['Kendall', 'Great', 'Tall']
        result = self.backend.simple_join(values)
        self.assertRegex(
            result,
            r'(\w+\s?)+'
        )

        result = self.backend.simple_join(values, space_characters=False)
        self.assertRegex(
            result,
            r'KendallGreatTall'
        )

    def test_finalize_sql(self):
        result = self.backend.finalize_sql('sql statement')
        self.assertTrue(result.endswith(';'))

    def test_de_sqlize_statement(self):
        result = self.backend.de_sqlize_statement('sql statement;')
        self.assertTrue(not result.endswith(';'))

    def test_wrap_parenthesis(self):
        result = self.backend.wrap_parenthentis('Kendall')
        self.assertRegex(
            result,
            r'\(Kendall\)'
        )

    def test_build_alias(self):
        result = self.backend.build_alias('count(name)', 'name_count')
        self.assertRegex(
            result,
            r'^count\(name\) as name_count$'
        )

    def test_quote_startswith(self):
        result = self.backend.quote_startswith('Ken')
        self.assertRegex(
            result,
            r'^\'Ken\%\'$'
        )

    def test_quote_endswith(self):
        result = self.backend.quote_endswith('all')
        self.assertRegex(
            result,
            r'^\'\%all\'$'
        )

    def test_quote_like(self):
        result = self.backend.quote_like('all')
        self.assertRegex(
            result,
            r'^\'\%all%\'$'
        )

    def test_dict_to_sql(self):
        result = self.backend.dict_to_sql({'name__eq': 'Kendall'})
        self.assertIsInstance(result, (list, tuple))
        self.assertTupleEqual(
            result,
            (['name__eq'], ["'Kendall'"])
        )

    def test_build_script(self):
        pass

    def test_decompose_filters_from_string(self):
        filters = [
            ('rowid__eq=1', [('rowid', '=', '1')]),
            ('rowid__gt=1', [('rowid', '>', '1')]),
            ('rowid__lt=1', [('rowid', '<', '1')]),
            ('rowid__gte=1', [('rowid', '>=', '1')]),
            ('rowid__lte=1', [('rowid', '<=', '1')])
        ]

        for lhv, rhv in filters:
            with self.subTest(lhv=lhv, rhv=rhv):
                result = self.backend.decompose_filters_from_string(lhv)
                self.assertIsInstance(result, list)
                self.assertListEqual(result, rhv)

    @unittest.expectedFailure
    def test_failed_decompose_filters_from_string(self):
        self.backend.decompose_filters_from_string('rowid__google=1')

    def test_decompose_filters(self):
        filters = [
            ({'rowid__eq': 1}, [('rowid', '=', 1)]),
            ({'rowid__gt': 1}, [('rowid', '>', 1)]),
            ({'rowid__lt': 1}, [('rowid', '<', 1)]),
            ({'rowid__gte': 1}, [('rowid', '>=', 1)]),
            ({'rowid__lte': 1}, [('rowid', '<=', 1)])
        ]

        for lhv, rhv in filters:
            with self.subTest(lhv=lhv, rhv=rhv):
                result = self.backend.decompose_filters(**lhv)
                self.assertIsInstance(result, list)
                self.assertListEqual(result, rhv)

    @unittest.expectedFailure
    def test_failed_decompose_filters(self):
        self.backend.decompose_filters_from_string({'rowid__google': 1})

    def test_build_filters(self):
        filters = [
            ([('rowid', '=', '1')], ["rowid = '1'"]),
            ([('rowid', '>', '1')], ["rowid > '1'"]),
            ([('rowid', '<', '1')], ["rowid < '1'"]),
            ([('rowid', '>=', '1')], ["rowid >= '1'"]),
            ([('rowid', '<=', '1')], ["rowid <= '1'"]),
        ]
        for lhv, rhv in filters:
            with self.subTest(lhv=lhv, rhv=rhv):
                result = self.backend.build_filters(lhv)
                self.assertIsInstance(result, list)
                self.assertListEqual(result, rhv)

    @unittest.expectedFailure
    def test_failed_build_filters(self):
        # TODO: If an operator is not found we should not
        # be able to build the filter
        self.backend.build_filters([('rowid', '<=>', '1')])

    def test_build_annotation(self):
        grouping_functions = []

        annotation_map = self.backend.build_annotation(name=Count('name'))
        self.assertTrue(dataclasses.is_dataclass(annotation_map))

        self.assertDictEqual(annotation_map.sql_statements_dict, {
                             'name': 'count(name)'})
        self.assertIn('name', annotation_map.alias_fields)
        self.assertIn('name', annotation_map.field_names)
        self.assertDictEqual(
            annotation_map.annotation_type_map,
            {'name': 'Count'}
        )

        self.assertListEqual(
            annotation_map.joined_final_sql_fields, [
                'count(name) as name']
        )
        self.assertTrue(annotation_map.requires_grouping)


class TestCore(unittest.TestCase):
    def setUp(self):
        table = Table('celebrities')
        db = Database(table)
        db.migrate()
        self.db = db

    def test_list_table_columns_sql(self):
        table = self.db.get_table('celebrities')
        query = table.backend.list_table_columns_sql(table)
        print(query)

    @unittest.expectedFailure
    def test_drop_indexes_sql(self):
        table = self.db.get_table('celebrities')
        sql = table.backend.drop_indexes_sql()

    def test_create_table_fields(self):
        table = self.db.get_table('celebrities')
        table._add_field('firstname', CharField('firstname'))
        table.backend.create_table_fields(table, ['firstname'])
        self.db.objects.all('celebrities')

    def test_list_tables_sql(self):
        table = self.db.get_table('celebrities')
        result = table.backend.list_tables_sql()
        print(result)

    def test_list_database_indexes(self):
        table = self.db.get_table('celebrities')
        result = table.backend.list_database_indexes()
        print(result)

    def test_list_table_indexes(self):
        table = self.db.get_table('celebrities')
        result = table.backend.list_table_indexes(table)
        print(result)

    def test_save_row_object(self):
        row = self.db.objects.create('celebrities', firstname='Kendall')
        row['firstname'] = 'Kylie'
        table = self.db.get_table('celebrities')
        query = table.backend.save_row_object(row)
        print(query)

    def test_save_row_object(self):
        row = self.db.objects.create('celebrities', firstname='Kendall')
        table = self.db.get_table('celebrities')
        query = table.backend.delete_row_object(row)
        print(query)


if __name__ == '__main__':
    unittest.main()
