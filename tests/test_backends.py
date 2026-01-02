import dataclasses
import pathlib
import sqlite3
import unittest
from unittest.mock import patch

from lorelie.backends import BaseRow, SQLiteBackend, connections
from lorelie.database.base import Database
from lorelie.database.functions.aggregation import Count
from lorelie.queries import Query
from lorelie.test.testcases import LorelieTestCase


class TestSQLiteBackend(LorelieTestCase):
    def test_in_memory(self):
        sqlite = self.create_connection()
        self.assertTrue(sqlite.database_name == ':memory:')
        self.assertTrue(sqlite.in_memory_connection)
        self.assertIsNone(sqlite.database_path)
        self.assertIsNone(sqlite.database_instance)

    def test_connection(self):
        sqlite = self.create_connection()
        self.assertIsInstance(sqlite.connection, sqlite3.Connection)
        self.assertIsNotNone(sqlite.connection)
        self.assertTrue(len(connections.created_connections) > 0)

    def test_connection_with_path(self):
        sqlite = SQLiteBackend(pathlib.Path(
            '.').joinpath('celebrities_test').absolute())
        self.assertIsNotNone(sqlite.connection)
        self.assertTrue(sqlite.database_path.exists())

    def test_connection_with_database(self):
        db = Database(name='celebrities_test_db')
        sqlite = SQLiteBackend(database_or_name=db)
        self.assertIsNotNone(sqlite.connection)
        self.assertTrue(sqlite.database_path.exists())

        db2 = Database(path=pathlib.Path('.').absolute())
        sqlite2 = SQLiteBackend(database_or_name=db2)
        self.assertIsNotNone(sqlite2.connection)
        self.assertTrue(sqlite2.in_memory_connection)

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
        self.assertEqual(result, "'Ken%'")

        result = connection.quote_startswith(20)
        self.assertEqual(result, "'20%'")

    def test_quote_endswith(self):
        connection = self.create_connection()
        result = connection.quote_endswith('all')
        self.assertRegex(result, r'^\'\%all\'$')

        result = connection.quote_endswith(20)
        self.assertEqual(result, "'%20'")

    def test_quote_like(self):
        connection = self.create_connection()
        result = connection.quote_like('all')
        self.assertRegex(result, r'^\'\%all%\'$')

        result = connection.quote_like(20)
        self.assertEqual(result, "'%20%'")

    def test_dict_to_sql(self):
        connection = self.create_connection()
        result = connection.dict_to_sql({'name__eq': 'Kendall'})
        self.assertIsInstance(result, (list, tuple))
        self.assertTupleEqual(result, (['name__eq'], ["'Kendall'"]))

    def test_build_script(self):
        sql_statements = [
            'create table celebrities (id integer primary key autoincrement not null, name text null)',
            "insert into celebrities values(1, 'Kendall Jenner')",
            "select * from celebrities order by id"
        ]
        connection = self.create_connection()
        result = connection.build_script(*sql_statements)
        self.assertIsInstance(result, str)
        expected_script = (
            'create table celebrities (id integer primary key autoincrement not null, name text null);\n'
            "insert into celebrities values(1, 'Kendall Jenner');\n"
            'select * from celebrities order by id;'
        )
        self.assertEqual(result, expected_script)

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


class TestBaseRow(LorelieTestCase):
    def setUp(self):
        self.backend = self.create_connection()
        self.fields = ['name']
        self.data = {'id': 1, 'name': 'Kendall'}

    def test_structure(self):
        row = BaseRow(self.fields, self.data, self.backend)
        self.assertEqual(row.pk, 1)
        self.assertEqual(row.name, 'Kendall')
        self.assertTrue('Kendall' in row)
        self.assertTrue(row['name'] == 'Kendall')

    def test_set_item(self):
        row = BaseRow(self.fields, self.data, self.backend)
        row['name'] = 'Kylie'
        self.assertTrue(row.mark_for_update)
        self.assertEqual(row.name, 'Kylie')
        self.assertIn('name', row.updated_fields)
        self.assertEqual(row.updated_fields['name'], 'Kylie')

    def test_save(self):
        db = self.create_database()
        table = db.get_table('celebrities')

        row = BaseRow(self.fields, self.data, table.backend)
        row.linked_to_table = table.name

        with patch('lorelie.backends.Query', spec=Query) as mquery:
            _query = mquery.return_value
            _query.add_sql_node.return_value = None
            _query.run.return_value = None

            row['name'] = 'Kylie'
            self.assertTrue(row.mark_for_update)

            row.save()

            self.assertEqual(row.name, 'Kylie')
            self.assertFalse(row.mark_for_update)

            _query.add_sql_node.assert_called()
            _query.run.assert_called()

    def test_delete(self):
        pass

    def test_refresh_from_database(self):
        pass


class TestBackendCoreFunctions(LorelieTestCase):
    def test_list_all_tables(self):
        self.create_database()
        conn = connections.get_last_connection()
        result = conn.list_all_tables()
        self.assertTrue(len(result) > 0)

        for item in result:
            print(vars(item))
            self.assertTrue(hasattr(item, 'name'))

    def test_list_table_columns_sql(self):
        db = self.create_database()
        table = db.get_table('celebrities')
        conn = connections.get_last_connection()
        result = conn.list_table_columns(table)
        # ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk']
        self.assertTrue(len(result) > 0)

    def test_list_database_indexes(self):
        self.create_database()
        conn = connections.get_last_connection()
        result = conn.list_database_indexes()
        # ['type', 'name', 'tbl_name', 'sql']
        self.assertTrue(len(result) > 0)

    def test_list_table_indexes(self):
        db = self.create_database()
        table = db.get_table('celebrities')
        conn = connections.get_last_connection()
        result = conn.list_table_indexes(table)
        print(result)

    def test_save_row_object(self):
        db = self.create_database()
        row = db.celebrities.objects.create(
            name='Kendall Jenner',
            height=170
        )

        row['name'] = 'Kylie Jenner'
        row.save()

        self.assertEqual(row['name'], 'Kylie Jenner')

    def test_delete_row_object(self):
        db = self.create_database()
        row = db.celebrities.objects.create(
            name='Kendall Jenner',
            height=170
        )

        row['name'] = 'Kylie Jenner'
        row.delete()

        self.assertFalse(db.celebrities.objects.all().exists())
