import sqlite3
import unittest
from unittest.mock import Mock

from lorelie.backends import SQLiteBackend
from lorelie.database.nodes import SelectNode
from lorelie.queries import Query
from lorelie.test.testcases import LorelieTestCase


class TestQuery(LorelieTestCase):
    @unittest.expectedFailure
    def test_cannot_run_if_not_evaluated(self):
        query = Query(backend=self.create_connection())
        query.run()

    def test_pre_sql_setup(self):
        table = Mock(name='Table')
        type(table).name = 'celebrities'

        tokens = [
            'select * from celebrities'
        ]

        query = Query(backend=self.create_connection())
        query.add_sql_nodes(tokens)
        query.pre_sql_setup()

        self.assertIsInstance(query.sql, str)
        self.assertEqual(query.sql, 'select * from celebrities;')
        self.assertTrue(len(query.statements) > 0)

        query.add_sql_node(SelectNode(table, '*'))
        self.assertIsNotNone(query.select_map)

    def test_fail_pre_sql_setup(self):
        query = Query(backend=self.create_connection())

        with self.assertRaises(ValueError):
            query.add_sql_node(25)

        # query.add_sql_node('select * from unknown_table')

        # with self.assertRaises(AttributeError):
        #     query.pre_sql_setup()

    def test_multiple_quick_runs(self):
        statements = [
            'create table if not exists celebrities (id integer primary key autoincrement not null, name text null);',
            # "insert into celebrities values(2, 'Kylie Jenner');",
            "select * from celebrities order by id;"
        ]

        table = Mock(name='Table')
        type(table).name = 'celebrities'
        type(table).backend = self.create_physical_database()

        for statement in statements:
            with self.subTest(statement=statement):
                query = Query(table=table)
                query.add_sql_node(statement)
                query.run()

    def test_mixed_table_runs(self):
        # This test is to watch the reaction of
        # "current_table" on the backend

        statements1 = [
            'create table if not exists celebrities (id integer primary key autoincrement not null, name text null);',
            "insert into celebrities values(1, 'Mariah Carey') on conflict do update set name=excluded.name where id = 1;",
            "select * from celebrities order by id;"
        ]

        statements2 = [
            'create table if not exists movies (id integer primary key autoincrement not null, name text null);',
            "insert into movies values(1, 'Glitters') on conflict do update set name=excluded.name where id = 1;",
            "select * from movies order by id;"
        ]

        connection = self.create_physical_database()

        table1 = Mock(name='Table')
        type(table1).name = 'celebrities'
        type(table1).str_field = 'celebrities'
        type(table1).backend = connection

        table2 = Mock(name='Table')
        type(table2).name = 'movies'
        type(table2).str_field = 'movies'
        type(table2).backend = connection

        results = {
            'celebrities': [],
            'movies': []
        }

        for statement in statements1:
            with self.subTest(statement=statement):
                query = Query(table=table1)
                query.add_sql_node(statement)
                query.run(commit=True)

        results['celebrities'] = query.result_cache

        for statement in statements2:
            with self.subTest(statement=statement):
                query2 = Query(table=table2)
                query2.add_sql_node(statement)
                query2.run(commit=True)

        results['movies'] = query2.result_cache

        for row in results['celebrities']:
            self.assertTrue(
                row.linked_to_table == 'celebrities',
                f'{row.linked_to_table} should be: {row}'
            )

        for row in results['movies']:
            self.assertTrue(
                row.linked_to_table == 'movies',
                f'{row.linked_to_table} should be: {row}'
            )

    def test_fail_execution(self):
        backend = SQLiteBackend()

        # Even though we have a connection, the table
        # does not exist which should raise an error
        with self.assertRaises(sqlite3.OperationalError):
            query = Query(backend=backend)
            query.add_sql_node('select * from celebrities')
            query.run()

        sql = ['select * from celebrities']
        with self.assertRaises(sqlite3.OperationalError):
            query = Query(backend=backend)
            query.run_transaction(backend=backend, sql_tokens=sql)

    def test_run_script(self):
        statements = [
            'create table if not exists celebrities (id integer primary key autoincrement not null, name text null);',
            "insert into celebrities (name) values('Kylie Jenner'), ('Kendall Jenner'), ('Addison Rae');"
        ]

        query = Query.run_transaction(
            backend=self.create_connection(),
            sql_tokens=statements
        )

        self.assertTrue(query.is_evaluated)
        self.assertTrue(query.is_transactional)
