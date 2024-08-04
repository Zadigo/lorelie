import sqlite3
import unittest

from lorelie.database.nodes import SelectNode, DeleteNode
from lorelie.expressions import Q
from lorelie.queries import Query
from lorelie.test.testcases import LorelieTestCase


class TestQuery(LorelieTestCase):
    @unittest.expectedFailure
    def test_cannot_run_if_not_evaluated(self):
        query = Query(backend=self.create_connection())
        query.run()

    def test_pre_sql_setup(self):
        backend = self.create_connection()
        tokens = ['select * from celebrities']
        
        query = Query()
        query.add_sql_nodes(tokens)
        query.pre_sql_setup()

        self.assertIsInstance(query.sql, str)
        self.assertEqual(query.sql, 'select * from celebrities;')

    def test_invalid_sql_raises_error_in_pre_sql(self):
        self.create_connection()
        query = Query()
        query.add_sql_node(';')
        self.assertRaises(sqlite3.Error, query.pre_sql_setup)

    def test_excution(self):
        backend = self.create_connection()

        query = Query(backend=backend)
        self.assertFalse(query.is_evaluated)
        self.assertIsNone(query.table)

        # Even though we have a connection, the table
        # does not exist which should raise an error
        query.add_sql_node('select * from celebrities')
        with self.assertRaises(sqlite3.Error):
            query.run()

        # sql = ['select * from celebrities']
        # with self.assertRaises(sqlite3.Error):
        #     query.run_script(backend=backend, sql_tokens=sql)

    def test_working_execution(self):
        backend = self.create_connection()

        query = Query(backend=backend)
        other = query.run_script(
            backend=backend,
            sql_tokens=[
                'create table celebrities (id integer primary key autoincrement not null, name text null)',
                "insert into celebrities values(1, 'Kendall Jenner')",
                "select * from celebrities order by id"
            ]
        )

        expected_script = "begin; create table celebrities (id integer primary key autoincrement not null, name text null); insert into celebrities values(1, 'Kendall Jenner'); select * from celebrities order by id; commit;"
        self.assertEqual(other.sql, expected_script)
        print(other.result_cache)
        # self.assertTrue(len(other.result_cache) > 0)

    def test_add_sql_node(self):
        self.create_connection()

        # Add select node
        table = self.create_table()
        select_node = SelectNode(table)

        query = Query()
        query.add_sql_node(select_node)

        self.assertIn(select_node, query.statements)
        self.assertEqual(query.select_map.select, select_node)

    def test_add_none_select_sql_node(self):
        self.create_connection()
        table = self.create_table()

        # Add a node that has nothing to do with a
        # select node -- watch the way the node is added
        query = Query()
        delete_node = DeleteNode(table, Q(name='Kendall'))
        query.add_sql_node(delete_node)
       
        with self.assertRaises(AttributeError):
            query.select_map.delete
