import sqlite3
from unittest.mock import patch

from lorelie.database.nodes import (
    ComplexNode,
    OrderByNode,
    RawSQL,
    SelectNode,
    WhereNode,
)
from lorelie.test.testcases import LorelieTestCase


@patch.object(sqlite3, 'connect')
class TestComplexNode(LorelieTestCase):
    def test_structure(self, mock_connect):
        select = SelectNode(self.create_table())
        where = WhereNode(name='Kendall')

        complex_node = ComplexNode(select, where)
        raw_sql = complex_node.as_sql(self.create_connection())

        self.assertIsInstance(raw_sql, RawSQL)
        self.assertIn(where, complex_node)

    def test_similar_nodes(self, mock_connect):
        select = SelectNode(self.create_table())
        where = WhereNode(name='Kendall')
        where2 = WhereNode(lastname='Kylie')

        complex_node = ComplexNode(select, where, where2)
        raw_sql = complex_node.as_sql(self.create_connection())

        print(raw_sql)

        # self.assertIsInstance(raw_sql, RawSQL)
        # self.assertIn(where, complex_node)

    def test_add_valid(self, mconn):
        c1 = ComplexNode() + SelectNode(self.create_table())
        self.assertIsInstance(c1, ComplexNode)

        c2 = c1 + OrderByNode(self.create_table())
        self.assertIsInstance(c2, ComplexNode)
        self.assertTrue(len(c2.nodes) > 0)

    def test_add_invalid(self, mconn):
        c1 = ComplexNode()
        c2 = 'Invalid value'

        c3 = c1 + c2

        self.assertEqual(c3, NotImplemented)
