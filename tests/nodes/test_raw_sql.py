import sqlite3
from unittest.mock import patch

from lorelie.database.nodes import (
    RawSQL,
    SelectNode,
    WhereNode,
)
from lorelie.test.testcases import LorelieTestCase


@patch.object(sqlite3, 'connect')
class TestRawSQL(LorelieTestCase):
    def test_structure(self, mock_connect):
        select = SelectNode(self.create_table())
        where = WhereNode(name='Kendall')

        instance = RawSQL(self.create_connection(), select, where)
        result = instance.as_sql()
        self.assertIsInstance(result, list)

        expected = ['select * from celebrities', "where name='Kendall'"]
        self.assertListEqual(list(result), expected)
        self.assertListEqual(result, expected)

        expected = "select * from celebrities where name='Kendall'"
        self.assertEqual(str(instance), expected)

        # TODO: Optimize
        self.assertTrue(expected == instance)

    def test_equality(self, mock_connect):
        select = SelectNode(self.create_table())
        instance = RawSQL(self.create_connection(), select)
        expected = 'select * from celebrities'
        self.assertTrue(expected == instance)
        self.assertTrue(instance == instance)

    def test_select_node_resolution(self, mock_connect):
        select = SelectNode(self.create_table(), limit=10)
        instance = RawSQL(self.create_connection(), select)
        select_map = instance.select_map
        self.assertTrue(select_map.should_resolve_map)
        print(select_map)

    def test_can_resolve(self, mock_connect):
        select = SelectNode(self.create_table(), limit=10)
        instance = RawSQL(self.create_connection(), select)
        self.assertTrue(instance.can_resolve)

        where = WhereNode(name='Kendall')
        instance = RawSQL(self.create_connection(), where)
        self.assertFalse(instance.can_resolve)

