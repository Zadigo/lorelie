import sqlite3
from unittest.mock import patch

from lorelie.database.nodes import (
    OrderByNode,
)
from lorelie.test.testcases import LorelieTestCase


@patch.object(sqlite3, 'connect')
class TestOrderByNode(LorelieTestCase):
    def setUp(self):
        self.table = self.create_table()
        
    def test_structure(self, mconn):
        node = OrderByNode(self.table, 'id')
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            ['order by id asc']
        )

    def test_descending(self, mconn):
        node = OrderByNode(self.table, '-id')
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            ['order by id desc']
        )

    def test_using_same_field_different_directions(self, mconn):
        with self.assertRaises(ValueError) as context:
            a = OrderByNode(self.table, 'name')
            b = OrderByNode(self.table, '-name')
            a & b
            self.assertRaises(ValueError, str(context.exception), "The field 'name' has been registered twice in ascending and descending fields")

    def test_using_same_fields(self, mconn):
        with self.assertRaises(ValueError) as context:
            table = self.table
            OrderByNode(table, '-name', '-name')
            OrderByNode(table, 'name', 'name')
            self.assertRaises(ValueError, str(context.exception), "The field 'name' has been registered twice in ascending and descending fields")

    def test_and_operation(self, mconn):
        # Using AND on this node should return a
        # new class with the joined fields
        table = self.table
        a = OrderByNode(table, 'name')
        b = OrderByNode(table, '-age')

        c = a & b

        self.assertIsInstance(c, OrderByNode)
        self.assertListEqual(
            c.as_sql(self.create_connection()),
            ['order by name asc, age desc']
        )

    def test_deconstruct(self, mconn):
        node = OrderByNode(self.table, 'id', '-name')
        result = node.deconstruct()
        self.assertTupleEqual(
            result,
            ('OrderByNode', 'celebrities', ('id', '-name'))
        )

    def test_invalid_field_type(self, mconn):
        with self.assertRaises(ValueError) as context:
            OrderByNode(self.table, 123)
            self.assertRaises(ValueError, str(context.exception), "Field '123' should be of type <str>")
