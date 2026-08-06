import sqlite3
from unittest.mock import patch

from lorelie.database.nodes import (
    ComplexNode,
    OrderByNode,
    SelectMap,
    SelectNode,
    WhereNode,
)
from lorelie.test.testcases import LorelieTestCase


@patch.object(sqlite3, 'connect')
class TestSelectNode(LorelieTestCase):
    def setUp(self):
        self.table = self.create_table()

    def test_structure(self, mconn):
        node = SelectNode(self.table)
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            ['select * from celebrities']
        )

    def test_distinct(self, mconn):
        node = SelectNode(self.table, distinct=True)
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            ['select distinct * from celebrities']
        )

    def test_all_parameters(self, mconn):
        node = SelectNode(
            self.table,
            distinct=True, 
            limit=10, # Limit is resolved by the SelectMap, so it won't be included in the SQL of this node
            offset=5 # Same as limit
        )
        
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            ['select distinct * from celebrities']
        )

    def test_with_view_name(self, mconn):
        # If the view is specified, the view_name takes precedence
        # over the table
        select = SelectNode(self.table, view_name='view_name')
        result = select.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            ['select * from view_name']
        )

    def test_deconstruct(self, mconn):
        node = SelectNode(self.table, limit=10, offset=5)
        result = node.deconstruct()
        self.assertListEqual(
            result,
            [
                'SelectNode',
                'celebrities',
                ['*'],
                {'distinct': False, 'limit': 10, 'offset': 5, 'view_name': None}
            ]
        )

    def test_call(self, mconn):
        node = SelectNode(self.table, 'firstname')
        result = node('lastname')

        self.assertIsInstance(result, SelectNode)
        self.assertEqual(result.fields, ['firstname', 'lastname'])


@patch.object(sqlite3, 'connect')
class TestSelectNodeExceptions(LorelieTestCase):
    def test_table_is_none(self, mconn):
        node = SelectNode(None)

        with self.assertRaises(ValueError) as context:
            node.as_sql(self.create_connection())
            self.assertEqual(str(context.exception), "Table cannot be None.")


@patch.object(sqlite3, 'connect')
class TestSelectMap(LorelieTestCase):
    def setUp(self):
        self.table = self.create_table()

    def test_structure(self, mconn):
        select = SelectNode(self.table)
        where = WhereNode(name='Kendall')
        orderby = OrderByNode(self.create_table(), 'name')

        select_map = SelectMap(select, where, orderby)

        self.assertTrue(select_map.should_resolve_map)
        sql = select_map.resolve(self.create_connection())
        self.assertIsInstance(sql, list)

    def test_limit_offset(self, mconn):
        select = SelectNode(self.table, limit=10, offset=5)
        where = WhereNode(name='Kendall')
        orderby = OrderByNode(self.table, 'name')

        select_map = SelectMap(select, where, orderby, limit=10, offset=5)
        sql = select_map.resolve(self.create_connection())
        self.assertListEqual(
            sql,
            [
                'select * from celebrities',
                "where name='Kendall'",
                'order by name asc',
                'limit 10',
                'offset 5'
            ]
        )

    def test_groupby_and_having(self, mconn):
        select = SelectNode(self.table)

        select_map = SelectMap(select, groupby='group by name', having='having name')
        sql = select_map.resolve(self.create_connection())

        self.assertListEqual(
            sql,
            [
                'select * from celebrities',
                'group by name',
                'having name'
            ]
        )

    def test_can_resolve(self, mconn):
        select_map = SelectMap()
        self.assertFalse(select_map.should_resolve_map)

    def test_uses_wrong_node_parameters(self, mconn):
        select = SelectNode(self.create_table())
        where = WhereNode(name='Kendall')
        select_map = SelectMap(where, select)
        self.assertFalse(select_map.should_resolve_map)

    def test_no_select_exception(self, mconn):
        map = SelectMap()
        with self.assertRaises(ValueError) as context:
            map.resolve(self.create_connection())
            self.assertEqual(str(context.exception), "SelectNode is required to resolve the SQL")

    def test_add_where_already_exists(self, mconn):
        where1 = WhereNode(name='Kendall')
        where2 = WhereNode(name='Kylie')

        map = SelectMap(where=where1)
        map.add_where(where2)

        self.assertIsNotNone(map.where)
        self.assertIsInstance(map.where, ComplexNode)

    def test_add_where_does_not_exist(self, mconn):
        map = SelectMap()
        where = WhereNode(name='Kendall')
        map.add_where(where)

        self.assertIsNotNone(map.where)
        self.assertIsInstance(map.where, WhereNode)

    def test_add_where_invalid(self, mconn):
        map = SelectMap()
        with self.assertRaises(ValueError) as context:
            map.add_where('invalid')
            self.assertEqual(str(context.exception), "Invalid WhereNode")

    def test_add_ordering_does_not_exist(self, mconn):
        map = SelectMap()
        order_by = OrderByNode(self.table)
        map.add_ordering(order_by)

        self.assertIsNotNone(map.order_by)
        self.assertIsInstance(map.order_by, OrderByNode)

    def test_add_ordering_already_exists(self, mconn):
        map = SelectMap()

        order_by1 = OrderByNode(self.table)
        order_by2 = OrderByNode(self.table)

        map.add_ordering(order_by1)
        map.add_ordering(order_by2)

        self.assertIsNotNone(map.order_by)
        self.assertIsInstance(map.order_by, OrderByNode)

    def test_add_ordering_invalid(self, mconn):
        map = SelectMap()
        with self.assertRaises(ValueError) as context:
            map.add_ordering('invalid')
            self.assertEqual(str(context.exception), "Invalid OrderByNode")
