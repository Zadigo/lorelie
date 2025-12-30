import sqlite3
import unittest
from unittest.mock import patch

from lorelie.database.nodes import (BaseNode, ComplexNode, DeleteNode,
                                    InsertNode, IntersectNode, JoinNode,
                                    OrderByNode, RawSQL, SelectMap, SelectNode,
                                    UpdateNode, ViewNode, WhereNode)
from lorelie.expressions import Q
from lorelie.test.testcases import LorelieTestCase


class TestBaseNode(LorelieTestCase):
    def test_structure(self):
        class CustomNode(BaseNode):
            def as_sql(self, backend):
                return 'custom sql'

        node = CustomNode(self.create_table())
        self.assertListEqual(node.fields, ['*'])
        self.assertIsNone(node.node_name)
        self.assertIsInstance(node + node, ComplexNode)
        self.assertEqual(node.as_sql(self.create_connection()), 'custom sql')


class TestInsertNode(LorelieTestCase):
    def test_structure(self):
        table = self.create_table()
        insert_values = {'firstname': 'Kendall'}
        node = InsertNode(table, insert_values=insert_values)
        sql = node.as_sql(self.create_connection())
        self.assertListEqual(
            sql,
            [
                "insert into celebrities (firstname) values('Kendall')",
                'returning id'
            ]
        )

        batch_values = [{'firstname': 'Kendall'}, {'firstname': 'Jaime'}]
        node = InsertNode(table, batch_values=batch_values)
        sql = node.as_sql(self.create_connection())
        self.assertListEqual(
            sql,
            [
                "insert into celebrities (firstname) values ('Kendall'), ('Jaime')",
                'returning id'
            ]
        )

    def test_different_value_types(self):
        data = {
            'name': 'Kendall',
            'age': 22,
            'height': lambda: 154,
            'city': ('LA'),
            'country': ['USA']
        }
        node = InsertNode(self.create_table(), insert_values=data)
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            [
                "insert into celebrities (name, age, height, city, country) values('Kendall', 22, 154, 'LA', '[''USA'']')",
                'returning id'
            ]
        )

    def test_batch_values(self):
        node = InsertNode(
            self.create_table(),
            batch_values=[{'name': 'Kendall'}, {'name': 'Kylie'}]
        )
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            [
                "insert into celebrities (name) values ('Kendall'), ('Kylie')",
                'returning id'
            ]
        )


@patch.object(sqlite3, 'connect')
class TestSelectNode(LorelieTestCase):
    def test_structure(self, mock_connect):
        node = SelectNode(self.create_table())
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            ['select * from celebrities']
        )

    def test_distinct(self, mock_connect):
        node = SelectNode(self.create_table(), distinct=True)
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            ['select distinct * from celebrities']
        )

    def test_limit(self, msqlite):
        node = SelectNode(self.create_table(), limit=10)
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            ['select * from celebrities limit 10']
        )

        # With offset
        node = SelectNode(self.create_table(), limit=10, offset=5)
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            ['select * from celebrities limit 10 offset 5']
        )

    def test_all_parameters(self, msqlite):
        node = SelectNode(self.create_table(), distinct=True, limit=10)
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            ['select distinct * from celebrities limit 10']
        )

    def test_with_view_name(self, msqlite):
        # If the view is specified, the view_name takes precedence
        # over the table
        select = SelectNode(self.create_table(), view_name='view_name')
        result = select.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            ['select * from view_name']
        )


@patch.object(sqlite3, 'connect')
class TestWhereNode(LorelieTestCase):
    def test_structure(self, mock_connect):
        node = WhereNode(firstname='Kendall')
        sql = node.as_sql(self.create_connection())
        self.assertEqual(sql, ["where firstname='Kendall'"])

    def test_expressions(self, mock_connect):
        node = WhereNode(firstname='Kendall', lastname='Jenner')
        sql = node.as_sql(self.create_connection())
        self.assertEqual(
            sql, ["where firstname='Kendall' and lastname='Jenner'"])

    def test_arguments(self, mock_connect):
        node = WhereNode(Q(firstname='Kendall'))
        sql = node.as_sql(self.create_connection())
        self.assertEqual(sql, ["where firstname='Kendall'"])

        combined = Q(firstname='Kendall') & Q(lastname='Jenner')
        node = WhereNode(combined)
        sql = node.as_sql(self.create_connection())
        self.assertEqual(
            sql,
            ["where (firstname='Kendall' and lastname='Jenner')"]
        )

    def test_complex_lookup_parameters(self, mock_connect):
        where = WhereNode(age__gte=10, age__lte=40)
        self.assertListEqual(
            where.as_sql(self.create_connection()),
            ['where age>=10 and age<=40']
        )

    def test_arguments_and_expressions(self, mock_connect):
        where = WhereNode(
            Q(lastname='Jenner'),
            firstname='Kendall',
            age__gt=40
        )
        self.assertListEqual(
            where.as_sql(self.create_connection()),
            ["where lastname='Jenner' and firstname='Kendall' and age>40"]
        )

    def test_enriching_existing_parameters(self, mock_connect):
        backend = self.create_connection()

        w1 = WhereNode(firstname='Kendall')
        self.assertListEqual(
            w1.as_sql(backend),
            ["where firstname='Kendall'"]
        )

        new_instance = w1(lastname='Jenner')
        self.assertListEqual(
            w1.as_sql(backend),
            ["where firstname='Kendall' and lastname='Jenner'"]
        )
        self.assertListEqual(
            new_instance.as_sql(backend),
            ["where firstname='Kendall' and lastname='Jenner'"]
        )

    def test_pass_wrong_type_in_dict_expression(self, mock_connect):
        node = WhereNode(firstname=Q(firstname='Kendall'))
        self.assertRaises(
            ValueError,
            node.as_sql,
            self.create_connection()
        )


@patch.object(sqlite3, 'connect')
class TestOrderByNode(LorelieTestCase):
    def test_structure(self, mock_connect):
        node = OrderByNode(self.create_table(), 'id')
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            ['order by id asc']
        )

    def test_descending(self, mock_connect):
        node = OrderByNode(self.create_table(), '-id')
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            ['order by id desc']
        )

    @unittest.expectedFailure
    def test_using_same_field_different_directions(self, mock_connect):
        table = self.create_table()
        a = OrderByNode(table, 'name')
        b = OrderByNode(table, '-name')
        a & b

    @unittest.expectedFailure
    def test_using_same_fields(self, mock_connect):
        table = self.create_table()
        OrderByNode(table, '-name', '-name')
        OrderByNode(table, 'name', 'name')

    def test_and_operation(self, mock_connect):
        # Using AND on this node should return a
        # new class with the joined fields
        table = self.create_table()
        a = OrderByNode(table, 'name')
        b = OrderByNode(table, '-age')

        c = a & b

        self.assertIsInstance(c, OrderByNode)
        self.assertListEqual(
            c.as_sql(self.create_connection()),
            ['order by name asc, age desc']
        )


@patch.object(sqlite3, 'connect')
class TestUpdateNode(LorelieTestCase):
    def test_structure(self, mock_connect):
        node = UpdateNode(
            self.create_table(),
            {'name': 'Kendall'},
            name='Kylie'
        )
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            [
                "update celebrities set name='Kendall'",
                "where name='Kylie'"
            ]
        )

    def test_with_where_node(self, mock_connect):
        node = UpdateNode(
            self.create_table(),
            {'name': 'Kendall'},
            Q(name='Kylie')
        )
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            [
                "update celebrities set name='Kendall'",
                "where name='Kylie'"
            ]
        )

    def test_mixed_args(self, mock_connect):
        node = UpdateNode(
            self.create_table(),
            {'name': 'Kendall'},
            Q(name='Kylie'),
            name='Julie'
        )
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            [
                "update celebrities set name='Kendall'",
                "where name='Kylie' and name='Julie'"
            ]
        )

    def test_cannot_use_q_functions(self, mock_connect):
        node = UpdateNode(
            self.create_table(),
            {'name': 'Kendall'},
            name=Q(name='Kendall')
        )
        self.assertRaises(
            ValueError,
            node.as_sql,
            self.create_connection()
        )


@patch.object(sqlite3, 'connect')
class TestDeleteNode(LorelieTestCase):
    def test_structure(self, mock_connect):
        delete = DeleteNode(self.create_table())
        delete.as_sql(self.create_connection())

    def test_with_where_node(self, mock_connect):
        delete = DeleteNode(self.create_table(), Q(name='Kendall'))
        result = delete.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            ['delete from celebrities', "where name='Kendall'"]
        )

    def test_with_multiple_where_node(self, mock_connect):
        delete = DeleteNode(self.create_table(), Q(name='Kendall'), Q(age=34))
        result = delete.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            ['delete from celebrities', "where name='Kendall' and age=34"]
        )


@patch.object(sqlite3, 'connect')
class TestJoinNode(LorelieTestCase):
    def test_structure(self, mock_connect):
        # celebrities -> followers
        db = self.create_foreign_key_database()
        manager = db.relationships['followers']

        node = JoinNode('followers', manager.relationship_map)
        result = node.as_sql(db.get_table('celebrities').backend)
        expected = [
            'inner join followers on followers.id = celebrities.celebrities_id'
        ]
        self.assertListEqual(result, expected)


@patch.object(sqlite3, 'connect')
class TestComplexNode(LorelieTestCase):
    def test_structure(self, mock_connect):
        select = SelectNode(self.create_table())
        where = WhereNode(name='Kendall')

        complex_node = ComplexNode(select, where)
        raw_sql = complex_node.as_sql(self.create_connection())

        self.assertIsInstance(raw_sql, RawSQL)
        self.assertIn(where, complex_node)


@patch.object(sqlite3, 'connect')
class TestIntersectNode(LorelieTestCase):
    def test_structure(self, mock_connect):
        select1 = SelectNode(self.create_table())
        select2 = SelectNode(self.create_table())

        node = IntersectNode(select1, select2)
        result = node.as_sql(self.create_connection())

        self.assertListEqual(
            result,
            ['select * from celebrities intersect select * from celebrities']
        )


@patch.object(sqlite3, 'connect')
class TestViewNode(LorelieTestCase):
    def test_structure(self, mock_connect):
        db = self.create_database()

        qs = db.celebrities.objects.all()
        node = ViewNode('my_view', qs)

        backend = db.get_table('celebrities').backend
        result = node.as_sql(backend)

        self.assertListEqual(
            result,
            [
                "create view if not exists my_view as select * from celebrities;"
            ]
        )


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


class TestSelectMap(LorelieTestCase):
    def test_structure(self):
        select = SelectNode(self.create_table())
        where = WhereNode(name='Kendall')
        orderby = OrderByNode(self.create_table(), 'name')

        select_map = SelectMap(select, where, orderby)

        self.assertTrue(select_map.should_resolve_map)
        sql = select_map.resolve(self.create_connection())
        self.assertIsInstance(sql, list)

    def test_limit_offset(self):
        select = SelectNode(self.create_table(), limit=10, offset=5)
        where = WhereNode(name='Kendall')
        orderby = OrderByNode(self.create_table(), 'name')

        select_map = SelectMap(select, where, orderby, limit=10, offset=5)
        sql = select_map.resolve(self.create_connection())
        print(sql)
        # self.assertListEqual(
        #     sql,
        #     [
        #         "select * from celebrities where name='Kendall' order by name asc limit 10 offset 5"
        #     ]
        # )

    def test_can_resolve(self):
        select_map = SelectMap()
        self.assertFalse(select_map.should_resolve_map)

    def test_uses_wrong_node_parameters(self):
        select = SelectNode(self.create_table())
        where = WhereNode(name='Kendall')
        select_map = SelectMap(where, select)
        self.assertFalse(select_map.should_resolve_map)
