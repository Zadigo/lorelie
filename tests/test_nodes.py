import dataclasses
import unittest

from lorelie.database.nodes import (BaseNode, ComplexNode, DeleteNode,
                                    InsertNode, IntersectNode, JoinNode,
                                    NodeAggregator, OrderByNode, SelectMap,
                                    SelectNode, UpdateNode, ViewNode,
                                    WhereNode)
from lorelie.database.tables.base import RelationshipMap, ValidatedData
from lorelie.database.tables.columns import Column
from lorelie.expressions import Q
from lorelie.test.testcases import LorelieTestCase


class TestNodeAggregator(LorelieTestCase):
    def test_structure(self):
        nodes = [
            SelectNode(self.create_table(), 'firstname'),
            WhereNode(Q(firstname='Kendall'))
        ]
        node = NodeAggregator(self.create_connection(), *nodes)
        result = node.as_sql()
        print(result)


class TestBaseNode(LorelieTestCase):
    def test_structure(self):
        node = BaseNode(table=self.create_table())
        self.assertEqual(node.node_name, NotImplemented)

        columns = node.pre_sql_setup(['name'])
        for column in columns:
            with self.subTest(column=column):
                self.assertIsInstance(column, Column)
                self.assertTrue(dataclasses.is_dataclass(column))
                self.assertTrue('name', column.name)


class TestInsertNode(LorelieTestCase):
    def test_structure(self):
        table = self.create_table()
        insert_values = {'name': 'Kendall'}
        node = InsertNode(table, insert_values=insert_values)
        sql = node.as_sql(self.create_connection())
        self.assertListEqual(
            sql,
            [
                "insert into celebrities (name) values('Kendall')",
                'returning id'
            ]
        )

        batch_values = [{'name': 'Kendall'}, {'name': 'Jaime'}]
        node = InsertNode(table, batch_values=batch_values)
        sql = node.as_sql(self.create_connection())
        self.assertListEqual(
            sql,
            [
                "insert into celebrities (name) values ('Kendall'), ('Jaime')",
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
            batch_values=[
                {'name': 'Kendall'},
                {'name': 'Kylie'}
            ]
        )
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            [
                "insert into celebrities (name) values ('Kendall'), ('Kylie')",
                'returning id'
            ]
        )

    def test_with_validated_values_dataclass(self):
        conn = self.create_connection()

        validated_value = conn.quote_value('Kendall')
        validated_data = ValidatedData(
            [validated_value],
            data={'name': validated_value}
        )

        table = self.create_table()
        table.backend = conn
        node = InsertNode(table, insert_values=validated_data)

        sql = node.as_sql(conn)
        self.assertEqual(
            sql,
            [
                "insert into celebrities (name) values('Kendall')",
                'returning id'
            ]
        )


class TestSelectNode(LorelieTestCase):
    def test_structure(self):
        node = SelectNode(self.create_table())
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            ['select * from celebrities']
        )

    def test_distinct(self):
        node = SelectNode(self.create_table(), distinct=True)
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            ['select distinct * from celebrities']
        )


class TestWhereNode(LorelieTestCase):
    def test_structure(self):
        node = WhereNode(firstname='Kendall')
        sql = node.as_sql(self.create_connection())
        self.assertEqual(sql, ["where firstname='Kendall'"])

    def test_expressions(self):
        node = WhereNode(firstname='Kendall', lastname='Jenner')
        sql = node.as_sql(self.create_connection())
        self.assertEqual(
            sql, ["where firstname='Kendall' and lastname='Jenner'"])

    def test_arguments(self):
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

    def test_complex_lookup_parameters(self):
        where = WhereNode(age__gte=10, age__lte=40)
        self.assertListEqual(
            where.as_sql(self.create_connection()),
            ['where age>=10 and age<=40']
        )

    def test_arguments_and_expressions(self):
        where = WhereNode(
            Q(lastname='Jenner'),
            firstname='Kendall',
            age__gt=40
        )
        self.assertListEqual(
            where.as_sql(self.create_connection()),
            ["where lastname='Jenner' and firstname='Kendall' and age>40"]
        )

    def test_enriching_existing_parameters(self):
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

    def test_cannot_use_q_functions(self):
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


class TestOrderByNode(LorelieTestCase):
    def test_structure(self):
        node = OrderByNode(self.create_table(), 'id')
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            ['order by id asc']
        )

    def test_descending(self):
        node = OrderByNode(self.create_table(), '-id')
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            ['order by id desc']
        )

    @unittest.expectedFailure
    def test_using_same_field_different_directions(self):
        table = self.create_table()
        a = OrderByNode(table, 'name')
        b = OrderByNode(table, '-name')
        a & b

    @unittest.expectedFailure
    def test_using_same_fields(self):
        table = self.create_table()
        OrderByNode(table, '-name', '-name')
        OrderByNode(table, 'name', 'name')

    def test_and_operation(self):
        table = self.create_table()
        a = OrderByNode(table, 'name')
        b = OrderByNode(table, '-age')

        c = a & b

        self.assertIsInstance(c, OrderByNode)
        self.assertListEqual(
            c.as_sql(self.create_connection()),
            ['order by name asc, age desc']
        )


class TestUpdateNode(LorelieTestCase):
    def test_structure(self):
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

    def test_with_where_node(self):
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

    def test_mixed_args(self):
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


class TestDeleteNode(LorelieTestCase):
    def test_structure(self):
        delete = DeleteNode(self.create_table())
        delete.as_sql(self.create_connection())

    def test_with_where_node(self):
        delete = DeleteNode(self.create_table(), Q(name='Kendall'))
        result = delete.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            ['delete from celebrities', "where name='Kendall'"]
        )

    def test_with_multiple_where_node(self):
        delete = DeleteNode(self.create_table(), Q(name='Kendall'), Q(age=34))
        result = delete.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            ['delete from celebrities', "where name='Kendall' and age=34"]
        )


class TestJoinNode(LorelieTestCase):
    def test_structure(self):
        db = self.create_foreign_key_database()

        celebrity = db.get_table('celebrity')
        follower = db.get_table('follower')

        relationship_map = RelationshipMap(celebrity, follower)
        node = JoinNode(celebrity, relationship_map)
        result = node.as_sql(self.create_connection())
        self.assertEqual(
            result,
            ['inner join celebrity on celebrity.id = follower.celebrity_id']
        )


class TestComplexNode(LorelieTestCase):
    def test_structure(self):
        select = SelectNode(self.create_table())
        where = WhereNode(name='Kendall')
        node = ComplexNode(select, where)
        result = node.as_sql(self.create_connection())
        print(result)


class TestIntersectNode(LorelieTestCase):
    def test_structure(self):
        select1 = SelectNode(self.create_table())
        select2 = SelectNode(self.create_table())

        node = IntersectNode(select1, select2)
        result = node.as_sql(self.create_connection())

        self.assertListEqual(
            result,
            ['select * from celebrities intersect select * from celebrities']
        )


class TestViewNode(LorelieTestCase):
    def test_structure(self):
        db = self.create_database()
        node = ViewNode('my_view', db.celebrities.objects.all())
        result = node.as_sql(db.get_table('celebrities').backend)
        self.assertListEqual(
            result,
            [
                "create view if not exists my_view as select * from celebrities;"
            ]
        )


class TestSelectMap(LorelieTestCase):
    def test_structure(self):
        db = self.create_foreign_key_database()

        t1 = db.get_table('celebrity')

        select_node = SelectNode(t1)
        orderby_node = OrderByNode(t1, 'id')

        select_map = SelectMap(select_node, order_by=orderby_node)
        resolution = select_map.resolve(self.create_connection())
        self.assertListEqual(
            resolution,
            ['select * from celebrity', 'order by id asc']
        )
