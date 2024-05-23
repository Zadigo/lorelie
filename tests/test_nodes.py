from lorelie.database.nodes import InsertNode, WhereNode
from lorelie.expressions import Q
from lorelie.test.testcases import LorelieTestCase

# from types import NotImplementedType
# import unittest

# from lorelie.backends import SQLiteBackend
# from lorelie.database.nodes import BaseNode, ComplexNode, InsertNode, OrderByNode, SelectNode, UpdateNode, WhereNode
# from lorelie.expressions import Q
# from lorelie.tables import Table

# table = Table('test_table')
# table.backend = SQLiteBackend()


class TestBaseNode(LorelieTestCase):
    pass


class TestInsertNode(LorelieTestCase):
    def test_structure(self):
        table = self.create_table()
        insert_values = {'firstname': 'Kendall'}
        node = InsertNode(table, insert_values=insert_values)
        sql = node.as_sql(self.create_connection())
        self.assertListEqual(
            sql,
            ["insert into celebrities (firstname) values('Kendall')"]
        )

        batch_values = [{'firstname': 'Kendall'}, {'firstname': 'Jaime'}]
        node = InsertNode(table, batch_values=batch_values)
        sql = node.as_sql(self.create_connection())
        self.assertListEqual(
            sql,
            ["insert into celebrities (firstname) values ('Kendall'), ('Jaime')"]
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
        print(node.as_sql(self.create_connection()))


class TestWhereNode(LorelieTestCase):
    def test_structure(self):
        node = WhereNode(firstname='Kendall')
        sql = node.as_sql(self.create_connection())
        self.assertEqual(sql, ["where firstname='Kendall'"])

    def test_expressions(self):
        node = WhereNode(firstname='Kendall', lastname='Jenner')
        sql = node.as_sql(self.create_connection())
        self.assertEqual(sql, ["where firstname='Kendall' and lastname='Jenner'"])

    def test_arguments(self):
        node = WhereNode(Q(firstname='Kendall'))
        sql = node.as_sql(self.create_connection())
        self.assertEqual(sql, ["where firstname='Kendall'"])

        combined = Q(firstname='Kendall') & Q(lastname='Jenner')
        node = WhereNode(combined)
        sql = node.as_sql(self.create_connection())
        self.assertEqual(sql, ["where (firstname='Kendall' and lastname='Jenner')"])

    def test_complex_lookup_parameters(self):
        where = WhereNode(age__gte=10, age__lte=40)
        self.assertListEqual(
            where.as_sql(self.create_connection()),
            ['where age>=10 and age<=40']
        )

    def test_arguments_and_expressions(self):
        where = WhereNode(
            Q(lastname='Jenner'),
            firstname='Kendall', age__gt=40
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



# class TestBaseNode(unittest.TestCase):
#     def test_structure(self):
#         n1 = BaseNode(table=table, fields=['firstname'])
#         n2 = BaseNode(table=table, fields=['lastname'])
#         self.assertFalse(n1 != n2)

#     def test_and(self):
#         a = BaseNode(table=table, fields=['firstname'])
#         b = BaseNode(table=table, fields=['lastname'])
#         c = a + b
#         self.assertIsInstance(c, ComplexNode)
#         self.assertEqual(c.as_sql(table.backend), NotImplementedType)

# class TestSelectNode(unittest.TestCase):
#     def test_structure(self):
#         select = SelectNode(table, 'firstname', 'lastname')
#         self.assertListEqual(
#             select.as_sql(table.backend),
#             ['select firstname, lastname from test_table']
#         )

#         select_distinct = SelectNode(
#             table, 'firstname', 'lastname', distinct=True)
#         self.assertListEqual(
#             select_distinct.as_sql(table.backend),
#             ['select distinct firstname, lastname from test_table']
#         )



# class TestOrderNode(unittest.TestCase):
#     def test_structure(self):
#         order_by = OrderByNode(table, 'name', 'age')
#         self.assertListEqual(
#             order_by.as_sql(table.backend),
#             ['order by name asc, age asc']
#         )

#         order_by = OrderByNode(table, 'name', '-age')
#         self.assertListEqual(
#             order_by.as_sql(table.backend),
#             ['order by name asc, age desc']
#         )

#         order_by = OrderByNode(table, '-name', '-age')
#         self.assertListEqual(
#             order_by.as_sql(table.backend),
#             ['order by name desc, age desc']
#         )

#     @unittest.expectedFailure
#     def test_has_same_fields(self):
#         # Test the user putting the same fields
#         order_by = OrderByNode(table, '-name', '-name')
#         self.assertListEqual(
#             order_by.as_sql(table.backend),
#             ['order by name desc']
#         )

#         order_by = OrderByNode(table, 'name', 'name')
#         self.assertListEqual(
#             order_by.as_sql(table.backend),
#             ['order by name asc']
#         )

#     def test_and_operation(self):
#         a = OrderByNode(table, 'name')
#         b = OrderByNode(table, '-age')
#         c = a & b
#         self.assertIsInstance(c, OrderByNode)
#         self.assertListEqual(
#             c.as_sql(table.backend),
#             ['order by name asc, age desc']
#         )

#     @unittest.expectedFailure
#     def test_and_operation_fail(self):
#         a = OrderByNode(table, 'name')
#         b = OrderByNode(table, '-name')
#         c = a & b


# class TestUpdateNode(unittest.TestCase):
#     def test_structure(self):
#         defaults = {'firstname': 'Kandy'}
#         update_node = UpdateNode(table, defaults, firstname='Kendall')
#         result = update_node.as_sql(table.backend)

#         self.assertListEqual(
#             result[:1],
#             ["update test_table set firstname='Kandy'"]
#         )

#         self.assertListEqual(
#             result[1:][0].as_sql(table.backend),
#             ["where firstname='Kendall'"]
#         )


# class TestInsertNode(unittest.TestCase):
#     def test_simple_update(self):
#         insert_defaults = {'firstname': 'Kendall'}
#         insert_node = InsertNode(
#             table,
#             insert_defaults
#         )
#         self.assertListEqual(
#             insert_node.as_sql(table.backend),
#             ["insert into test_table (firstname) values('Kendall')"]
#         )

#     def test_insert_mode(self):
#         insert_node = InsertNode(
#             table,
#             firstname='Kendall'
#         )
#         self.assertListEqual(
#             insert_node.as_sql(table.backend),
#             ["insert into test_table (firstname) values('Kendall')"]
#         )
