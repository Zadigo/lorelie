import sqlite3
from unittest.mock import patch

from lorelie.database.nodes import ComplexNode, WhereNode
from lorelie.expressions import Q
from lorelie.test.testcases import LorelieTestCase


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

    def test_addition(self, mock_connect):
        w1 = WhereNode(firstname='Kendall')
        w2 = WhereNode(lastname='Jenner', age=25)

        combined = w1 + w2
        result = combined.as_sql(self.create_connection())

        self.assertListEqual(
            list(result),
            ["where firstname='Kendall'", "where lastname='Jenner' and age=25"],
            f'Failed to combine WhereNodes using + operator: {type(result)}'
        )

        self.assertIsInstance(combined, ComplexNode)
        self.assertIn(w1, combined)
        self.assertIn(w2, combined)
