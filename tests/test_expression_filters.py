import unittest
from lorelie.database.expression_filters import ExpressionFilter
from lorelie.test.testcases import LorelieTestCase


class TestExpressionFilter(LorelieTestCase):
    def test_structure(self):
        connection = self.create_connection()
        instance = ExpressionFilter('age=1', connection)
        self.assertListEqual(instance.parsed_expressions, [('age', '=', '1')])

    def test_valid_expressions(self):
        # ExpressionFilter('google=1')
        # ExpressionFilter('google__eq=1')
        # ExpressionFilter({'google__eq': 1})
        # ExpressionFilter('google__id__eq=1')
        # ExpressionFilter([('google', 'id', 'eq', 1)])
        connection = self.create_connection()

        expressions = [
            ('age=1', ['age', 'eq', '1']),
            ('age__eq=1', ['age', 'eq', '1']),
            ('ages__id__eq=1', ['ages', 'id', 'eq', '1']),
            (['ages', 'eq', 1], ['ages', 'eq', 1])
        ]
        for expression in expressions:
            with self.subTest(expression=expression):
                instance = ExpressionFilter(expression[0], connection)
                self.assertListEqual(
                    expression[1], instance.parsed_expressions)
