import unittest
from lorelie.database.expression_filters import ExpressionFilter
from lorelie.test.testcases import LorelieTestCase


class TestExpressionFilter(LorelieTestCase):
    def test_structure(self):
        instance = ExpressionFilter('age=1')
        self.assertListEqual(instance.parsed_expressions, [('age', '=', '1')])

    def test_valid_expressions(self):
        # ExpressionFilter('google=1')
        # ExpressionFilter('google__eq=1')
        # ExpressionFilter({'google__eq': 1})
        # ExpressionFilter('google__id__eq=1')
        # ExpressionFilter([('google', 'id', 'eq', 1)])

        expressions = [
            ('age=1', ['age', '=', '1']),
            ('age__eq=1', ['age', '=', '1']),
            ('ages__id__eq=1', ['ages', 'id', '=', '1']),
            ([('ages', 'eq', 1)], [('ages', '=', 1)])
        ]
        for value_to_test, expected in expressions:
            with self.subTest(expression=value_to_test):
                instance = ExpressionFilter(value_to_test)
                print(instance.parsed_expressions)
                # self.assertListEqual(
                #     expression[1],
                #     instance.parsed_expressions
                # )
