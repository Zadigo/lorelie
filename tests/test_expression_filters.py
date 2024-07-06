import unittest
from lorelie.database.expression_filters import ExpressionFilter


# s = ExpressionFilter('google=1')
# s = ExpressionFilter('google__eq=1')
# s = ExpressionFilter({'google__eq': 1})
# s = ExpressionFilter('google__id__eq=1')
# print(s.expressions_maps[0])

# s = ExpressionFilter('google')
# print(list(map(lambda x: x, ExpressionFilter('google__id__eq=1'))))

class TestExpressionFilter(unittest.TestCase):
    def test_structure(self):
        pass

    def test_valid_expressions(self):
        expressions = [
            ('age=1', ['age', 'eq', '1']),
            ('age__eq=1', ['age', 'eq', '1']),
            ('ages__id__eq=1', ['ages', 'id', 'eq', '1']),
        ]
        for expression in expressions:
            with self.subTest(expression=expression):
                instance = ExpressionFilter(expression[0])
                self.assertListEqual(expression[1], instance.expressions)
