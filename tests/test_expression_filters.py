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
            ('age=1', [('age', '=', '1')]),
            ('age__eq=1', [('age', '=', '1')]),
            ('age__lt=1', [('age', '<', '1')]),
            ({'age__lt': 1}, [('age', '<', 1)])
            # ('ages__id__eq=1', ['ages', 'id', '=', '1'])
        ]
        for lhv, expected in expressions:
            with self.subTest(expression=lhv):
                instance = ExpressionFilter(lhv)
                self.assertListEqual(
                    expected,
                    instance.parsed_expressions
                )

    def test_specific_case_isnull(self):
        filters_to_test = [
            ('age__isnull=True', [('age', 'is', 'null')]),
            ('age__isnull=False', [('age', 'is not', 'null')])
        ]
        for lhv, expected in filters_to_test:
            with self.subTest(lhv=lhv):
                instance = ExpressionFilter(lhv)
                self.assertEqual(instance.parsed_expressions, expected)

    def test_specific_datetimes(self):
        filters_to_test = [
            ('date_of_birth__month=1', [
             ("strftime(%m, date_of_birth)", '=', '1')])
        ]
        for lhv, expected in filters_to_test:
            with self.subTest(lhv=lhv):
                instance = ExpressionFilter(lhv)
                self.assertEqual(instance.parsed_expressions, expected)
