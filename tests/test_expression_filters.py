from lorelie.database.expression_filters import ExpressionFiltersMixin
from lorelie.test.testcases import LorelieTestCase


class TestExpressionFilterMixin(LorelieTestCase):
    def setUp(self):
        self.instance = ExpressionFiltersMixin()

    def test_translate_operators_from_tokens(self):
        translation = self.instance.translate_operators_from_tokens(
            [['age', 'eq', 1]]
        )
        lhv, operator, rhv = translation[0]

        self.assertEqual(lhv, 'age')
        self.assertEqual(operator, '=')
        self.assertEqual(rhv, 1)

    def test_decompose_filters_columns(self):
        expressions = [
            ('age=1', ['age']),
            ('age__eq=1', ['age']),
            # ('ages__id__eq=1', ['ages', 'id']),
            # ({'ages__id__eq': 1}, ['ages', 'id'])
        ]
        for value_to_test, expected in expressions:
            with self.subTest(expression=value_to_test):
                columns = self.instance.decompose_filters_columns(
                    value_to_test)
                self.assertListEqual(columns, expected)

    def test_build_filters(self):
        items = [
            ('age', '=', 1)
        ]
        built_filters = self.instance.build_filters(items)
        self.assertListEqual(built_filters, [
            'age = 1',
            "name like '%John%'"
        ])

    # def test_structure(self):
    #     instance = ExpressionFilter('age=1')
    #     self.assertListEqual(instance.parsed_expressions, [('age', '=', '1')])

    # def test_valid_expressions(self):
    #     # ExpressionFilter('google=1')
    #     # ExpressionFilter('google__eq=1')
    #     # ExpressionFilter({'google__eq': 1})
    #     # ExpressionFilter('google__id__eq=1')
    #     # ExpressionFilter([('google', 'id', 'eq', 1)])

    #     expressions = [
    #         ('age=1', ['age', '=', '1']),
    #         ('age__eq=1', ['age', '=', '1']),
    #         ('ages__id__eq=1', ['ages', 'id', '=', '1']),
    #         ([('ages', 'eq', 1)], [('ages', '=', 1)])
    #     ]
    #     for value_to_test, expected in expressions:
    #         with self.subTest(expression=value_to_test):
    #             instance = ExpressionFilter(value_to_test)
    #             print(instance.parsed_expressions)
    #             # self.assertListEqual(
    #             #     expression[1],
    #             #     instance.parsed_expressions
    #             # )
