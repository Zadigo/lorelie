from lorelie.database.expressions.mixins import ExpressionFiltersMixin
from lorelie.database.expressions.base import Expression
from lorelie.test.testcases import LorelieTestCase


class TestExpressionFilterMixin(LorelieTestCase):
    def setUp(self):
        self.instance = ExpressionFiltersMixin()

    def test_translate_operator_from_tokens(self):
        translation = self.instance.translate_operator_from_tokens(
            ['age', 'eq', 1]
        )
        lhv, operator, rhv = translation

        self.assertEqual(lhv, 'age')
        self.assertEqual(operator, '=')
        self.assertEqual(rhv, 1)

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
            ({'age': 18, 'firstname': 'Kendall'}, ['age', 'firstname'])
        ]

        for value_to_test, expected in expressions:
            with self.subTest(expression=value_to_test):
                columns = self.instance.decompose_filters_columns(
                    value_to_test)
                self.assertListEqual(columns, expected)

    def test_decompose_filters_from_string(self):
        expressions = [
            ('age=1', [('age', '=', '1')]),
            ('age__eq=1', [('age', '=', '1')]),
            ('ages__id__eq=1', [('ages', 'id', '=', '1')])
        ]

        for value_to_test, expected in expressions:
            with self.subTest(expression=value_to_test):
                decomposition = self.instance.decompose_filters_from_string(
                    value_to_test)
                self.assertListEqual(decomposition, expected)

    def test_decompose_filters(self):
        expressions = [
            ({'age__eq': 1}, [('age', '=', 1)]),
            ({'ages__id__eq': 1}, [('ages', 'id', '=', 1)]),
            ({'age': 18, 'firstname': 'Kendall'},
             [('age', '=', 18), ('firstname', '=', 'Kendall')])
        ]

        for value_to_test, expected in expressions:
            with self.subTest(expression=value_to_test):
                decomposition = self.instance.decompose_filters(
                    **value_to_test)
                self.assertListEqual(decomposition, expected)


class TestExpression(LorelieTestCase):
    def test_build_filters(self):
        table = self.create_table()

        items = [
            [[('age', 'eq', 1)], [('age', 'eq', 1)]],
            [{'age__eq': 1}, [('age', '=', 1)]],
            # [{'age__eq': 1, 'score__gte': 10}, [('age', '=', '1')]],
            # ['age=1', [('age', '=', '1')]],
        ]

        for value, expected in items:
            with self.subTest(item=value):
                expression = Expression(value, table)
                self.assertEqual(expression.parsed_expressions, expected)

                print(expression.build_filters(value))
