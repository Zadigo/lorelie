import unittest
from lorelie.expressions import (CombinedExpression, F, NegatedExpression, Q,
                                 Value, When, Case)
from lorelie.test.testcases import LorelieTestCase


class TestQ(LorelieTestCase):
    def test_structure(self):
        instance = Q(firstname='Kendall')
        sql = instance.as_sql(self.create_connection())
        self.assertIsInstance(sql, list)
        self.assertListEqual(sql, ["firstname='Kendall'"])

        instance = Q(firstname='Kendall', lastname='Jenner')
        sql = instance.as_sql(self.create_connection())
        self.assertIsInstance(sql, list)
        self.assertListEqual(
            sql, ["firstname='Kendall' and lastname='Jenner'"])

        instance = Q(is_valid=True)
        sql = instance.as_sql(self.create_connection())
        print(sql)

    def test_and(self):
        a = Q(firstname='Kendall')
        b = Q(firstname='Kylie')
        c = a & b

        self.assertIsInstance(c, CombinedExpression)
        sql = c.as_sql(self.create_connection())

        self.assertIsInstance(sql, list)
        self.assertListEqual(
            sql, ["(firstname='Kendall' and firstname='Kylie')"])

    def test_or(self):
        a = Q(firstname='Kendall')
        b = Q(firstname='Kylie')
        c = a | b
        self.assertIsInstance(c, CombinedExpression)
        sql = c.as_sql(self.create_connection())
        self.assertIsInstance(sql, list)
        self.assertListEqual(
            sql, ["(firstname='Kendall' or firstname='Kylie')"])

    def test_multiple_filters(self):
        logic = Q(firstname='Kendall', age__gt=20, age__lte=50)
        result = logic.as_sql(self.create_connection())
        self.assertListEqual(
            result, ["firstname='Kendall' and age>20 and age<=50"])

    def test_multioperators(self):
        multi = (
            Q(firstname='Kendall') |
            Q(lastname='Jenner') &
            Q(age__gt=25, age__lte=56)
        )
        result = multi.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            ["(firstname='Kendall' or (lastname='Jenner' and age>25 and age<=56))"]
        )

    def test_negation(self):
        # NegatedExpression
        instance = ~Q(firstname='Kendall')
        self.assertIsInstance(instance, NegatedExpression)
        sql = instance.as_sql(self.create_connection())
        self.assertEqual(sql, "not firstname='Kendall'")

        # Q & NegatedExpression
        instance = ~Q(firstname='Kendall') & Q(lastname='Jenner')
        sql = instance.as_sql(self.create_connection())
        self.assertEqual(sql, "not firstname='Kendall' and lastname='Jenner'")

        # NegatedExpression & NegatedExpression
        instance = ~Q(firstname='Kendall') & ~Q(lastname='Jenner')
        sql = instance.as_sql(self.create_connection())
        self.assertEqual(sql, "not firstname='Kendall' and lastname='Jenner'")

    def test_mixed_expressions(self):
        # TODO: We should not be able to mix
        # different types of expressions
        instance = ~Q(name='Kendall') & F('name')
        sql = instance.as_sql(self.create_connection())
        print(sql)


class TestCombinedExpression(LorelieTestCase):
    def test_structure(self):
        a = Q(firstname='Kendall')
        b = Q(firstname='Kylie')

        instance = CombinedExpression(a, b)
        instance.build_children()

        self.assertTrue(len(instance.children) == 3)
        self.assertIsInstance(instance.children[0], Q)
        self.assertIsInstance(instance.children[1], str)
        self.assertTrue(instance.children[1] == 'and')
        self.assertIsInstance(instance.children[-1], Q)

        self.assertListEqual(
            instance.as_sql(self.create_connection()),
            ["(firstname='Kendall' and firstname='Kylie')"]
        )

    def test_mixed_expressions(self):
        a = Q(name='Kendall')
        b = F('age')

        instance = CombinedExpression(a, b)
        instance.build_children()
        self.assertListEqual(
            instance.as_sql(self.create_connection()),
            ["(name='Kendall' and age)"]
        )

    def test_F_expressions(self):
        a = F('age')
        b = F('age')

        instance = CombinedExpression(a, b)
        instance.build_children()
        self.assertListEqual(
            instance.as_sql(self.create_connection()),
            ['(age and age)']
        )

    def test_joining_combined_expressions(self):
        a = CombinedExpression(F('name'))
        b = CombinedExpression(F('age'))

        c = a & b
        # TODO: This returns  ['(and ())']
        print(c.as_sql(self.create_connection()))

    def test_joining_combined_expression_with_simple(self):
        a = CombinedExpression(Q(firstname='Kendall'))
        b = Q(age__gt=26)

        c = a & b
        print(c.as_sql(self.create_connection()))


class TestWhen(LorelieTestCase):
    def test_structure(self):
        instance = When(Q(name='Kendall'), 'Kylie')
        sql = instance.as_sql(self.create_connection())
        self.assertEqual(sql, "when name='Kendall' then 'Kylie'")

    def test_with_string(self):
        instance = When('name=Kendall', then_case='Kylie')
        sql = instance.as_sql(self.create_connection())
        self.assertEqual(sql, "when name='Kendall' then 'Kylie'")


class TestCase(LorelieTestCase):
    @unittest.expectedFailure
    def test_no_alias_name(self):
        condition = When('firstname=Kendall', 'kendall')
        case = Case(condition)
        case.as_sql(self.create_connection())

    def test_structure(self):
        condition = When('firstname=Kendall', 'Kylie')
        case = Case(condition, default='Aurélie')
        case.alias_field_name = 'firstname_alias'

        self.assertEqual(
            case.as_sql(self.create_connection()),
            "case when firstname='Kendall' then 'Kylie' else 'Aurélie' end firstname_alias"
        )


class TestFFunction(LorelieTestCase):
    def test_structure(self):
        result = F('name') + 'other'
        self.assertIsInstance(result, CombinedExpression)

    def test_resolution_methods(self):
        result = F('age') + 'height'
        sql = result.as_sql(self.create_connection())
        self.assertEqual(sql[0], "(age + 'height')")

        operations = [
            # age + 1
            F('age') + 1,
            # age - 1
            F('age') - 1,
            # age * 1
            F('age') * 1,
            # FIXME:  age / 1
            # result = F('age') / 1
            F('age') + F('age') + 1
        ]

        expected = [
            '(age + 1)',
            '(age - 1)',
            '(age x 1)',
            '(age / 1)',
            '(age + age + 1)'
        ]

        backend = self.create_connection()
        for operation in operations:
            with self.subTest(operation=operation):
                result = operation.as_sql(backend)
                self.assertIn(result[0], expected)


class TestValue(LorelieTestCase):
    def test_structure(self):
        instance = Value(1)
        sql = instance.as_sql(self.create_connection())
        self.assertEqual(sql, [1])

        instance = Value('a')
        sql = instance.as_sql(self.create_connection())
        self.assertEqual(sql, ["'a'"])

        instance = Value(1.2)
        sql = instance.as_sql(self.create_connection())
        self.assertEqual(sql, [1.2])

        # Callables are transformed to strings
        instance = Value(lambda: 'Kendall')
        sql = instance.as_sql(self.create_connection())
        self.assertIsInstance(sql[0], str)

        instance = Value(Q(age__gt=25))
        sql = instance.as_sql(self.create_connection())
        self.assertIsInstance(sql[0], str)
