import re
import unittest

from lorelie.backends import SQLiteBackend
from lorelie.expressions import Q, Case, CombinedExpression, When

backend = SQLiteBackend()


class TestQ(unittest.TestCase):
    def test_structure(self):
        instance = Q(firstname='Kendall')
        sql = instance.as_sql(backend)
        self.assertIsInstance(sql, list)
        self.assertListEqual(sql, ["firstname='Kendall'"])
        self.assertRegex(
            sql[0],
            r"^firstname\=\'Kendall\'$"
        )

    def test_and(self):
        a = Q(firstname='Kendall')
        b = Q(firstname='Kylie')
        c = a & b
        self.assertIsInstance(c, CombinedExpression)
        sql = c.as_sql(backend)
        self.assertIsInstance(sql, list)
        self.assertListEqual(
            sql,
            ["(firstname='Kendall' and firstname='Kylie')"]
        )

        self.assertRegex(
            sql[0],
            r"^\(firstname\=\'Kendall'\sand\sfirstname\=\'Kylie\'\)$"
        )

    def test_or(self):
        a = Q(firstname='Kendall')
        b = Q(firstname='Kylie')
        c = a | b
        self.assertIsInstance(c, CombinedExpression)
        sql = c.as_sql(backend)
        self.assertIsInstance(sql, list)
        self.assertListEqual(
            sql,
            ["(firstname='Kendall' or firstname='Kylie')"]
        )

    def test_multioperators(self):
        pass


class TestCombinedExpression(unittest.TestCase):
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
            instance.as_sql(backend),
            ["(firstname='Kendall' and firstname='Kylie')"]
        )


class TestWhen(unittest.TestCase):
    def test_structure(self):
        instance = When('firstname=Kendall', 'kendall')
        sql = instance.as_sql(backend)
        self.assertIsInstance(sql, list)
        self.assertRegex(
            sql,
            r"^when\sfirstname\=\'Kendall\'\sthen\s\'kendall\'$"
        )


class TestCase(unittest.TestCase):
    @unittest.expectedFailure
    def test_no_alias_name(self):
        condition = When('firstname=Kendall', 'kendall')
        case = Case(condition)
        case.as_sql(backend)

    def test_structure(self):
        condition = When('firstname=Kendall', 'kendall')
        case = Case(condition)
        case.alias_name = 'firstname_alias'

        sql = case.as_sql(backend)
        self.assertIsInstance(sql, list)
        self.assertRegex(
            sql,
            r"^when\sfirstname\=\'Kendall\'\sthen\s\'kendall\'$"
        )


if __name__ == '__main__':
    unittest.main()
