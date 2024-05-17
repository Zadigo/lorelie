import unittest

from lorelie.backends import SQLiteBackend
from lorelie.fields.base import IntegerField
from lorelie.functions import ExtractYear, Length, Lower, Upper
from lorelie.tables import Table
from tests.db import create_table

table = Table('celebrities', fields=[IntegerField('age')])
table.backend = SQLiteBackend()


class TestFunctions(unittest.TestCase):
    def test_lower_function(self):
        table = create_table()
        instance = Lower('age')
        instance.backend = table.backend
        sql = instance.as_sql()
        expected_sql = 'lower(age)'
        self.assertEqual(sql, expected_sql)

    def test_upper_function(self):
        table = create_table()
        instance = Upper('age')
        instance.backend = table.backend
        sql = instance.as_sql()
        expected_sql = 'upper(age)'
        self.assertEqual(sql, expected_sql)

    def test_length_function(self):
        table = create_table()
        instance = Length('age')
        instance.backend = table.backend
        sql = instance.as_sql()
        expected_sql = 'length(age)'
        self.assertEqual(sql, expected_sql)

    def test_extract_year_function(self):
        table = create_table()
        instance = ExtractYear('date_of_birth')
        instance.backend = table.backend
        sql = instance.as_sql()
        expected_sql = "strftime('%Y', date_of_birth)"
        self.assertEqual(sql, expected_sql)


if __name__ == '__main__':
    unittest.main()
