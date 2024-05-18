import unittest

from database._functions.window import PercentRank, Rank, Window

from lorelie.backends import SQLiteBackend
from lorelie.fields.base import IntegerField
from lorelie.functions import (Concat, ExtractDay, ExtractHour, ExtractMinute,
                               ExtractMonth, ExtractYear, Length, Lower, LTrim,
                               MD5Hash, RTrim, SHA256Hash, SubStr, Trim, Upper)
from lorelie.tables import Table

backend = SQLiteBackend()
table = Table('celebrities', fields=[IntegerField('age')])
table.backend = backend
backend.set_current_table(table)


class TestFunctions(unittest.TestCase):
    def test_lower_function(self):
        instance = Lower('age')
        sql = instance.as_sql(backend)
        expected_sql = 'lower(age)'
        self.assertEqual(sql, expected_sql)

    def test_upper_function(self):
        instance = Upper('age')
        sql = instance.as_sql(backend)
        expected_sql = 'upper(age)'
        self.assertEqual(sql, expected_sql)

    def test_length_function(self):
        instance = Length('age')
        sql = instance.as_sql(backend)
        expected_sql = 'length(age)'
        self.assertEqual(sql, expected_sql)

    def test_extract_day_function(self):
        instance = ExtractDay('date_of_birth')
        sql = instance.as_sql(backend)
        expected_sql = "strftime('%d', date_of_birth)"
        self.assertEqual(sql, expected_sql)

    def test_extract_month_function(self):
        instance = ExtractMonth('date_of_birth')
        sql = instance.as_sql(backend)
        expected_sql = "strftime('%m', date_of_birth)"
        self.assertEqual(sql, expected_sql)

    def test_extract_year_function(self):
        instance = ExtractYear('date_of_birth')
        sql = instance.as_sql(backend)
        expected_sql = "strftime('%Y', date_of_birth)"
        self.assertEqual(sql, expected_sql)

    def test_extract_hour_function(self):
        instance = ExtractHour('date_of_birth')
        sql = instance.as_sql(backend)
        expected_sql = "strftime('%H', date_of_birth)"
        self.assertEqual(sql, expected_sql)

    def test_extract_minute_function(self):
        instance = ExtractMinute('date_of_birth')
        sql = instance.as_sql(backend)
        expected_sql = "strftime('%M', date_of_birth)"
        self.assertEqual(sql, expected_sql)

    def test_md5_hash_function(self):
        instance = MD5Hash('name')
        sql = instance.as_sql(backend)
        expected_sql = "hash(name)"
        self.assertEqual(sql, expected_sql)

        instance = SHA256Hash('name')
        sql = instance.as_sql(backend)
        expected_sql = "sha256(name)"
        self.assertEqual(sql, expected_sql)

    def test_trims(self):
        instances = [
            ('trim(name)', Trim('name')),
            ('ltrim(name)', LTrim('name')),
            ('rtrim(name)', RTrim('name')),
            ('substr(name, 1, 1)', SubStr('name', 1, 1)),
            # ('concat(name)', Concat)
        ]

        for instance in instances:
            expected_sql, klass = instance
            with self.subTest(expected_sql=expected_sql, klass=klass):
                sql = klass.as_sql(table.backend)
                self.assertEqual(expected_sql, sql)


class TestWindowFunctions(unittest.TestCase):
    def test_window_function_with_string(self):
        window = Window(expression=Rank('age'))
        self.assertEqual(
            window.as_sql(table.backend),
            'rank() over (order by age) as window_rank_age'
        )

    def test_rank(self):
        window = Window(
            expression=Rank(Length('name')),
            order_by='name'
        )
        expected_sql = 'rank() over (order by length(name)) as window_rank_name'
        self.assertEqual(
            expected_sql,
            window.as_sql(table.backend)
        )

    def test_percent_rank(self):
        window = Window(
            expression=PercentRank(Length('name')),
            order_by='name'
        )
        expected_sql = 'percent_rank() over (order by length(name)) as window_percentrank_name'
        self.assertEqual(
            expected_sql,
            window.as_sql(table.backend)
        )


if __name__ == '__main__':
    unittest.main()
