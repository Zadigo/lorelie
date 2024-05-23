# import unittest


# from lorelie.backends import SQLiteBackend
# from lorelie.fields.base import IntegerField
# from lorelie.database.functions.dates import (ExtractDay, ExtractHour, ExtractMinute,
#                                               ExtractMonth, ExtractYear)
# from lorelie.tables import Table

# backend = SQLiteBackend()
# table = Table('celebrities', fields=[IntegerField('age')])
# table.backend = backend
# backend.set_current_table(table)


# class TestDates(unittest.TestCase):
#     def test_extract_day_function(self):
#         instance = ExtractDay('date_of_birth')
#         sql = instance.as_sql(backend)
#         expected_sql = "strftime('%d', date_of_birth)"
#         self.assertEqual(sql, expected_sql)

#     def test_extract_month_function(self):
#         instance = ExtractMonth('date_of_birth')
#         sql = instance.as_sql(backend)
#         expected_sql = "strftime('%m', date_of_birth)"
#         self.assertEqual(sql, expected_sql)

#     def test_extract_year_function(self):
#         instance = ExtractYear('date_of_birth')
#         sql = instance.as_sql(backend)
#         expected_sql = "strftime('%Y', date_of_birth)"
#         self.assertEqual(sql, expected_sql)

#     def test_extract_hour_function(self):
#         instance = ExtractHour('date_of_birth')
#         sql = instance.as_sql(backend)
#         expected_sql = "strftime('%H', date_of_birth)"
#         self.assertEqual(sql, expected_sql)

#     def test_extract_minute_function(self):
#         instance = ExtractMinute('date_of_birth')
#         sql = instance.as_sql(backend)
#         expected_sql = "strftime('%M', date_of_birth)"
#         self.assertEqual(sql, expected_sql)


# if __name__ == '__main__':
#     unittest.main()
