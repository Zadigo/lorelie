

import datetime

from lorelie.database.functions.dates import (Extract, ExtractDay, ExtractHour,
                                              ExtractMinute, ExtractMonth,
                                              ExtractYear)
from lorelie.test.testcases import LorelieTestCase


class TestDates(LorelieTestCase):
    def test_structure(self):
        db = self.create_database()
        db.celebrities.objects.create(
            'celebrities', name='Kendall', height=202)

        queryset = db.celebrities.objects.annotate(
            'celebrities',
            year=Extract('created_on', 'year')
        )
        item = queryset[0]
        current_year = datetime.datetime.now().year
        self.assertEqual(item.year, str(current_year))

    def test_extract_day_function(self):
        connection = self.create_connection()
        instance = ExtractDay('date_of_birth')
        sql = instance.as_sql(connection)
        expected_sql = "strftime('%d', date_of_birth)"
        self.assertEqual(sql, expected_sql)

    def test_extract_month_function(self):
        connection = self.create_connection()
        instance = ExtractMonth('date_of_birth')
        sql = instance.as_sql(connection)
        expected_sql = "strftime('%m', date_of_birth)"
        self.assertEqual(sql, expected_sql)

    def test_extract_year_function(self):
        connection = self.create_connection()
        instance = ExtractYear('date_of_birth')
        sql = instance.as_sql(connection)
        expected_sql = "strftime('%Y', date_of_birth)"
        self.assertEqual(sql, expected_sql)

    def test_extract_hour_function(self):
        connection = self.create_connection()
        instance = ExtractHour('date_of_birth')
        sql = instance.as_sql(connection)
        expected_sql = "strftime('%H', date_of_birth)"
        self.assertEqual(sql, expected_sql)

    def test_extract_minute_function(self):
        connection = self.create_connection()
        instance = ExtractMinute('date_of_birth')
        sql = instance.as_sql(connection)
        expected_sql = "strftime('%M', date_of_birth)"
        self.assertEqual(sql, expected_sql)
