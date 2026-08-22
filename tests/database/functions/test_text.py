from unittest.mock import MagicMock

from lorelie.backends import SQL
from lorelie.database.functions import (
    Count,
    ExtractYear,
    Length,
    Lower,
    Max,
    Min,
    Upper,
)
from lorelie.database.tables.base import Table
from lorelie.test.testcases import LorelieTestCase


class TestFunctionsSQL(LorelieTestCase):
    def setUp(self):
        instance = SQL()
        instance.table = MagicMock(spec=Table)
        self.sql_backend = instance
        # Just for the purpose of testing,
        # implement a FakeTable on the SQL
        # mixin class. Technically the table
        # would be on SQLiteBackend class

    def test_lower_sql(self):
        instance = Lower('name')
        instance.backend = self.sql_backend
        result = instance.as_sql(self.sql_backend)
        self.assertTrue(result == 'lower(name)')

    def test_upper_sql(self):
        instance = Upper('name')
        instance.backend = self.sql_backend
        result = instance.as_sql(self.sql_backend)
        self.assertTrue(result == 'upper(name)')

    def test_length_sql(self):
        instance = Length('name')
        instance.backend = self.sql_backend
        result = instance.as_sql(self.sql_backend)
        self.assertTrue(result == 'length(name)')

    def test_max_sql(self):
        instance = Max('id')
        instance.backend = self.sql_backend
        result = instance.as_sql(self.sql_backend)
        expected_result = 'max(id)'
        self.assertTrue(result == expected_result, f'Got: {result}')

    def test_min_sql(self):
        instance = Min('id')
        instance.backend = self.sql_backend
        result = instance.as_sql(self.sql_backend)
        expected_result = 'min(id)'
        self.assertTrue(result == expected_result, f'Got: {result}')

    def test_extract_year(self):
        instance = ExtractYear('date_of_birth')
        instance.backend = self.sql_backend
        result = instance.as_sql(self.sql_backend)
        expected_result = "strftime('%Y', date_of_birth)"
        self.assertTrue(result == expected_result)

    def test_count_sql(self):
        instance = Count('name')
        instance.backend = self.sql_backend
        result = instance.as_sql(self.sql_backend)
        self.assertTrue(result == 'count(name)')
