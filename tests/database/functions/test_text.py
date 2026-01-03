from lorelie.test.testcases import LorelieTestCase


class TestFunctionsSQL(LorelieTestCase):
    def setUp(self):
        instance = SQL()
        setattr(instance, 'table', FakeTable())
        self.sql_backend = instance
        # Just for the purpose of testing,
        # implement a FakeTable on the SQL
        # mixin class. Technically the table
        # would be on SQLiteBackend class

    def test_lower_sql(self):
        instance = Lower('name')
        instance.backend = self.sql_backend
        result = instance.as_sql()
        self.assertTrue(result == 'lower(name)')

    def test_upper_sql(self):
        instance = Upper('name')
        instance.backend = self.sql_backend
        result = instance.as_sql()
        self.assertTrue(result == 'upper(name)')

    def test_length_sql(self):
        instance = Length('name')
        instance.backend = self.sql_backend
        result = instance.as_sql()
        self.assertTrue(result == 'length(name)')

    def test_max_sql(self):
        instance = Max('id')
        instance.backend = self.sql_backend
        result = instance.as_sql()
        expected_result = 'select rowid, * from fake_table where id=(select max(id) from fake_table)'
        self.assertTrue(result == expected_result)

    def test_min_sql(self):
        instance = Min('id')
        instance.backend = self.sql_backend
        result = instance.as_sql()
        expected_result = 'select rowid, * from fake_table where id=(select min(id) from fake_table)'
        self.assertTrue(result == expected_result)

    def test_extract_year(self):
        instance = ExtractYear('date_of_birth')
        instance.backend = self.sql_backend
        result = instance.as_sql()
        expected_result = "strftime('%Y', date_of_birth)"
        self.assertTrue(result == expected_result)

    def test_count_sql(self):
        instance = Count('name')
        instance.backend = self.sql_backend
        result = instance.as_sql()
        self.assertTrue(result == 'count(name)')
