
from lorelie.database.functions.aggregation import (Avg,
                                                    CoefficientOfVariation,
                                                    Count, Max,
                                                    MeanAbsoluteDifference,
                                                    Min, StDev, Sum, Variance)
from lorelie.database.functions.text import Length
from lorelie.test.testcases import LorelieTestCase

# select rowid, * from celebrities where age=(select max(age) from celebrities)


class TestAggregation(LorelieTestCase):
    def test_max(self):
        table = self.create_table()
        table.backend = self.create_connection()

        instance = Max('age')
        sql = instance.as_sql(table.backend)
        self.assertEqual(sql, "max(age)")

    def test_min(self):
        table = self.create_table()
        table.backend = self.create_connection()

        instance = Min('age')
        sql = instance.as_sql(table.backend)
        self.assertEqual(sql, "min(age)")

    def test_avg(self):
        table = self.create_table()
        table.backend = self.create_connection()

        instance = Avg('age')
        sql = instance.as_sql(table.backend)
        self.assertEqual(sql, "avg(age)")

    def test_variance(self):
        table = self.create_table()
        table.backend = self.create_connection()

        instance = Variance('age')
        sql = instance.as_sql(table.backend)
        self.assertEqual(sql, "variance(age)")

    def test_stdev(self):
        table = self.create_table()
        table.backend = self.create_connection()

        instance = StDev('age')
        sql = instance.as_sql(table.backend)
        self.assertEqual(sql, "stdev(age)")

    def test_sum(self):
        table = self.create_table()
        table.backend = self.create_connection()

        instance = Sum('age')
        sql = instance.as_sql(table.backend)
        self.assertEqual(sql, "sum(age)")

    def test_count(self):
        table = self.create_table()
        table.backend = self.create_connection()

        instance = Count('age')
        sql = instance.as_sql(table.backend)
        self.assertEqual(sql, "count(age)")

    def test_coefficient_of_variation(self):
        table = self.create_table()
        table.backend = self.create_connection()

        instance = CoefficientOfVariation('age')
        sql = instance.as_sql(table.backend)
        self.assertEqual(sql, "coeffofvariation(age)")

    def test_mean_absolute_difference(self):
        table = self.create_table()
        table.backend = self.create_connection()

        instance = MeanAbsoluteDifference('age')
        sql = instance.as_sql(table.backend)
        self.assertEqual(sql, "meanabsdifference(age)")

    def test_aggregation_from_function(self):
        table = self.create_table()
        table.backend = self.create_connection()

        nested = Max(Length('name'))
        sql = nested.as_sql(table.backend)
        self.assertEqual(sql, 'max(length(name))')
