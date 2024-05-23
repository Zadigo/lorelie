# import unittest

# from lorelie.backends import SQLiteBackend
# from lorelie.database.functions.aggregation import (Avg,
#                                                     CoefficientOfVariation,
#                                                     Count, Max,
#                                                     MeanAbsoluteDifference,
#                                                     Min, StDev, Sum, Variance)
# from lorelie.database.functions.text import Length
# from lorelie.fields.base import IntegerField
# from lorelie.tables import Table

# backend = SQLiteBackend()
# table = Table('celebrities', fields=[IntegerField('age')])
# table.backend = backend
# backend.set_current_table(table)

# # select rowid, * from celebrities where age=(select max(age) from celebrities)


# class TestAggregation(unittest.TestCase):
#     def test_max_aggregation(self):
#         instance = Max('age')
#         sql = instance.as_sql(table.backend)
#         expected_sql = "max(age)"
#         self.assertEqual(sql, expected_sql)

#     def test_min_aggregation(self):
#         instance = Min('age')
#         sql = instance.as_sql(table.backend)
#         expected_sql = "min(age)"
#         self.assertEqual(sql, expected_sql)

#     def test_avg_aggregation(self):
#         instance = Avg('age')
#         sql = instance.as_sql(table.backend)
#         expected_sql = "avg(age)"
#         self.assertEqual(sql, expected_sql)

#     def test_variance_aggregation(self):
#         instance = Variance('age')
#         sql = instance.as_sql(table.backend)
#         expected_sql = "variance(age)"
#         self.assertEqual(sql, expected_sql)

#     def test_std_deviation_aggregation(self):
#         instance = StDev('age')
#         sql = instance.as_sql(table.backend)
#         expected_sql = "stdev(age)"
#         self.assertEqual(sql, expected_sql)

#     def test_sum_aggregation(self):
#         instance = Sum('age')
#         sql = instance.as_sql(table.backend)
#         expected_sql = "sum(age)"
#         self.assertEqual(sql, expected_sql)

#     def test_count_aggregation(self):
#         instance = Count('age')
#         sql = instance.as_sql(table.backend)
#         expected_sql = "count(age)"
#         self.assertEqual(sql, expected_sql)

#     def test_coefficient_of_variation_aggregation(self):
#         instance = CoefficientOfVariation('age')
#         sql = instance.as_sql(table.backend)
#         expected_sql = "coeffofvariation(age)"
#         self.assertEqual(sql, expected_sql)

#     def test_mean_absolute_difference_aggregation(self):
#         instance = MeanAbsoluteDifference('age')
#         sql = instance.as_sql(table.backend)
#         expected_sql = "meanabsdifference(age)"
#         self.assertEqual(sql, expected_sql)

#     def test_nested_aggregation(self):
#         nested = Max(Length('name'))
#         sql = nested.as_sql(table.backend)
#         self.assertEqual(sql, 'max(length(name))')
