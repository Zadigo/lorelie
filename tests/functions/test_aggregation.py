import unittest

from lorelie.backends import SQLiteBackend
from lorelie.database.functions.aggregation import (Avg,
                                                    CoefficientOfVariation,
                                                    Count, Max,
                                                    MeanAbsoluteDifference,
                                                    Min, StDev, Sum, Variance)
from lorelie.fields.base import IntegerField
from lorelie.tables import Table

backend = SQLiteBackend()
table = Table('celebrities', fields=[IntegerField('age')])
table.backend = backend
backend.set_current_table(table)


class TestAggregation(unittest.TestCase):
    pass


if __name__ == '__main__':
    unittest.main()
