import unittest

from database.functions.window import PercentRank, Rank, Window

from lorelie.backends import SQLiteBackend
from lorelie.database.functions.text import Length
from lorelie.fields.base import IntegerField
from lorelie.tables import Table

backend = SQLiteBackend()
table = Table('celebrities', fields=[IntegerField('age')])
table.backend = backend
backend.set_current_table(table)



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
