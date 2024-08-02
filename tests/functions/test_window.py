from lorelie.database.functions.text import Length
from lorelie.database.functions.window import (CumeDist, DenseRank, FirstValue,
                                               Lag, LastValue, Lead, NthValue,
                                               NTile, PercentRank, Rank,
                                               RowNumber, Window)
from lorelie.test.testcases import LorelieTestCase


class WindowMixin:
    def setUp(self):
        self.db = db = self.create_database()
        db.celebrities.objects.create(name='Julie', height=167)
        db.celebrities.objects.create(name='Julie', height=195)
        db.celebrities.objects.create(name='Julie', height=199)


class TestRank(WindowMixin, LorelieTestCase):
    def test_structure(self):
        instance = Window(Rank('age'), order_by='age')
        sql = instance.as_sql(self.create_connection())
        self.assertEqual(sql, 'rank() over (order by age)')

    def test_query_window(self):
        window = Window(Rank('height'), order_by='rank_height')
        qs = self.db.celebrities.objects.annotate(rank_height=window)
        values = qs.values('height', 'rank_height')
        self.assertIn('rank_height', values[0])


class TestPercentRank(WindowMixin, LorelieTestCase):
    def test_structure(self):
        instance = Window(function=PercentRank('age'), order_by='age')
        sql = instance.as_sql(self.create_connection())
        self.assertEqual(
            sql,
            'percent_rank() over (order by age)'
        )

    def test_query_window(self):
        window = Window(PercentRank('height'))
        qs = self.db.celebrities.objects.annotate(percent_rank=window)
        values = qs.values('height', 'percent_rank')
        self.assertIn('percent_rank', values[0])


class TestCumeDist(WindowMixin, LorelieTestCase):
    def test_structure(self):
        instance = Window(
            CumeDist('age'),
            partition_by='age',
            order_by='age'
        )
        sql = instance.as_sql(self.create_connection())
        self.assertEqual(
            sql,
            'cume_dist() over (partition by age order by age)'
        )

    def test_query_window(self):
        window = Window(CumeDist('height'))
        qs = self.db.celebrities.objects.annotate(cume_dist=window)
        values = qs.values('height', 'cume_dist')
        self.assertIn('cume_dist', values[0])


class TestWindowFunctions(LorelieTestCase):
    def test_window_function_with_string(self):
        window = Window(Rank('age'))
        self.assertEqual(
            window.as_sql(self.create_connection()),
            'rank() over (order by age)'
        )

    def test_rank(self):
        window = Window(Rank(Length('name')), order_by='name')
        expected_sql = 'rank() over (order by length(name))'
        self.assertEqual(
            window.as_sql(self.create_connection()),
            expected_sql
        )

    def test_percent_rank(self):
        window = Window(
            PercentRank(Length('name')),
            order_by='name'
        )
        expected_sql = 'percent_rank() over (order by length(name))'
        self.assertEqual(
            window.as_sql(self.create_connection()),
            expected_sql,
        )
