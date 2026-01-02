from lorelie.database.functions.window import (CumeDist, FirstValue, LastValue, NTile, PercentRank, Rank,
                                               Window)
from lorelie.test.testcases import LorelieTestCase

from tests.items import create_random_celebrities


class WindowMixin(LorelieTestCase):
    def setUp(self):
        db = self.create_database()
        self.table = db.get_table('celebrities')
        self.table.objects.bulk_create(create_random_celebrities(2))


class TestRank(WindowMixin):
    def test_structure(self):
        instance = Window(Rank('age'), order_by='age')
        sql = instance.as_sql(self.create_connection())
        self.assertEqual(sql, 'rank() over (order by age)')

    def test_query_window(self):
        window = Window(Rank('height'), order_by='-height')
        qs = self.table.objects.annotate(rank_height=window)

        values = qs.values('height', 'rank_height')
        print(values)
        # self.assertIn('rank_height', values[0])


class TestPercentRank(WindowMixin):
    def test_structure(self):
        instance = Window(function=PercentRank('height'), order_by='height')
        sql = instance.as_sql(self.create_connection())
        self.assertEqual(
            sql,
            'percent_rank() over (order by height)'
        )

    def test_query_window(self):
        window = Window(PercentRank('height'))
        qs = self.table.objects.annotate(percent_rank=window)
        values = qs.values('height', 'percent_rank')
        print(values)
        # self.assertIn('percent_rank', values[0])


class TestCumeDist(WindowMixin):
    def test_structure(self):
        instance = Window(
            CumeDist('height'),
            partition_by='height',
            order_by='height'
        )
        sql = instance.as_sql(self.create_connection())
        self.assertEqual(
            sql,
            'cume_dist() over (partition by height order by height)'
        )

    def test_query_window(self):
        window = Window(CumeDist('height'))

        qs = self.table.objects.annotate(cume_dist=window)
        values = qs.values('height', 'cume_dist')

        print(values)
        # self.assertIn('cume_dist', values[0])


class TestFirstValue(WindowMixin):
    def test_structure(self):
        window = Window(FirstValue('height'), order_by='height')
        expected_sql = 'first_value(height) over (order by height)'
        self.assertEqual(
            window.as_sql(self.create_connection()),
            expected_sql
        )

    def test_query_window(self):
        window = Window(FirstValue('height'))

        qs = self.table.objects.annotate(first_value=window)
        values = qs.values('height', 'first_value')

        print(values)
        # self.assertIn('first_value', values[0])


class TestLastValue(WindowMixin):
    def test_structure(self):
        window = Window(LastValue('height'), order_by='height')
        expected_sql = 'last_value(height) over (order by height)'
        self.assertEqual(
            window.as_sql(self.create_connection()),
            expected_sql
        )

    def test_query_window(self):
        window = Window(LastValue('height'))

        qs = self.table.objects.annotate(last_value=window)
        values = qs.values('height', 'last_value')

        print(values)
        # self.assertIn('first_value', values[0])


class TestNTile(WindowMixin):
    def test_structure(self):
        window = Window(NTile(1), order_by='height')
        expected_sql = 'ntile(1) over (order by height)'
        self.assertEqual(
            window.as_sql(self.create_connection()),
            expected_sql
        )

    def test_query_window(self):
        window = Window(NTile('height'))

        qs = self.table.objects.annotate(ntile=window)
        values = qs.values('height', 'ntile')

        print(values)
        # self.assertIn('first_value', values[0])
