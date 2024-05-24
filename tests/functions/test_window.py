from lorelie.database.functions.window import CumeDist, PercentRank, Rank, Window
from lorelie.test.testcases import LorelieTestCase


class TestRank(LorelieTestCase):
    def test_structure(self):
        instance = Window(Rank('age'), order_by='age')
        sql = instance.as_sql(self.create_connection())
        self.assertEqual(sql, 'rank() over (order by age) as window_rank_age')

    def test_query_window(self):
        db = self.create_database()
        db.objects.create('celebrities', name='Julie', height=167)
        db.objects.create('celebrities', name='Julie', height=195)
        db.objects.create('celebrities', name='Julie', height=199)

        window = Window(Rank('height'))
        qs = db.objects.annotate('celebrities', rank_height=window)
        values = qs.values('height', 'rank_height')
        print(values)


class TestPercentRank(LorelieTestCase):
    def test_structure(self):
        instance = Window(function=PercentRank('age'), order_by='age')
        sql = instance.as_sql(self.create_connection())
        self.assertEqual(
            sql,
            'percent_rank() over (order by age) as window_percentrank_age'
        )


class TestCumeDist(LorelieTestCase):
    def test_structure(self):
        instance = Window(function=CumeDist('age'),
                          partition_by='age', order_by='age')
        sql = instance.as_sql(self.create_connection())
        self.assertEqual(
            sql,
            'cume_dist() over (partition by age order by age) as window_cumedist_age'
        )

# class TestWindowFunctions(unittest.TestCase):
#     def test_window_function_with_string(self):
#         window = Window(expression=Rank('age'))
#         self.assertEqual(
#             window.as_sql(table.backend),
#             'rank() over (order by age) as window_rank_age'
#         )

#     def test_rank(self):
#         window = Window(
#             expression=Rank(Length('name')),
#             order_by='name'
#         )
#         expected_sql = 'rank() over (order by length(name)) as window_rank_name'
#         self.assertEqual(
#             expected_sql,
#             window.as_sql(table.backend)
#         )

#     def test_percent_rank(self):
#         window = Window(
#             expression=PercentRank(Length('name')),
#             order_by='name'
#         )
#         expected_sql = 'percent_rank() over (order by length(name)) as window_percentrank_name'
#         self.assertEqual(
#             expected_sql,
#             window.as_sql(table.backend)
#         )


# if __name__ == '__main__':
#     unittest.main()
