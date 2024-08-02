from lorelie.backends import BaseRow
from lorelie.queries import Query, QuerySet
from lorelie.test.testcases import LorelieTestCase


class TestQuerySet(LorelieTestCase):
    def test_structure(self):
        # Determine how queryset should act if there
        # is no sql statements in the query
        query = Query(backend=self.create_connection())
        qs = QuerySet(query)
        self.assertFalse(qs.query.is_evaluated)

        list(qs)
        self.assertTrue(qs.query.is_evaluated)
        # We did not add any node which makes that
        # the statement is just a semicolon
        self.assertEqual(qs.sql_statement, ';')

    def test_queryset_is_lazy(self):
        db = self.create_database()
        table = db.get_table('celebrities')
        qs = table.objects.all()
        self.assertFalse(qs.query.is_evaluated)

    def test_with_nodes(self):
        db = self.create_database()
        qs = db.celebrities.objects.all()

        list(qs)

        self.assertEqual(
            qs.query.sql,
            "select * from celebrities;"
        )

        # backend = self.create_connection()

        # table = self.create_table()
        # table.backend = backend

        # query = Query(backend=backend)
        # query.add_sql_node(SelectNode(table, 'name', 'age'))

        # queryset = QuerySet(query)
        # list(queryset)

        # print(queryset.sql_statement)

    def test_dunders(self):
        db = self.create_database()
        db.celebrities.objects.create(name='Kendall', height=203)
        qs = db.celebrities.objects.all()

        item = qs[0]
        self.assertIsInstance(item, BaseRow)

        for item in qs:
            with self.subTest(item=item):
                self.assertIsInstance(item, BaseRow)

        # TODO: Raises an error
        # result = 'Kendall' in qs
        # self.assertTrue(result)

        # TODO: Raises recursion error
        # self.assertTrue(qs == qs)

        self.assertTrue(len(qs) == 1)

        # TODO: Raises an error
        # self.assertIsInstance(qs.dataframe, pandas.DataFrame)
