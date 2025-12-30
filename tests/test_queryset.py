from unittest.mock import MagicMock
from lorelie.backends import SQLiteBackend
from lorelie.queries import QuerySet
from lorelie.queries import Query, log_queries
from lorelie.test.testcases import LorelieTestCase
from lorelie.database.tables.base import Table


class TestQuerySet(LorelieTestCase):
    @classmethod
    def setUpClass(cls):
        statements = [
            'create table if not exists celebrities (id integer primary key autoincrement not null, name text);',
            "insert into celebrities (name) values('Kylie Jenner'), ('Kendall Jenner'), ('Addison Rae');"
        ]

        backend = SQLiteBackend()
        mtable = MagicMock(name='Table', spec=Table)
        type(mtable).name = 'celebrities'
        type(mtable).backend = backend

        mfield = MagicMock(name='Field', spec=['name', 'to_python'])
        mfield.to_python.side_effect = lambda x: x

        mtable.get_field.return_value = mfield

        query = Query.run_transaction(
            table=mtable,
            backend=backend,
            sql_tokens=statements
        )

        cls.qs = QuerySet(query)

    def test_structure(self):
        self.assertTrue(len(self.qs) > 0)
        self.assertIsNotNone(self.qs.query)
        self.assertIsInstance(self.qs.query, Query)

    def test_get_item(self):
        row = self.qs[0]
        self.assertIsNotNone(row)
        self.assertEqual(row['name'], 'Kylie Jenner')

    def test_iteration(self):
        pass

    def test_contains(self):
        pass

    def test_equality(self):
        pass

    def test_length(self):
        pass

    def test_dataframe(self):
        pass

    def test_sql_statement(self):
        pass

    def test_check_alias_view_names(self):
        pass

    def test_load_cache(self):
        pass

    def test_first(self):
        row = self.qs.first()
        print(row)

    def test_last(self):
        row = self.qs.last()
        self.assertIsNotNone(row)
        print(row)
        print(list(log_queries))

    def test_filter(self):
        qs = self.qs.filter(name='Kendall Jenner')
        for row in qs:
            print(row)

    def test_get(self):
        row = self.qs.get(id=2)
        print(row)

    def test_annotate(self):
        pass

    def test_all(self):
        qs = self.qs.all()

    def test_values(self):
        values = self.qs.values('name')
        print(values)

    def test_get_dataframe(self):
        pass

    def test_aggegate(self):
        pass

    def test_count(self):
        pass

    def test_exclude(self):
        pass

    def test_update(self):
        pass

    def test_exists(self):
        pass

    def test_skip_transforms(self):
        pass

    def test_order_by(self):
        qs = self.qs.order_by('-name')
        for row in qs:
            print(row)

    # def test_structure(self):
    #     # Determine how queryset should act if there
    #     # is no sql statements in the query
    #     query = Query(backend=self.create_connection())
    #     qs = QuerySet(query)
    #     self.assertFalse(qs.query.is_evaluated)

    #     list(qs)
    #     self.assertTrue(qs.query.is_evaluated)
    #     # We did not add any node which makes that
    #     # the statement is just a semicolon
    #     self.assertEqual(qs.sql_statement, ';')

    # def test_queryset_is_lazy(self):
    #     db = self.create_database()
    #     table = db.get_table('celebrities')
    #     qs = table.objects.all()
    #     self.assertFalse(qs.query.is_evaluated)

    # def test_with_nodes(self):
    #     db = self.create_database()
    #     qs = db.celebrities.objects.all()

    #     list(qs)

    #     self.assertEqual(
    #         qs.query.sql,
    #         "select * from celebrities;"
    #     )

    #     # backend = self.create_connection()

    #     # table = self.create_table()
    #     # table.backend = backend

    #     # query = Query(backend=backend)
    #     # query.add_sql_node(SelectNode(table, 'name', 'age'))

    #     # queryset = QuerySet(query)
    #     # list(queryset)

    #     # print(queryset.sql_statement)

    # def test_dunders(self):
    #     db = self.create_database()
    #     db.celebrities.objects.create(name='Kendall', height=203)
    #     qs = db.celebrities.objects.all()

    #     item = qs[0]
    #     self.assertIsInstance(item, BaseRow)

    #     for item in qs:
    #         with self.subTest(item=item):
    #             self.assertIsInstance(item, BaseRow)

    #     # TODO: Raises an error
    #     # result = 'Kendall' in qs
    #     # self.assertTrue(result)

    #     # TODO: Raises recursion error
    #     # self.assertTrue(qs == qs)

    #     self.assertTrue(len(qs) == 1)

    #     # TODO: Raises an error
    #     # self.assertIsInstance(qs.dataframe, pandas.DataFrame)
