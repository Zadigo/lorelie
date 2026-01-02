from unittest.mock import MagicMock
from lorelie.backends import SQLiteBackend
from lorelie.database.base import Database
from lorelie.database.nodes import SelectNode
from lorelie.queries import QuerySet
from lorelie.queries import Query
from lorelie.test.testcases import LorelieTestCase


class TestQuerySet(LorelieTestCase):
    @classmethod
    def setUpClass(cls):
        statements = [
            'create table if not exists celebrities (id integer primary key autoincrement not null, name text);',
            "insert into celebrities (name) values('Kylie Jenner'), ('Kendall Jenner'), ('Addison Rae');"
        ]
        backend = SQLiteBackend()

        table = cls.create_table(cls)
        cls.db = Database(table)

        mfield = MagicMock(name='Field', spec=['name', 'to_python'])
        mfield.to_python.side_effect = lambda x: x

        # Create the table and insert some data
        Query.run_transaction(
            table=table,
            backend=backend,
            sql_tokens=statements
        )

        # Create a basic select query and queryset for testing
        # the test functions below which will always operate
        # in the conext of selecting items from the dabase
        query = Query(table=table)
        query.add_sql_node(SelectNode(table, '*'))
        cls.qs = QuerySet(query)

    def test_structure(self):
        # The result cache should be empty before evaluation
        self.assertListEqual(self.qs.result_cache, [])
        # Calling len() should evaluate the queryset
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
        qs1 = self.qs.all()
        row = qs1.first()
        self.assertEqual(
            qs1.query.sql,
            'select * from celebrities order by id desc limit 1;'
        )

    def test_last(self):
        qs1 = self.qs.all()
        row = qs1.last()
        self.assertEqual(
            qs1.query.sql,
            'select * from celebrities order by id desc limit 1;'
        )

    def test_filter(self):
        qs2 = self.qs.filter(id=1)
        # Logically None because the query has not been evaluated yet
        self.assertIsNone(qs2.query.sql)

        qs3 = qs2.filter(name='Kylie Jenner')
        list(qs3)
        self.assertEqual(
            qs3.query.sql,
            "select * from celebrities where id=1 and name='Kylie Jenner';"
        )

    def test_get(self):
        row = self.qs.get(id=2)
        self.assertIsNotNone(row)

    def test_annotate(self):
        pass

    def test_all(self):
        qs = self.qs.all()

        for item in qs:
            pass

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
