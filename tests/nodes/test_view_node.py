import sqlite3
from unittest.mock import Mock, patch

from lorelie.database.nodes import (
    ViewNode,
)
from lorelie.queries import Query, QuerySet
from lorelie.test.testcases import LorelieTestCase


@patch.object(sqlite3, 'connect')
class TestViewNode(LorelieTestCase):
    def setUp(self):
        self.sql = 'select * from celebrities;'

        mquery = Mock(spec=Query, sql=self.sql)
        mqs = Mock(spec=QuerySet, query=mquery)
        
        self.mqs = mqs

    def test_structure(self, mconn):
        node = ViewNode('my_view', self.mqs)
        result = node.as_sql(self.create_connection())

        self.assertListEqual(
            result,
            [
                f"create view if not exists my_view as {self.sql}"
            ]
        )

        # qs = db.celebrities.objects.all()
        # node = ViewNode('my_view', qs)

        # backend = db.get_table('celebrities').backend
        # result = node.as_sql(backend)

        # self.assertListEqual(
        #     result,
        #     [
        #         "create view if not exists my_view as select * from celebrities;"
        #     ]
        # )

    def test_with_temporary(self, mconn):
        node = ViewNode('my_view', self.mqs, temporary=True)
        result = node.as_sql(self.create_connection())

        self.assertListEqual(
            result,
            [
                f"create temp view if not exists my_view as {self.sql}"
            ]
        )

    def test_with_fields(self, mconn):
        node = ViewNode('my_view', self.mqs, fields=['name', 'age'])
        result = node.as_sql(self.create_connection())

        self.assertListEqual(
            result,
            [
                f"create view (name, age) if not exists my_view as {self.sql}"
            ]
        )

    def test_queryset_invalid(self, mconn):
        node = ViewNode('my_view', 'invalid_queryset')
        with self.assertRaises(ValueError):
            node.as_sql(self.create_connection())
