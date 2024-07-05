import unittest

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
        # qs.get()
        print(qs.sql_statement)
