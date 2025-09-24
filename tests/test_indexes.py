import unittest

from lorelie.database.indexes import Index
from lorelie.expressions import Q
from lorelie.test.testcases import LorelieTestCase


class TestIndex(LorelieTestCase):
    def test_structure(self):
        table = self.create_table()
        table.backend = self.create_connection()

        instance = Index('test_name', fields=['name'])
        instance.prepare(table)
        result = instance.as_sql(table.backend)

        self.assertIsInstance(result, str)
        self.assertTrue("create index idx_test_name_" in result)
        self.assertTrue("on celebrities (name)" in result)

    def test_with_functions(self):
        table = self.create_table()
        table.backend = self.create_connection()

        instance = Index(
            'test_name',
            fields=['name'],
            condition=Q(name='Kendall')
        )
        instance.prepare(table)
        result = instance.as_sql(table.backend)
        self.assertTrue("where name='Kendall" in result)

    def test_name_different_from_function_name(self):
        table = self.create_table()
        table.backend = self.create_connection()

        # TODO: The fields are differing which means we should
        # not be able to create an index with completly two
        # different fields in fields and condition
        instance = Index(
            'test_name',
            fields=['name', 'height'],
            condition=Q(age=25)
        )
        instance.prepare(table)
        result = instance.as_sql(table.backend)
        print(result)

    @unittest.expectedFailure
    def test_field_does_not_exist(self):
        table = self.create_table()
        table.backend = self.create_connection()

        # TODO: This should raise an error
        instance = Index(
            'test_name',
            fields=['lastname']
        )
        instance.as_sql(table)
