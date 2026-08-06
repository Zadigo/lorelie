
import sqlite3
from unittest.mock import patch

from lorelie.database.nodes import (
    InsertNode,
)
from lorelie.test.testcases import LorelieTestCase


@patch.object(sqlite3, 'connect')
class TestInsertNode(LorelieTestCase):
    def setUp(self):
        self.table = self.create_table()

    def test_with_single_value(self, mconn):        
        insert_values = {'firstname': 'Kendall'}
        node = InsertNode(self.table, insert_values=insert_values)
        sql = node.as_sql(self.create_connection())
        
        self.assertListEqual(
            sql,
            [
                "insert into celebrities (firstname) values('Kendall')",
                'returning id'
            ]
        )

    def test_with_batch_values(self, mconn):
        batch_values = [{'firstname': 'Kendall'}, {'firstname': 'Jaime'}]
        node = InsertNode(self.table, batch_values=batch_values)
        sql = node.as_sql(self.create_connection())

        self.assertListEqual(
            sql,
            [
                "insert into celebrities (firstname) values ('Kendall'), ('Jaime')",
                'returning id'
            ]
        )

    def test_insert_values(self, mconn):
        data = {
            'name': 'Kendall',
            'age': 22,
            'height': lambda: 154,
            'city': ('LA'),
            'country': ['USA']
        }
        node = InsertNode(self.create_table(), insert_values=data)
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            [
                "insert into celebrities (name, age, height, city, country) values('Kendall', 22, 154, 'LA', '[''USA'']')",
                'returning id'
            ]
        )

    def test_returning(self, mconn):
        node = InsertNode(
            self.create_table(),
            insert_values={'name': 'Kendall'},
            returning=['id']
        )
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            [
                "insert into celebrities (name) values('Kendall')",
                'returning id'
            ]
        )

    def test_all_parameters(self, mconn):
        node = InsertNode(
            self.create_table(),
            insert_values={'name': 'Kendall'},
            # batch_values should take precedence over insert_values
            batch_values=[{'name': 'Kylie'}],
            returning=['id']
        )
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            [
                "insert into celebrities (name) values ('Kylie')",
                'returning id'
            ]
        )


@patch.object(sqlite3, 'connect')
class TestInsertNodeExceptions(LorelieTestCase):
    def setUp(self):
        self.table = self.create_table()

    def test_table_is_none(self, mconn):
        node = InsertNode(None, insert_values={'name': 'Kendall'})

        with self.assertRaises(ValueError) as context:
            node.as_sql(self.create_connection())
            self.assertEqual(str(context.exception), "Table cannot be None.")

    def test_batch_has_invalid_values(self, mconn):
        values = ['invalid', 1234]

        for value in values:
            with self.assertRaises(ValueError) as context:
                InsertNode(self.table, batch_values=[value])
                self.assertEqual(f"'{value}' should be a dictionnary", str(context.exception))
