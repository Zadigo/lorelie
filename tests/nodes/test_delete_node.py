import sqlite3
from unittest.mock import patch

from lorelie.database.nodes import (
    DeleteNode,
)
from lorelie.expressions import Q
from lorelie.test.testcases import LorelieTestCase


@patch.object(sqlite3, 'connect')
class TestDeleteNode(LorelieTestCase):
    def test_structure(self, mock_connect):
        delete = DeleteNode(self.create_table())
        delete.as_sql(self.create_connection())

    def test_with_where_node(self, mock_connect):
        delete = DeleteNode(self.create_table(), Q(name='Kendall'))
        result = delete.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            ['delete from celebrities', "where name='Kendall'"]
        )

    def test_with_multiple_where_node(self, mock_connect):
        delete = DeleteNode(self.create_table(), Q(name='Kendall'), Q(age=34))
        result = delete.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            ['delete from celebrities', "where name='Kendall' and age=34"]
        )
