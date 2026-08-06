import sqlite3
from unittest.mock import patch

from lorelie.database.nodes import (
    IntersectNode,
    SelectNode,
)
from lorelie.test.testcases import LorelieTestCase


@patch.object(sqlite3, 'connect')
class TestIntersectNode(LorelieTestCase):
    def test_structure(self, mock_connect):
        select1 = SelectNode(self.create_table())
        select2 = SelectNode(self.create_table())

        node = IntersectNode(select1, select2)
        result = node.as_sql(self.create_connection())

        self.assertListEqual(
            result,
            ['select * from celebrities intersect select * from celebrities']
        )
