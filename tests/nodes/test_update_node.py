import sqlite3
from unittest.mock import patch

from lorelie.database.nodes import (
    DeleteNode,
    UpdateNode,
)
from lorelie.expressions import Q
from lorelie.test.testcases import LorelieTestCase


@patch.object(sqlite3, 'connect')
class TestUpdateNode(LorelieTestCase):
    def test_structure(self, mock_connect):
        node = UpdateNode(
            self.create_table(),
            {'name': 'Kendall'},
            name='Kylie'
        )
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            [
                "update celebrities set name='Kendall'",
                "where name='Kylie'"
            ]
        )

    def test_with_where_node(self, mock_connect):
        node = UpdateNode(
            self.create_table(),
            {'name': 'Kendall'},
            Q(name='Kylie')
        )
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            [
                "update celebrities set name='Kendall'",
                "where name='Kylie'"
            ]
        )

    def test_mixed_args(self, mock_connect):
        node = UpdateNode(
            self.create_table(),
            {'name': 'Kendall'},
            Q(name='Kylie'),
            name='Julie'
        )
        result = node.as_sql(self.create_connection())
        self.assertListEqual(
            result,
            [
                "update celebrities set name='Kendall'",
                "where name='Kylie' and name='Julie'"
            ]
        )

    def test_cannot_use_q_functions(self, mock_connect):
        node = UpdateNode(
            self.create_table(),
            {'name': 'Kendall'},
            name=Q(name='Kendall')
        )
        self.assertRaises(
            ValueError,
            node.as_sql,
            self.create_connection()
        )


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


# @patch.object(sqlite3, 'connect')
# class TestJoinNode(LorelieTestCase):
#     def test_structure(self, mock_connect):
#         # celebrities -> followers
#         db = self.create_foreign_key_database()
#         manager = db.relationships['followers']

#         node = JoinNode('followers', manager.relationship_map)
#         result = node.as_sql(db.get_table('celebrities').backend)
#         expected = [
#             'inner join followers on followers.id = celebrities.celebrities_id'
#         ]
#         self.assertListEqual(result, expected)

