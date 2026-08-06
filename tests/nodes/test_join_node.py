import sqlite3
from unittest.mock import patch

from lorelie.database.nodes import JoinNode
from lorelie.test.testcases import LorelieTestCase


@patch.object(sqlite3, 'connect')
class TestJoinNode(LorelieTestCase):
    def test_cross_join(self, mconn):
        # celebrities -> followers
        db = self.create_foreign_key_database()
        manager = db.relationships['followers']

        node = JoinNode('followers', manager.relationship_map)
        result = node.as_sql(db.get_table('celebrities').backend)
        expected = [
            'inner join followers on followers.id = celebrities.celebrities_id'
        ]
        self.assertListEqual(result, expected)

    def test_full_outer_join(self, mconn):
        # celebrities <- followers
        pass
        