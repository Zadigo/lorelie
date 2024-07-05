import unittest

from lorelie.database.base import RelationshipMap
from lorelie.database.manager import ForeignTablesManager
from lorelie.fields.relationships import BaseRelationshipField, ForeignKeyField
from lorelie.test.testcases import LorelieTestCase


class TestRelationship(LorelieTestCase):
    def test_structure(self):
        db = self.create_foreign_key_database()
        self.assertTrue(db.has_relationships)
        self.assertIsInstance(
            list(db.relationships.values())[0],
            ForeignTablesManager
        )
        self.assertIn(
            'celebrities_followers',
            db.relationships.keys()
        )

        table = db.get_table('celebrities')
        self.assertTrue(table.is_foreign_key_table)


class TestForeignKeyField(LorelieTestCase):
    def test_structure(self):
        t1 = self.create_table()
        t2 = self.create_table()

        relationship_map = RelationshipMap(t1, t2)
        field = ForeignKeyField(relationship_map=relationship_map)
        self.assertTrue(field.is_relationship_field)
