from lorelie.database.base import RelationshipMap
from lorelie.database.tables.base import Table
from lorelie.fields.base import CharField
from lorelie.fields.relationships import ForeignKeyField
from lorelie.test.testcases import LorelieTestCase


class TestRelationship(LorelieTestCase):
    def test_structure(self):
        fields = [CharField(name='name')]
        t1 = Table(name='table1', fields=fields)
        t2 = Table(name='table2', fields=fields)

        relationship_map = RelationshipMap(t1, t2)
        self.assertEqual(relationship_map.relationship_name, 'table1_table2')
        self.assertEqual(relationship_map.forward_field_name, t1.name)
        self.assertEqual(
            relationship_map.backward_field_name,
            f'{t2.name}_set'
        )
        self.assertEqual(
            relationship_map.foreign_backward_related_field_name,
            f'{t2.name}_id'
        )
        self.assertEqual(
            relationship_map.foreign_forward_related_field_name,
            f'{t1.name}_id'
        )

        print(relationship_map.get_relationship_condition(t1))

        # pass
        # db = self.create_foreign_key_database()
        # self.assertTrue(db.has_relationships)

        # self.assertIsInstance(
        #     list(db.relationships.values())[0],
        #     ForeignTablesManager
        # )
        # self.assertIn(
        #     'celebrities_followers',
        #     db.relationships.keys()
        # )

        # table = db.get_table('celebrities')
        # self.assertTrue(table.is_foreign_key_table)


class TestForeignKeyField(LorelieTestCase):
    def test_structure(self):
        t1 = self.create_table()
        t2 = self.create_table()

        relationship_map = RelationshipMap(t1, t2)
        field = ForeignKeyField(relationship_map=relationship_map)
        self.assertTrue(field.is_relationship_field)
