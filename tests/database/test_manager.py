from lorelie.database.manager import DatabaseManager, ForeignTablesManager
from lorelie.queries import QuerySet
from lorelie.database.tables.base import RelationshipMap
from lorelie.test.testcases import LorelieTestCase


class TestDatabaseManager(LorelieTestCase):
    def setUp(self):
        db = self.create_database()
        self.manager = DatabaseManager()
        self.manager.database = db
        self.manager.table = db.get_table('celebrities')
        self.manager.table_map = db.table_map
        self.manager.create(name='Kendall Jenenr')
        self.manager.create(name='Addison Rae')

    def test_structure(self):
        qs = self.manager.all()
        self.assertIsInstance(qs, QuerySet)


class TestForeignTablesManager(LorelieTestCase):
    def setUp(self):
        db = self.create_foreign_key_database()

        t1 = db.get_table('celebrity')
        t2 = db.get_table('follower')

        celebrity = t1.objects.create(name='Addison Rae', age=23)
        t2.objects.create(name='Julie Ordon', celebrity=celebrity)

        relationship_map = RelationshipMap(t1, t2)
        self.forward_manager = ForeignTablesManager.new(t1, relationship_map)
        self.backward_manager = ForeignTablesManager.new(t2, relationship_map)

    def test_structure(self):
        qs = self.forward_manager.all()
        self.assertIsInstance(qs, QuerySet)

        self.assertIsInstance(qs, QuerySet)

        list(qs)
        print(qs)
        self.assertEqual(
            qs.query.sql,
            "select * from celebrity inner join follower on celebrity.id=follower.celebrity_id;"
        )
