from lorelie.exceptions import TableExistsError
from lorelie.tables import Table
from lorelie.test.testcases import LorelieTestCase


class TestDatabase(LorelieTestCase):
    def test_structure(self):
        db = self.create_empty_database
        self.assertTrue(db.in_memory)

        with self.assertRaises(TableExistsError):
            db.get_table('celebrities')

        self.assertFalse(db.migrations.migrated)
        self.assertFalse(db.has_relationships)

        db.migrate()

    def test_direct_table_attribute(self):
        db = self.create_database()
        self.assertIsInstance(db.celebrities, Table)
