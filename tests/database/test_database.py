import pathlib

from lorelie.database.base import Database
from lorelie.database.manager import DatabaseManager
from lorelie.database.tables.base import Table
from lorelie.exceptions import TableExistsError
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
        self.assertIsInstance(db.celebrities.objects, DatabaseManager)

    def test_different_connection_types(self):
        # In memory
        db = Database()
        self.assertTrue(db.in_memory)

        # Physical (no path)
        db = Database(name='test_database')
        self.assertFalse(db.in_memory)

        db = Database(name='test_database2', path=pathlib.Path('.'))
        self.assertFalse(db.in_memory)

        # In memory
        db = Database(path=pathlib.Path('.'))
        self.assertTrue(db.in_memory)
