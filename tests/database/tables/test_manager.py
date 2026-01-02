

from dataclasses import dataclass
from lorelie.database.base import Database
from lorelie.database.functions.text import Lower
from lorelie.database.manager import DatabaseManager
from lorelie.test.testcases import LorelieTestCase


class TestDatabaseManager(LorelieTestCase):
    def setUp(self):
        table = self.create_table()
        manager = DatabaseManager()
        db = Database(table)
        db.migrate()
        manager.database = db
        manager.table = table
        manager.table_map = db.table_map
        self.manager = manager
        self.db = db

    def test_structure(self):
        self.assertIsNotNone(self.manager.database)
        self.assertIsNotNone(self.manager.table)
        self.assertIsNotNone(self.manager.table_map)

    def test_create(self):
        row = self.manager.create(name='Kendall')
        self.assertIsNotNone(row)
        self.assertEqual(row['name'], 'Kendall')

    def test_annotate(self):
        _celebrities = [
            {'name': 'Kylie Jenner'},
            {'name': 'Kendall Jenner'},
            {'name': 'Addison Rae'}
        ]

        @dataclass
        class Celebrity:
            name: str

        celebrities = [Celebrity(**data) for data in _celebrities]

        self.manager.bulk_create(celebrities)
        qs = self.manager.annotate(lowered_name=Lower('name'))

        for obj in qs:
            self.assertEqual(obj.lowered_name, obj.name.lower())
