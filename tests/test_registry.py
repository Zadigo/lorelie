from lorelie.database.base import Database
from lorelie.database.tables.base import Table
from lorelie.fields.base import CharField
from lorelie.test.testcases import LorelieTestCase
from lorelie import registry


class TestMasterRegistry(LorelieTestCase):
    def test_structure(self):
        self.assertTrue(hasattr(registry, 'known_tables'))
        self.assertIsInstance(registry.known_tables, dict)

    def test_register_and_get_table(self):
        table = Table('testing', fields=[CharField('name')])
        database = Database(table)

        self.assertTrue(len(registry.known_tables.keys()) > 0)
