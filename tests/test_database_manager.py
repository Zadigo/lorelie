from lorelie.database.manager import DatabaseManager, ForeignTablesManager
from lorelie.test.testcases import AsyncLorelieTestCase, LorelieTestCase


class TestDatabaseManager(LorelieTestCase):
    def test_structure(self):
        db = self.create_database()
        instance = DatabaseManager.as_manager(
            table_map=db.table_map,
            database=db
        )
        instance.create(name='Kendall Jenner', height=156)
        queryset = instance.all('celebrities')
        self.assertTrue(queryset[0].name == 'Kendall Jenner')


class TestForeignKeyManager(LorelieTestCase):
    def test_structure(self):
        db = self.create_foreign_key_database()
        instance = ForeignTablesManager(db.relationships['followers'])
        print(instance.all('celebrities'))


class AsyncDatabaseManager(AsyncLorelieTestCase):
    async def test_structure(self):
        db = await self.create_database()
        instance = DatabaseManager.as_manager(
            table_map=db.table_map,
            database=db
        )
        await instance.acreate('celebrities', name='Kendall Jenner', height=156)
        queryset = await instance.aall('celebrities')
        self.assertTrue(queryset[0].name == 'Kendall Jenner')
