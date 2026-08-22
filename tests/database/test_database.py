import pathlib

import pytest

from lorelie.database.base import Database
from lorelie.database.manager import DatabaseManager
from lorelie.database.tables.base import Table
from lorelie.exceptions import TableExistsError
from lorelie.test.testcases import LorelieTestCase


@pytest.mark.parametrize(
    'name,path',
    [
        (
            'with path',
            pathlib.Path(__file__).joinpath('testdb')
        ),
        (
            'no path',
            None
        )
    ]
)
def test_structure(none_migrated_database, name, path):
    if path is None:
        assert none_migrated_database.in_memory is True

    assert none_migrated_database.migrations.JSON_MIGRATIONS_SCHEMA is not None
    # assert none_migrated_database.migrations.migrated is False
    # assert none_migrated_database.has_relationships is False


def test_structure_migrate(none_migrated_database):
    none_migrated_database.migrate()
    assert none_migrated_database.migrations.migrated is True
    # assert none_migrated_database.has_relationships is False

@pytest.mark.parametrize(
    'testcase,name,path',
    [
        (
            'in memory',
            None,
            None
        ),
        (
            'physical - no path',
            'test_database',
            None
        ),
        (
            'physical - with path',
            'test_database',
            pathlib.Path(__file__).parent.absolute()
        ),
        (
            'in memory - with path',
            None,
            pathlib.Path(__file__).parent.absolute()
        )
    ]
)
def test_different_connection_types(testcase, name, path):
    db = Database(name=name, path=path)

    if 'in memory' in testcase:
        assert db.in_memory is True

    if 'physical' in testcase:
        assert db.in_memory is False



class TestDatabase(LorelieTestCase):
    def test_table_does_not_exist(self):
        db = self.create_empty_database
        with self.assertRaises(TableExistsError):
            db.get_table('celebrities')

    # def test_path_parameter(self):
    #     db = Database(path=pathlib.Path('.'))
    #     self.assertFalse(db.in_memory)

    def test_table_is_invalid(self):
        with self.assertRaises(TypeError):
            Database('test_table')

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

    def test_create_database_with_name(self):
        db = Database(name='my_database')
        self.assertEqual(db.database_name, 'my_database')
        self.assertFalse(db.in_memory)
