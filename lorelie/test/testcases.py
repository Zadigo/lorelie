import unittest
from functools import lru_cache

from lorelie.backends import SQLiteBackend
from lorelie.database.base import Database
from lorelie.fields.base import CharField, IntegerField
from lorelie.tables import Table

__all__ = [
    'LorelieTestCase'
]


class LorelieTestCase(unittest.TestCase):
    @lru_cache(maxsize=300)
    def create_connection(self):
        return SQLiteBackend()

    def create_database(self):
        table = Table('celebrities', fields=[
            CharField('name'),
            IntegerField('height', min_value=150, default=150)
        ])
        db = Database(table)
        db.migrate()
        return db

    def create_table(self):
        table = Table('celebrities', fields=[
            CharField('name'),
            IntegerField('height', min_value=150, default=150)
        ])
        return table
