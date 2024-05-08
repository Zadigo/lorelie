import unittest

from lorelie.backends import BaseRow
from lorelie.database import Database
from lorelie.fields import CharField, IntegerField
from lorelie.tables import Table


class TestDatabase(unittest.TestCase):
    def setUp(self):
        table = Table('celebrities', fields=[
            CharField('firstname'),
            CharField('lastname'),
            IntegerField('followers')
        ])
        db = Database(table)
        db.migrate()
        self.db = db

    def test_general_structure(self):
        self.assertTrue(self.db.in_memory)

        table = self.db.get_table('celebrities')

        self.assertIsInstance(table, Table)
        self.assertTrue(len(self.db.table_instances) > 0)

    def test_connection_manipulation(self):
        self.db.objects.create(
            'celebrities',
            firstname='Kendall',
            lastname='Jenner',
            followers=10
        )

        celebrity = self.db.objects.get(
            'celebrities',
            firstname='Kendall'
        )

        self.assertIsInstance(celebrity, BaseRow)
        self.assertIsInstance(celebrity.id, int)
        self.assertTrue(celebrity.firstname == 'Kendall')
        self.assertIsInstance(celebrity, BaseRow)


if __name__ == '__main__':
    unittest.main()
