import unittest

from lorelie.backends import BaseRow
from lorelie.fields import CharField, IntegerField
from lorelie.tables import Database, Table


class TestDatabase(unittest.TestCase):
    def setUp(self):
        table = Table('celebrities', fields=[
            CharField('firstname'),
            CharField('lastname'),
            IntegerField('followers')
        ])
        database = Database(table)
        database.migrate()
        self.database = database

    def test_general_structure(self):
        table = self.database.get_table('celebrities')
        self.assertIsInstance(table, Table)
        self.assertTrue(len(self.database.table_instances) > 0)

    def test_table_manipulation(self):
        self.database.objects.create(
            'celebrities',
            firstname='Kendall',
            lastname='Jenner',
            followers=10
        )

        celebrity = self.database.objects.get(
            'celebrities',
            firstname='Kendall'
        )
        # celebrity = queryset[0]
        self.assertIsInstance(celebrity, BaseRow)
        self.assertIsInstance(celebrity.id, int)
        self.assertTrue(celebrity.firstname == 'Kendall')
        self.assertIsInstance(celebrity, BaseRow)


if __name__ == '__main__':
    unittest.main()
