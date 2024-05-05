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
        database = Database(table, name='followings')
        # database.make_migrations()
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
        self.assertIsInstance(celebrity, BaseRow)
        self.assertIsInstance(celebrity.id, int)
        print(vars(celebrity))
        # self.assertTrue(celebrity.firstname == 'Kendall')


if __name__ == '__main__':
    unittest.main()
