import unittest

from lorelie.backends import BaseRow
from lorelie.database import Database
from lorelie.fields.base import CharField
from lorelie.tables import Table


class TestDatabaseManager(unittest.TestCase):
    def setUp(self):
        table = Table(
            'celebrities',
            ordering=['firstname'],
            fields=[CharField('firstname')],
            str_field='firstname'
        )

        db = Database(table)
        db.migrate()
        db.objects.create('celebrities', firstname='Kendall')
        db.objects.create('celebrities', firstname='Kylie')
        db.objects.create('celebrities', firstname='Taylor')
        db.objects.create('celebrities', firstname='Jade')
        db.objects.create('celebrities', firstname='Lucie')
        db.objects.create('celebrities', firstname='Anya-Taylor')
        db.objects.create('celebrities', firstname='Pauline')
        self.db = db

    def test_return_value(self):
        celebrity = self.db.objects.first('celebrities')
        self.assertIsInstance(celebrity, BaseRow)

    def test_get(self):
        names_to_get = ['Taylor', 'Jade']
        for name in names_to_get:
            with self.subTest(name=name):
                celebrity = self.db.objects.get(
                    'celebrities',
                    firstname=name
                )
                self.assertTrue(celebrity.firstname == name)
                self.assertTrue(celebrity['firstname'] == name)

    # def test_all(self):
    #     queryset = self.db.objects.all('celebrities')
    #     self.assertTrue(len(queryset) == 7)

    # def test_filter(self):
    #     queryset = self.db.objects.filter(
    #         'celebrities',
    #         firstname__contains='Ken'
    #     )
    #     self.assertTrue(len(queryset) == 1)


if __name__ == '__main__':
    unittest.main()
