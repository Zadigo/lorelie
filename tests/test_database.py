import unittest

from lorelie.backends import BaseRow
from lorelie.database import Database
from lorelie.expressions import Q
from lorelie.fields import CharField, IntegerField, JSONField
from lorelie.tables import Table

celebrities = [
    {
        'firstname': 'Kendall',
        'lastname': 'Jenner',
        'followers': 1000
    },
    {
        'firstname': 'Kylie',
        'lastname': 'Jenner',
        'followers': 100,
        'metadata': {
            'age': 56
        }
    },
    {
        'firstname': 'Margot',
        'lastname': 'Robbie',
        'followers': 156600
    },
    {
        'firstname': 'Lena',
        'lastname': 'Situation',
        'followers': 4454
    },
    {
        'firstname': 'Aya',
        'lastname': 'Nakamura',
        'followers': 334345
    }
]


class TestDatabase(unittest.TestCase):
    def setUp(self):
        table = Table('celebrities', ordering=['firstname'], fields=[
            CharField('firstname'),
            CharField('lastname'),
            IntegerField('followers'),
            JSONField('metadata', null=True)
        ])
        db = Database(table)
        db.migrate()

        for celebrity in celebrities:
            db.objects.create('celebrities', **celebrity)

        self.db = db

    def test_general_structure(self):
        self.assertTrue(self.db.in_memory)

        table = self.db.get_table('celebrities')

        self.assertIsInstance(table, Table)
        self.assertTrue(len(self.db.table_instances) > 0)

    # def test_connection_manipulation(self):
    #     self.db.objects.create(
    #         'celebrities',
    #         firstname='Kendall',
    #         lastname='Jenner',
    #         followers=10
    #     )

    #     celebrity = self.db.objects.get(
    #         'celebrities',
    #         firstname='Kendall'
    #     )

    #     self.assertIsInstance(celebrity, BaseRow)
    #     self.assertIsInstance(celebrity.id, int)
    #     self.assertTrue(celebrity.firstname == 'Kendall')
    #     self.assertIsInstance(celebrity, BaseRow)

    def test_all_query(self):
        queryset = self.db.objects.all('celebrities')
        # TODO: Should expect Queryset
        # self.assertIsInstance(queryset, list)
        self.assertTrue(len(queryset) == 5)

        celebrity = queryset[-1]
        self.assertIsInstance(celebrity, BaseRow)
        self.assertIsInstance(celebrity.firstname, str)
        self.assertEqual(celebrity.firstname, 'Margot')

    def test_get_query(self):
        celebrity = self.db.objects.get('celebrities', firstname='Margot')
        self.assertTrue(celebrity.firstname == 'Margot')

    def test_values_query(self):
        values = self.db.objects.values('celebrities', 'id')
        self.assertIsInstance(values, list)
        self.assertIsInstance(values[0], dict)

    def test_filter_query(self):
        queryset = self.db.objects.filter(
            'celebrities',
            lastname__contains='Jenner'
        )
        self.assertEqual(len(queryset), 2)

        queryset = self.db.objects.filter(
            'celebrities',
            firstname='Kylie',
            lastname='Jenner'
        )
        self.assertEqual(len(queryset), 1)
        self.assertIn(queryset, 'Kylie')

        queryset = self.db.objects.filter(
            'celebrities',
            Q(firstname='Kendall') | Q(lastname='Robbie')
        )
        self.assertEqual(len(queryset), 2)

        queryset = self.db.objects.filter(
            'celebrities',
            Q(followers__gte=1000) & Q(followers__lte=5000)
        )
        self.assertEqual(len(queryset), 2)


if __name__ == '__main__':
    unittest.main()
