import unittest

from lorelie.backends import BaseRow
from lorelie.database.base import Database
from lorelie.expressions import Q
from lorelie.fields.base import CharField, IntegerField, JSONField
from lorelie.queries import QuerySet
from lorelie.tables import Table
from lorelie.test.testcases import LorelieTestCase


class TestDatabase(LorelieTestCase):
    def test_structure(self):
        db = self.create_empty_database
        self.assertTrue(db.in_memory)

        with self.assertRaises(KeyError):
            db.get_table('celebrities')

        self.assertFalse(db.migrations.migrated)
        self.assertFalse(db.has_relationships)

        db.migrate()


# table = Table(
#     'celebrities',
#     ordering=['firstname'],
#     fields=[
#         CharField('firstname'),
#         CharField('lastname'),
#         IntegerField('followers'),
#         JSONField('metadata', null=True)
#     ]
# )
# db = Database(table)
# db.migrate()

# # Cannot test this class maybe because of
# # the async???


# class TestDatabase(unittest.TestCase):
#     def setUp(self):
#         for celebrity in celebrities:
#             db.objects.create('celebrities', **celebrity)

#     def test_all_query(self):
#         qs = db.objects.all('celebrities')
#         self.assertIsInstance(qs, QuerySet)
#         self.assertTrue(len(qs) == 5)

#         celebrity = qs[-1]
#         self.assertIsInstance(celebrity, BaseRow)
#         self.assertIsInstance(celebrity.firstname, str)
#         self.assertEqual(celebrity.firstname, 'Margot')

#     def test_get_query(self):
#         celebrity = db.objects.get('celebrities', firstname='Margot')
#         self.assertTrue(celebrity.firstname == 'Margot')

#     def test_values_query(self):
#         values = db.objects.values('celebrities', 'id')
#         self.assertIsInstance(values, list)
#         self.assertIsInstance(values[0], dict)

#     def test_filter_query(self):
#         qs = db.objects.filter(
#             'celebrities',
#             lastname__contains='Jenner'
#         )
#         self.assertEqual(len(qs), 2)

#         qs = db.objects.filter(
#             'celebrities',
#             firstname='Kylie',
#             lastname='Jenner'
#         )
#         self.assertEqual(len(qs), 1)
#         self.assertIn(qs, 'Kylie')

#         qs = db.objects.filter(
#             'celebrities',
#             Q(firstname='Kendall') | Q(lastname='Robbie')
#         )
#         self.assertEqual(len(qs), 2)

#         qs = db.objects.filter(
#             'celebrities',
#             Q(followers__gte=1000) & Q(followers__lte=5000)
#         )
#         self.assertEqual(len(qs), 2)


# if __name__ == '__main__':
#     unittest.main()
