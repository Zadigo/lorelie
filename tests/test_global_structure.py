

import asyncio
import dataclasses

from lorelie import log_queries
from lorelie.constraints import CheckConstraint, UniqueConstraint
from lorelie.database import registry, triggers
from lorelie.database.base import Database
from lorelie.database.functions.aggregation import Count, Sum
from lorelie.database.functions.text import Lower
from lorelie.database.functions.window import Rank, Window
from lorelie.database.indexes import Index
from lorelie.database.views import View
from lorelie.expressions import Case, F, Q, When
from lorelie.fields.base import CharField, DateTimeField, IntegerField
from lorelie.tables import Table
from lorelie.test.testcases import LorelieTestCase


class TestGlobalStructure(LorelieTestCase):
    def test_project_structure(self):
        table = Table(
            'products',
            fields=[
                CharField('name'),
                IntegerField('price', default=1),
                DateTimeField('created_on', auto_add=True)
            ],
            indexes=[
                Index(
                    'my_index',
                    fields=['name'],
                    condition=Q(name='Kendall')
                )
            ]
        )

        db = Database(table)
        db.migrate()

        for i in range(10):
            db.celebrities.objects.create('products', name=f'Product {i}')

        # all
        qs = db.celebrities.objects.all('products')
        self.assertTrue(qs.exists())

        # annotate
        qs = db.celebrities.objects.annotate('products', a=Lower('name'))
        self.assertEqual(qs.first().a, 'product 0')

        # filter
        qs = db.celebrities.objects.filter('products', Q(
            name='Product 0') | Q(name='Product 1'))
        self.assertEqual(qs.count(), 2)

        item = qs[0]
        item['name'] = 'My product'
        item.save()
        self.assertTrue(item.name, 'My Product')

        qs2 = qs.aggregate(Sum('price'), Count('price'))
        self.assertEqual(qs2['price__sum'], 2)

        # TODO:
        # qs3 = qs.annotate(lowered_name=Lower('name'))
        # print(qs3)
