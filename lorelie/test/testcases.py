import unittest
from functools import cached_property, lru_cache

from lorelie.backends import SQLiteBackend
from lorelie.constraints import CheckConstraint, UniqueConstraint
from lorelie.database.base import Database
from lorelie.database.indexes import Index
from lorelie.database.tables.base import Table
from lorelie.exceptions import ValidationError
from lorelie.expressions import Q
from lorelie.fields.base import (BooleanField, CharField, DateTimeField,
                                 FloatField, IntegerField)

__all__ = [
    'LorelieTestCase'
]


class LorelieTestCase(unittest.TestCase):
    @lru_cache(maxsize=300)
    def create_connection(self):
        return SQLiteBackend()

    def create_physical_database(self):
        return SQLiteBackend(database_or_name='celebrities_test')

    @cached_property
    def create_empty_database(self):
        return Database()

    def create_complex_table(self):
        def validate_name(value):
            if value == 'Taylor Swift':
                raise ValidationError(
                    "Name should not be Taylor Swift"
                )

        table = Table('stars', fields=[
            CharField('name', unique=True, validators=[validate_name]),
            IntegerField('age', null=True),
            IntegerField('height', min_value=150, max_value=212),
            FloatField('followers', default=0.0),
            BooleanField('is_active', default=True),
            DateTimeField('created_on', auto_add=True)
        ])
        table.indexes[0].name
        return table

    def create_database(self, using=None, log_queries=False):
        if using is not None:
            table = using
        else:
            table = self.create_table()

        db = Database(table, log_queries=log_queries)
        db.migrate()

        return db

    def create_table(self):
        fields = [
            CharField('name'),
            IntegerField('height', min_value=150, default=152),
            DateTimeField('updated_on', auto_update=True),
            DateTimeField('created_on', auto_add=True)
        ]

        return Table('celebrities', fields=fields)

    def create_constrained_table(self):
        table = Table(
            'celebrities',
            fields=[
                CharField('name', null=True),
                IntegerField('height', null=True)
            ],
            constraints=[
                CheckConstraint('no_kendall', Q(name__ne='Kendall')),
                CheckConstraint('height_over_150', Q(height__gte=150))
            ]
        )
        return table

    def create_unique_constrained_table(self):
        table = Table(
            'celebrities',
            fields=[
                CharField('name', null=True),
                IntegerField('height', null=True)
            ],
            constraints=[
                UniqueConstraint('my_constraint', fields=['name', 'height'])
            ]
        )
        return table

    def create_full_table(self):
        """Creates a table with all the type of constraints
        and parameters available to be created on a table"""
        table = Table(
            'celebrities',
            fields=[
                CharField('firstname'),
                IntegerField('age', min_value=18, max_value=99),
                CharField('country', default='France')
            ],
            constraints=[
                CheckConstraint(
                    'firstname',
                    Q(firstname='Julie')
                ),
                UniqueConstraint(
                    'unique_firstname',
                    fields=['firstname']
                )
            ],
            indexes=[
                Index(
                    'firstname_index',
                    fields=['firstname'],
                    condition=Q(firstname='Kendall')
                )
            ]
        )
        return table

    def create_foreign_key_database(self):
        table1 = Table(
            'celebrities',
            fields=[
                CharField('name'),
                IntegerField('age')
            ]
        )

        table2 = Table(
            'followers',
            fields=[
                IntegerField('number_of_follower')
            ]
        )
        db = Database(table1, table2)
        db.foreign_key('followers', table1, table2)
        db.migrate()
        return db


class AsyncLorelieTestCase(unittest.IsolatedAsyncioTestCase):
    async def create_database(self, using=None):
        if using is not None:
            table = using
        else:
            table = await self.create_table()

        db = Database(table)
        db.migrate()
        return db

    async def create_table(self):
        table = Table('celebrities', fields=[
            CharField('name'),
            IntegerField('height', min_value=150, default=150)
        ])
        return table
