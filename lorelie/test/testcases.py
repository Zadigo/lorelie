import unittest
from functools import cached_property, lru_cache

from lorelie.backends import SQLiteBackend
from lorelie.constraints import CheckConstraint, UniqueConstraint
from lorelie.database.base import Database
from lorelie.exceptions import ValidationError
from lorelie.expressions import Q
from lorelie.fields.base import (BooleanField, CharField, DateTimeField,
                                 FloatField, IntegerField)
from lorelie.tables import Table

__all__ = [
    'LorelieTestCase'
]


class LorelieTestCase(unittest.TestCase):
    @lru_cache(maxsize=300)
    def create_connection(self):
        return SQLiteBackend()

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
            FloatField('followers', default=0),
            BooleanField('is_active', default=True),
            DateTimeField('created_on', auto_add=True)
        ])
        return table

    def create_database(self, using=None):
        if using is not None:
            table = using
        else: 
            table = self.create_table()
            
        db = Database(table)
        db.migrate()
        return db

    def create_table(self):
        table = Table('celebrities', fields=[
            CharField('name'),
            IntegerField('height', min_value=150, default=150)
        ])
        return table

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