import pathlib
import asyncio
import dataclasses

from lorelie.aggregation import (Avg, CoefficientOfVariation, Count, Max,
                                 MeanAbsoluteDifference, Min, StDev, Sum,
                                 Variance)
from lorelie.backends import SQLiteBackend
from lorelie.database.base import Database
from lorelie.database.indexes import Index
from lorelie.database.nodes import OrderByNode
from lorelie.expressions import Q, Case, When
from lorelie.fields.base import CharField, DateTimeField, IntegerField, Value
from lorelie.functions import (Concat, ExtractDay, ExtractMonth, ExtractYear,
                               Lower, SubStr, Upper)
from lorelie.tables import Table

table = Table(
    'products',
    ordering=['-name'],
    str_field='name',
    fields=[
        CharField('name'),
        IntegerField('price', default=0),
        DateTimeField('created_on', auto_add=True)
    ]
)

models = Table(
    'models',
    str_field='firstname',
    fields=[
        CharField('firstname'),
        CharField('lastname')
    ]
)

db = Database(table, models)
db.foreign_key(table, models)


# @db.register_trigger(table=table, trigger='pre_save')
# def some_trigger(instance, table, **kwargs):
#     pass


db.migrate()

db.objects.create('products', name='Jupe de luxe')

a = When('name__eq=Jupe de luxe', 'Mode')
b = When('name__eq=Jupe de mode', 'Fashion')
c = When(Q(name__contains='Jupe') & Q(name__ne='Google'), 'Quick')
case = Case(a, b, c, default='google')
v = db.objects.annotate('products', c=case)
print(v)
print(v.sql_statement)
