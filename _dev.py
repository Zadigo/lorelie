import dataclasses

from lorelie.aggregation import (Avg, CoefficientOfVariation, Count, Max,
                                 MeanAbsoluteDifference, Min, StDev, Sum,
                                 Variance)
from lorelie.backends import SQLiteBackend
from lorelie.database.base import Database
from lorelie.database.indexes import Index
from lorelie.database.nodes import OrderByNode
from lorelie.expressions import Q
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

db = Database(table, models, name='test_database')
db.foreign_key(table, models)
db.migrate()

item = db.objects.get('products', id__eq=2)
print(item)
# item.models_rel.create(firstname='Kendall', lastname='Jenner')
# qs = item.models_rel.all()
# print(qs)
# print(qs.sql_statement)

# @dataclasses.dataclass
# class Product:
#     name: str
#     price: int


# new_product = Product('Jupe moyenne', 45)
# new_product2 = Product('Manteau bleu', 45)

# product = db.objects.create('products', name='Jupe courte', price=10)
# product = db.objects.create('products', name='Jupe longue', price=0)
# product = db.objects.create('products', name='Jupe longue', price=45)

# qs = db.objects.annotate('products', Count('price'))
# qs2 = qs.filter(name='Jupe longue').filter(id=2)
# print(qs2)
# print(qs2.sql_statement)

# result = db.objects.aggregate(
#     'products',
#     Count('price'),
#     Sum('price'),
#     Avg('price'),
#     MeanAbsoluteDifference('price'),
#     Variance('price'),
#     StDev('price'),
#     CoefficientOfVariation('price'),
#     Max('price'),
#     Min('price')
# )
# print(result)
