import dataclasses

from lorelie.aggregation import Count, Sum
from lorelie.backends import SQLiteBackend
from lorelie.database.base import Database
from lorelie.database.indexes import Index
from lorelie.database.nodes import OrderByNode
from lorelie.expressions import Q
from lorelie.fields.base import CharField, DateTimeField, IntegerField, Value
from lorelie.functions import Lower, SubStr, Upper, ExtractYear, ExtractMonth, ExtractDay, Concat
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

db = Database(table, name='test_database')
db.migrate()

# @dataclasses.dataclass
# class Product:
#     name: str
#     price: int


# new_product = Product('Jupe moyenne', 45)
# new_product2 = Product('Manteau bleu', 45)

# product = db.objects.create('products', name='Jupe courte', price=10)
# product = db.objects.create('products', name='Jupe longue', price=0)
# product = db.objects.create('products', name='Jupe longue', price=45)


# qs = db.objects.annotate('products', Max('name'))
qs = db.objects.annotate('products')
print(qs.values('id'))
# print(qs.values('upper_name'))


# from django.db.models import Model
# m = Model.objects.annotate()
