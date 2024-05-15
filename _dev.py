import dataclasses
from lorelie.aggregation import Count, Sum
from lorelie.backends import SQLiteBackend
from lorelie.database.base import Database
from lorelie.database.nodes import OrderByNode
from lorelie.expressions import Q
from lorelie.fields.base import CharField, DateTimeField, IntegerField
from lorelie.functions import Lower, Upper
from lorelie.tables import Table

table = Table(
    'products',
    ordering=['-name'],
    fields=[
        CharField('name'),
        IntegerField('price', default=0),
        DateTimeField('created_on', auto_add=True)
    ]
)

db = Database(table)
db.migrate()


@dataclasses.dataclass
class Product:
    name: str
    price: int


new_product = Product('Jupe moyenne', 45)
new_product2 = Product('Manteau bleu', 45)

product = db.objects.create('products', name='Jupe courte', price=10)
product = db.objects.create('products', name='Jupe longue', price=0)
product = db.objects.create('products', name='Jupe longue', price=45)
# product = db.objects.create('products', new_product)
# qs = db.objects.bulk_create('products', new_product, new_product2)

# product = db.objects.first('products')
# product = db.objects.last('products')
# qs = db.objects.all('products')
# product = db.objects.get('products', id=1)
# qs = db.objects.order_by('products', 'name')
# result = db.objects.aggregate('products', Count('price'), Sum('price'))
# result = db.objects.count('products')
# qs = db.objects.values('products', 'price', 'id')
# qs = db.objects.distinct('products', 'name')
# qs = db.objects.filter('products', name__contains='Jupe')
# qs = db.objects.annotate('products', lowered=Lower('name'))

# # sub_qs = qs.values('id', 'lowered')
# sub_qs = qs.order_by('price')
# print(sub_qs)
# print(sub_qs.sql_statement)
# # print(sub_qs.values('lowered'))
# sub_qs.update(name='Kendall Jupe')
# print(db.objects.values('products', 'name'))

qs = db.objects.filter('products', name='Jupe courte')
print(qs.get(name__contains='courte'))
print(qs.sql_statement)
# qs = db.objects.filter('products', name__eq='Jupe longue')
# qs = db.objects.filter('products', name__contains='Jupe')
# qs = db.objects.filter('products', name__gte='Jupe')
# qs = db.objects.filter('products', name__lte='Jupe')
# qs = db.objects.filter(
#     'products',
#     Q(name__contains='Jupe') | Q(name__contains='Jupe')
# )
# product = db.objects.get('products', name='Jupe courte')
# print(product)
# qs = db.objects.annotate(
#     'products',
#     lowered=Lower('name'),
#     uppered=Upper('name')
# )
# print(qs)
# product = qs[0]
# print(product.uppered)
# print(qs.sql_statement)
# qs = db.objects.all('products')
# print(qs, product)
# print(qs)
# print(qs.query)

# qs = db.objects.all('products').order_by('name')
# print(qs)
# print(qs.sql_statement)

# from django.db.models import Model
# Model.objects.bulk_create()
