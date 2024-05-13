from fields.base import CharField

from lorelie.database.base import Database
from lorelie.expressions import Q
from lorelie.tables import Table

table = Table(
    'products',
    fields=[
        CharField('name')
    ],
    ordering=['name']
)
db = Database(table)
db.migrate()
product = db.objects.create('products', name='Jupe courte')
product = db.objects.create('products', name='Jupe longue')
product = db.objects.first('products')
product = db.objects.last('products')

qs = db.objects.filter('products', name__eq='Jupe longue')
qs = db.objects.filter('products', name__contains='Jupe')
# qs = db.objects.filter('products', name__gte='Jupe')
# qs = db.objects.filter('products', name__lte='Jupe')
# qs = db.objects.filter(
#     'products',
#     Q(name__contains='Jupe') | Q(name__contains='Jupe')
# )
product = db.objects.get('products', name='Jupe courte')
print(product)

# qs = db.objects.all('products')
# print(qs, product)
# print(qs)
# print(qs.query)
