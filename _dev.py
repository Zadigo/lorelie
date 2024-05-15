from fields.base import CharField

from lorelie.backends import SQLiteBackend
from lorelie.database.base import Database
from lorelie.expressions import Q
from lorelie.functions import Lower, Upper
from lorelie.tables import Table
from lorelie.database.nodes import OrderByNode

# db = Database(table)
# db.migrate()
# product = db.objects.create('products', name='Jupe courte')
# product = db.objects.create('products', name='Jupe longue')
# product = db.objects.first('products')
# product = db.objects.last('products')

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
