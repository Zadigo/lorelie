import asyncio
import dataclasses

from lorelie import log_queries
from lorelie.constraints import CheckConstraint, UniqueConstraint
from lorelie.database import registry, triggers
from lorelie.database.base import Database
from lorelie.database.functions.text import Lower
from lorelie.database.functions.window import Rank, Window
from lorelie.database.indexes import Index
from lorelie.expressions import Case, F, Q, When
from lorelie.fields.base import CharField, DateTimeField, IntegerField
from lorelie.tables import Table
from lorelie.database.functions import Count
from lorelie.database.views import View

table = Table('products', fields=[
    CharField('name'),
    DateTimeField('created_on', auto_add=True)
],
    index=[Index('my_index', fields=['name'], condition=Q(name='Kendall'))]
)

db = Database(table, log_queries=True)
db.migrate()

for i in range(10):
    db.objects.create('products', name=f'Product {i}')

Window(Rank(), order_by=F('age').asc())

# db.objects.create('products', name='Jupe')
# item = db.objects.first('products')
# item.refresh_from_database()


# # TODO: Watch if we can create with a manual ID
# # TODO: Watch if we can create by passing a datetime in created on/modified on with auto times
