import asyncio
import dataclasses

from database.functions.text import Lower

from lorelie import log_queries
from lorelie.constraints import CheckConstraint, UniqueConstraint
from lorelie.database.base import Database
from lorelie.database.functions.window import Rank, Window
from lorelie.database.indexes import Index
from lorelie.expressions import Case, F, Q, When
from lorelie.fields.base import CharField, DateTimeField, IntegerField
from lorelie.tables import Table
from lorelie.database import registry

table = Table('products', fields=[
    CharField('name'),
    DateTimeField('created_on', auto_add=True)
],
    index=[Index('my_index', fields=['name'], condition=Q(name='Kendall'))]
)

db = Database(table, name='products')
# db.make_migrations()
db.migrate()
db.objects.create('products', name='Jupe')

# db.objects.create('products', name='Jupe')
# item = db.objects.first('products')
# item.refresh_from_database()


# # TODO: Watch if we can create with a manual ID
# # TODO: Watch if we can create by passing a datetime in created on/modified on with auto times
