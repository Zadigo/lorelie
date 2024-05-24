import asyncio
import dataclasses

from database.functions.text import Lower

from lorelie import log_queries
from lorelie.constraints import CheckConstraint, UniqueConstraint
from lorelie.database.base import Database
from lorelie.database.indexes import Index
from lorelie.expressions import Case, F, Q, When
from lorelie.fields.base import CharField, DateTimeField, IntegerField
from lorelie.tables import Table
from lorelie.database.functions.window import Window, Rank

table = Table('products', fields=[
    CharField('name'),
    DateTimeField('created_on', auto_add=True)
])

db = Database(table)
db.migrate()

db.objects.create('products', name='Julie')
db.objects.update_or_create('products', name='Julie')
db.objects.get_or_create('products', name='Justine')

qs = db.objects.annotate(
    'products',
    Window(Rank('name'), partition_by=F('name'))
)
print(qs)
print(list(log_queries))


# async def create():
#     await db.objects.acreate('products', name='Julie')
#     await asyncio.sleep(1)


# async def getall():
#     qs = await db.objects.aall('products')
#     print(qs)
#     await asyncio.sleep(4)


# async def main():
#     while True:
#         t1 = asyncio.create_task(create())
#         t2 = asyncio.create_task(getall())
#         asyncio.gather(t1, t2)
#         await asyncio.sleep(2)

# asyncio.run(main())


# # TODO: Watch if we can create with a manual ID
# # TODO: Watch if we can create by passing a datetime in created on/modified on with auto times
