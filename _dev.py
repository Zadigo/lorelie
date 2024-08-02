import asyncio
import dataclasses

from lorelie import log_queries
from lorelie.database.base import Database
from lorelie.fields.base import CharField, DateTimeField
from lorelie.tables import Table

table = Table(
    'products',
    str_field='name',
    fields=[
        CharField('name'),
        DateTimeField('created_on', auto_add=True)
    ]
)

db = Database(table)
db.migrate()


@dataclasses.dataclass
class Celebrity:
    name: str


table.objects.create(name='Kendall Jenner')
table.objects.create(name='Kylie Jenner')
qs = table.objects.all()
# print(log_queries.container)
