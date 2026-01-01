import pathlib


from lorelie.database.base import Database
from lorelie.database.tables.base import Table
from lorelie.fields.base import CharField
from lorelie.database.indexes import Index
from lorelie.expressions import Q

fields = [
    CharField('name', max_length=5)
]

indexes = [
    Index('unique_name', ['name']),
    Index('another_index', ['name'], condition=Q(name='Kendall'))
]

tb = Table('company', fields=fields, indexes=indexes)


db = Database(tb, name='companies', path=pathlib.Path('.'), log_queries=True)
db.migrate()

tb.objects.create(name='Kendall')
