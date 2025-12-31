import pathlib


from lorelie.database.base import Database
from lorelie.database.tables.base import Table
from lorelie.fields.base import CharField

fields = [
    CharField('name', max_length=5)
]
tb = Table('company', fields=fields)
db = Database(tb, name='companies', path=pathlib.Path('.'))
db.migrate()

# print(tb.objects.create(name='Test Company 1'))
