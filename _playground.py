import pathlib


from lorelie.database.base import Database
from lorelie.database.tables.base import Table
from lorelie.fields.base import CharField

fields = [
    CharField('name', max_length=5)
]

tb = Table('company', fields=fields)


db = Database(tb, name='companies', path=pathlib.Path(
    '.'), log_queries=True, mask_values=True)
db.migrate()

tb.objects.create(name='Kendall')
print(tb.objects.all())
