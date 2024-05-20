from lorelie.database.base import Database
from lorelie.expressions import F
from lorelie.fields.base import IntegerField
from lorelie.tables import Table

table = Table('products', fields=[
    IntegerField('price', default=0)
])

db = Database(table, name='products')
db.migrate()

# db.objects.create('products', price=1)
qs = db.objects.annotate('products', my_name=F('price') + F('price'))
print(qs)
print(qs.sql_statement)
print(qs.values())
