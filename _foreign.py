from lorelie.database.base import Database
from lorelie.fields.base import CharField, IntegerField
from lorelie.fields.relationships import ForeignKeyActions, ForeignKeyField
from lorelie.tables import Table

table1 = Table('celebrities', fields=[
    CharField('firstname')
])

table2 = Table('pictures', fields=[
    ForeignKeyField(table1, 'r_pictures', on_delete=ForeignKeyActions.CASCADE)
])

db = Database(table1, table2)
db.migrate()

table2.objects.all()

# names__age__eq = 1
# names__eq = 1
# names__age__another__eq = 1
