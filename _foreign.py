from lorelie.database.base import Database
from lorelie.fields.base import CharField, IntegerField
from lorelie.tables import Table

table1 = Table('names', fields=[
    CharField('firstname')
])

table2 = Table('ages', fields=[
    IntegerField('age')
])

db = Database(table1, table2)
db.foreign_key(
    table1,
    table2,
    related_name='custom_name'
)
db.migrate()

# names__age__eq = 1
# names__eq = 1
# names__age__another__eq = 1
