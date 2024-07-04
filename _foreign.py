from lorelie.database.base import Database
from lorelie.fields.base import CharField, IntegerField
from lorelie.tables import Table

table1 = Table('names', fields=[
    CharField('age')
])

table2 = Table('ages', fields=[
    IntegerField('age')
])

db = Database()
db.migrate()
