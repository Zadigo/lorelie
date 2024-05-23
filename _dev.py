from lorelie.database.base import Database
from lorelie.fields.base import CharField
from lorelie.tables import Table

table = Table('celebrities', fields=[
    CharField('name')
])

db = Database(table)
db.migrate()

# TODO: Watch if we can create with a manual ID
# TODO: Watch if we can create by passing a datetime in created on/modified on with auto times
