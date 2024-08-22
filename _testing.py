import datetime
from lorelie import log_queries
from lorelie.database.base import Database
from lorelie.database.tables.base import Table
from lorelie.fields.base import DateField

table = Table('celebrity', fields=[
    DateField('last_seen')
])

db = Database(table)
db.migrate()

d = datetime.datetime.now()
o = table.objects.create(last_seen=d.date())

o['last_seen'] = d.date()
o.save()
print(o.last_seen)
