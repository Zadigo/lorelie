from lorelie.fields import base
from lorelie.database.base import Database
import pathlib
from lorelie import log_queries
from lorelie.tables import Table

# # In memory
# db = Database()
# # Physical
# db = Database(name='test_database')
# db = Database(name='test_database2', path=pathlib.Path('.'))
# # In memory
# db = Database(path=pathlib.Path('.'))

table = Table(
    'celebrities',
    str_field='firstname',
    fields=[
        base.CharField('firstname'),
        base.JSONField('data', default={'sweet': 1}),
        base.BooleanField('is_valid', default=False)
    ]
)
db = Database(table, log_queries=True)
db.migrate()

db.objects.create('celebrities', firstname='Kendall')

# qs = db.objects.all('celebrities')
# qs.exists()
# item = qs[0]
# item['firstname'] = 'Julie'
# item.save()
# print(item)

# item = db.objects.create('celebrities', firstname='Kylie')
# item.delete()

# print(db.objects.all('celebrities'))
# # print(log_queries.container)
# print(qs.values())


qs1 = db.objects.all('celebrities')
qs2 = db.objects.all('celebrities')
qs3 = db.objects.intersect('celebrities', qs1, qs2)
print(qs3)

# for log in log_queries.container:
#     print(log)
