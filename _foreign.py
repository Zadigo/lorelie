from lorelie import log_queries
from lorelie.database.base import Database
from lorelie.database.tables.base import RelationshipMap, Table
from lorelie.expressions import F, Q
from lorelie.fields.base import CharField, IntegerField
from lorelie.fields.relationships import ForeignKeyActions, ForeignKeyField

RelationshipMap.forward_field_name

celebrity = Table('celebrity', fields=[
    CharField('name')
])

image_table = Table('image', fields=[
    ForeignKeyField(celebrity, 'celebrity_image')
])

db = Database(celebrity, image_table, name='google')
db.migrate()

# print(celebrity.objects.filter(
#     Q(name__contains='Kendall') | ~Q(name__contains='Addison')))
# print(celebrity.objects.filter(Q(name=F('name') + 'Google')))
print(log_queries.container)

# for i in range(3):
#     new_celebrity = celebrity.objects.create(name='Addison Rae')
#     image = image_table.objects.create(celebrity_image=new_celebrity)

# qs = celebrity.objects.all()
# item = qs[0]
# # On joined columns we need to implement celebrity.id
# # as opposed to using the "id" otherwise it cannot know
# # which id field to query
# # see: select * from celebrity inner join image on celebrity.id=image.celebrity_id where id=1;
# # should be "where celebrity.id=1"
# qs2 = item.celebrity_image_set.filter(id=1)
# list(qs2)
# print(qs.query.sql)
