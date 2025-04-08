from lorelie import log_queries
from lorelie.database.base import Database
from lorelie.database.indexes import Index
from lorelie.database.tables.base import Table
from lorelie.expressions import F, Q
from lorelie.fields.base import CharField, URLField
from lorelie.fields.relationships import ForeignKeyActions, ForeignKeyField

table2 = Table(
    'websites',
    str_field='url',
    fields=[
        URLField('url')
    ]
)

table = Table(
    'celebrities',
    str_field='name',
    # indexes=[
    #     Index('name_idx', fields=['name'], condition=None)
    # ],
    # fields=[]
    fields=[
        CharField('name'),
        ForeignKeyField(table2, 'website', on_delete=ForeignKeyActions.CASCADE)
    ]
)

# db = Database(table, name='celebs')

# db.migrate()

# qs = table.objects.all()
# item1 = qs.first()
# item2 = qs.last()


# qs = table.objects.order_by('name').order_by('-name')
# print(qs)
# print(log_queries.container)

# print(F('age'))
print('a')
