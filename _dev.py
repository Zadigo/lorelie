from lorelie import log_queries
from lorelie.database.indexes import Index
from lorelie.constraints import CheckConstraint, UniqueConstraint
from lorelie.database.functions.aggregation import Count
from lorelie.database.base import Database
from lorelie.expressions import F, Q, Case, Value, When
from lorelie.fields.base import DateTimeField, IntegerField, CharField, JSONField, CommaSeparatedField
from lorelie.tables import Table

fields = [
    CharField('name', null=True),
    IntegerField('unit_price', default=0),
    IntegerField('price', default=0),
    JSONField('meta', null=True),
    CommaSeparatedField('items', null=True),
    DateTimeField('created_on', auto_add=True)
]

constraints = [
    # FIXME: When using jupe -> Nothing, Jupe -> Valid
    # CheckConstraint('some_constraint', Q(name='jupe'))

    # NOTE: Jupe, jupe will not raise unique constraint
    # UniqueConstraint('one_jupe', fields=['name'])

    CheckConstraint('price', Q(price__gte=0)),
    CheckConstraint('price', Q(unit_price__gte=0) & Q(unit_price__lt=1000))
]

indexes = [
    Index('name', fields=['name'], condition=Q(name__contains='jupe'))
]

table = Table(
    'products', 
    fields=fields, 
    constraints=constraints,
    ordering=['name'],
    index=indexes
)

db = Database(table)
db.migrate()

db.objects.create('products', name='jupe a')
db.objects.create('products', name='jupe b', price=34)
db.objects.update_or_create('products', create_defaults={'price': 25}, name='jupe b')
qs = db.objects.filter('products', name__contains='jupe')
print(qs)

# TODO: Watch if we can create with a manual ID
# TODO: Watch if we can create by passing a datetime in created on/modified on with auto times
