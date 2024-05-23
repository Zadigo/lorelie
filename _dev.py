from database.functions.text import Lower
from lorelie.constraints import CheckConstraint, UniqueConstraint
from lorelie.database.base import Database
from lorelie.database.indexes import Index
from lorelie.expressions import F, Q, Case, When
from lorelie.fields.base import CharField
from lorelie.tables import Table
import dataclasses

table = Table(
    'celebrities',
    fields=[
        CharField('name')
    ],
    constraints=[
        UniqueConstraint('name', fields=['name']),
        CheckConstraint('age', Q(age__gt=22))
    ],
    index=[
        Index('idx_age', fields=['age'], condition=Q(age__gt=25))
    ],
    str_field='name',
    ordering=['name']
)

db = Database(table)
db.migrate()

db.objects.all('celebrities')

db.objects.order_by('celebrities', 'name')

db.objects.count('celebrities')

db.objects.first('celebrities')
db.objects.last('celebrities')

db.objects.create('celebrities', name='Anya-Taylor Joy')

db.objects.filter('celebrities', Q(name_contains='Anya-Taylor Joy'))
db.objects.filter('celebrities', Q(name__contains='Anya-Taylor Joy') | Q(name__contains='Eugénie Bouchard'))

db.objects.get('celebrities', Q(name_contains='Anya-Taylor Joy'))

db.objects.annotate('celebrities', lowered_name=Lower('name'))

db.objects.values('celebrities', 'name')

db.objects.dataframe('celebrities', 'name')


@dataclasses.dataclass
class Celebrity:
    name: str
celebrities = [Celebrity('Jennifer Lawrence')]
db.objects.bulk_create('celebrities', celebrities)


# TODO: Watch if we can create with a manual ID
# TODO: Watch if we can create by passing a datetime in created on/modified on with auto times
