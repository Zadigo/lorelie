from django.forms import FloatField
from lorelie.constraints import CheckConstraint, UniqueConstraint
from lorelie.database.base import Database
from lorelie.database.indexes import Index
from lorelie.database.tables.base import Table
from lorelie.expressions import F, Q
from lorelie.fields.base import CharField, IntegerField, BooleanField, DateField, DateTimeField, URLField, SlugField
from lorelie.database.views import View


def name_validator(value: str):
    if value == 'Facebook':
        raise ValueError("Name must contain only alphabetic characters.")


fields = [
    CharField('name', max_length=5, unique=True, validators=[name_validator]),
    IntegerField('revenue', min_value=0, max_value=1000000),
    FloatField('score'),
    BooleanField('is_active', default=True),
    URLField('website'),
    DateField('founded_date'),
    DateTimeField('last_updated'),
    SlugField('slug', max_length=50),
]

constraints = [
    UniqueConstraint(fields=['name', 'website'], name='unique_name_website'),
    CheckConstraint('score', Q(score__gte=0.0) & Q(score__lte=100.0)),
]

indexes = [
    Index('revenue', ['revenue'], Q(revenue__gte=50000)),
    Index('active', ['is_active'], Q(is_active=True)),
]

tb = Table(
    'company',
    fields=fields,
    constraints=constraints,
    ordering=['-revenue'],
    str_field='name'
)


db = Database(tb, log_queries=True)
db.migrate()

tb.objects.create(
    name='OpenAI',
    revenue=100000,
    score=95.5,
    is_active=True,
    website='https://openai.com',
    founded_date='2015-12-11',
    last_updated='2024-01-01T12:00:00Z',
    slug='openai'
)

tb.objects.create(
    name='Google',
    revenue=900000,
    score=98.0,
    is_active=True,
    website='https://google.com',
    founded_date='1998-09-04',
    last_updated='2024-01-02T12:00:00Z',
    slug='google'
)

qs = tb.objects.all()
company = tb.objects.get(id=1)
company = qs.get(name='OpenAI')
qs2 = tb.objects.filter(revenue__gte=50000).order_by('-score')
qs3 = qs2.annotate(profit_margin=F('revenue') * 0.2)
qs4 = tb.objects.exclude(name='Google')

view = View('active_companies', tb.objects.filter(is_active=True))
qs = view(tb)
