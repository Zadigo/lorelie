import time

from lorelie.aggregation import Avg, Count
from lorelie.database.base import Database
from lorelie.database.indexes import Index
from lorelie.expressions import Case, Q, When
from lorelie.fields.base import (CharField, DateField, DateTimeField,
                                 IntegerField, JSONField, Value)
from lorelie.functions import (ExtractDay, ExtractMonth, ExtractYear, Length,
                               Lower, Upper)
from lorelie.tables import Table


def example_validator(value):
    pass


celebrities = Table(
    'celebrities',
    ordering=['firstname'],
    fields=[
        CharField('firstname', max_length=200),
        CharField('lastname', validators=[example_validator]),
        IntegerField('age', null=True, min_value=18, max_value=99),
        IntegerField('followers', default=0),
        JSONField('goals', null=True),
        DateField('date_of_birth', null=True),
        DateTimeField('created_on', auto_add=True)
    ],
    index=[
        Index('firstname_index', 'firstname')
    ],
    str_field='firstname'
)

social_media = Table(
    'socialmedia',
    fields=[
        CharField('name', unique=True),
        DateTimeField('created_on', auto_add=True)
    ]
)

# TODO: Raise en error when trying to create
# a foreign key with a table that does not
# exist in the table map
db = Database(celebrities, social_media)
db.foreign_key(
    celebrities,
    social_media,
    on_delete=None,
    related_name='f_my_table'
)
db.migrate()

celebrities = [
    {
        'firstname': 'Kendall',
        'lastname': 'Jenner',
        'age': 19,
        'followers': 1000
    },
    {
        'firstname': 'Kylie',
        'lastname': 'Jenner',
        'age': 20,
        'followers': 100
    },
    {
        'firstname': 'Margot',
        'lastname': 'Robbie',
        'age': None,
        'followers': 156600
    },
    {
        'firstname': 'Aurélie',
        'lastname': 'Konaté',
        'age': None,
        'followers': 156600
    },
    {
        'firstname': 'Jade',
        'lastname': 'Parka',
        'age': None,
        'followers': 156600
    },
    {
        'firstname': 'Lena',
        'lastname': 'Situation',
        'age': 27,
        'followers': 4454
    },
    {
        'firstname': 'Aya',
        'lastname': 'Nakamura',
        'date_of_birth': '1992-1-1',
        'age': None,
        'goals': {'do_as_show': True},
        'followers': 334345
    }
]

for celebrity in celebrities:
    db.objects.create('celebrities', **celebrity)


# print(db.objects.all('lorelie_migrations'))

# celebrity = db.objects.get(
#     'celebrities',
#     firstname='Kendall',
#     lastname='Jenner'
# )
# celebrity = db.objects.first('celebrities')
# celebrity = db.objects.last('celebrities')
# celebrity['firstname'] = 'Vlada'
# celebrity.save()
# print(celebrity.firstname)

# queryset = db.objects.all('celebrities')

# queryset = db.objects.order_by('celebrities', 'firstname', '-lastname')
# print(queryset)

# values = db.objects.values('celebrities', 'firstname')
# print(values)

# df = db.objects.dataframe('celebrities')
# print(df)

# queryset = db.objects.filter('celebrities', firstname='Aya', lastname='Nakamura')
# queryset = db.objects.filter('celebrities', lastname__contains='Jenner')
# queryset = db.objects.filter('celebrities', age__eq=20)
# queryset = db.objects.filter('celebrities', age__gte=20)
# queryset = db.objects.filter('celebrities', age__gt=20)
# queryset = db.objects.filter('celebrities', age__lte=20)
# queryset = db.objects.filter('celebrities', age__lt=20)
# queryset = db.objects.filter('celebrities', age__isnull=True)
# queryset = db.objects.filter('celebrities', lastname__in=['Jenner', 'Nakamura'])
# queryset = db.objects.filter('celebrities', lastname__startswith='Par')
# queryset = db.objects.filter('celebrities', lastname__endswith='mura')
# queryset = db.objects.filter('celebrities', lastname__ne='Jenner')
# queryset = db.objects.filter('celebrities', age__range=[20, 25])

# a = Q(firstname='Kendall')
# b = Q(lastname='Jenner')
# c = Q(age__gte=20)
# queryset = db.objects.filter('celebrities', a | b)
# TODO: Verify this section
# queryset = db.objects.filter('celebrities', a | b | c)
# queryset = db.objects.filter('celebrities', a | b & c)
# queryset = db.objects.filter('celebrities', a, lastname='Jenner')
# queryset = db.objects.filter('celebrities', a | b, age__in=[20])


# queryset = db.objects.values('celebrities', 'goals')
# queryset = db.objects.annotate('celebrities', lowered=Lower('firstname'))
# queryset = db.objects.annotate('celebrities', upped=Upper('firstname'))
# queryset = db.objects.annotate('celebrities', count=Count('firstname'))
# queryset = db.objects.annotate(
#     'celebrities',
#     year=ExtractYear('date_of_birth'),
#     month=ExtractMonth('date_of_birth'),
#     day=ExtractDay('date_of_birth')
# )
# queryset.values('id')
# print(queryset.sql_statement)


# async def main():
#     queryset = await db.objects.async_all('celebrities')
#     print(queryset)

# asyncio.run(main())

# queryset = db.objects.annotate('celebrities', name_count=Count(
#     'firstname'), name_length=Length('firstname'))
# print(queryset.last().goals)
# condition = When('firstname__eq=Kendall', 'KendallKendall')
# case = Case(condition)
# db.objects.annotate('celebrities', some_name=case)
# print(queryset.values())

# celebrity.firstname = 'Julie'
# celebrity['firstname'] = 'Julie'
# updated = celebrity.save()
# print(updated.id)
# values = db.objects.as_values('celebrities', 'firstname')
# print(values)


# db.objects.create(
#     'celebrities',
#     firstname=Value('google', output_field=CharField)
# )
# db.objects.filter('celebrities', firstname=Value('Kendall'))

# print(db.celebrities_tbl.objects.all('celebrities'))

# print(queryset.values('year', 'month', 'day'))

# result = db.objects.aggregate('celebrities', Count('age'), Avg('age'))
# result = db.objects.aggregate('celebrities', avg_age=Avg('age'))
# print(result)


# count = db.objects.count('celebrities')
# print(count)


# qs = db.objects.filter('celebrities', firstname='Margot')


# print(qs.all())
# print(qs.first())
# print(qs.last())
# print(qs.count())
# print(qs.values('id'))

# qs.update(age=26)
# db.objects.values('celebrities', 'firstname', 'age')

f = db.objects.foreign_table('celebrities__socialmedia')
print(vars(f))
