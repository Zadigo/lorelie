from lorelie.database import Database
from lorelie.expressions import Case, Q, When
from lorelie.fields import CharField, IntegerField, JSONField, Value
from lorelie.functions import Count, Length, Lower, Upper
from lorelie.tables import Table

table = Table(
    'celebrities',
    ordering=['firstname'],
    fields=[
        CharField('firstname'),
        CharField('lastname'),
        IntegerField('age', null=True),
        IntegerField('followers', default=0),
        JSONField('goals', null=True)
    ],
    str_field='firstname'
)
db = Database(table)
# db.many_to_many(table, table, related_name='my_name')
db.migrate()


celebrities = [
    {
        'firstname': 'Kendall',
        'lastname': 'Jenner',
        'age': None,
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
        'age': None,
        'followers': 334345
    }
]

for celebrity in celebrities:
    db.objects.create('celebrities', **celebrity)

# celebrity = db.objects.get(
#     'celebrities',
#     firstname='Kendall',
#     lastname='Jenner'
# )
# print(celebrity)

# db.objects.create('celebrities', firstname='Kendall', lastname='Jenner', age=20)
# db.objects.create('celebrities', firstname='Aurélie', lastname='Konaté')
# db.objects.create('celebrities', firstname='Kylie', lastname='Jenner')
# db.objects.create('celebrities', firstname='Jade', lastname='Parka')
# db.objects.create('celebrities', firstname='Aya', lastname='Nakamura', goals={'age': 26})

# queryset = db.objects.all('celebrities')

# values = db.objects.values('celebrities', 'firstname')
# print(values)

# df = db.objects.dataframe('celebrities')
# print(df)

# queryset = db.objects.filter('celebrities', firstname='Kendall', lastname='Jenner')
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
# queryset = db.objects.annotate(
#     'celebrities',
#     lowered_firstname=Lower('firstname')
# )
# queryset = db.objects.annotate(
#     'celebrities',
#     uppered_firstname=Upper('firstname')
# )
# queryset = db.objects.annotate(
#     'celebrities',
#     count_firstname=Count('firstname')
# )

# print(queryset)

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
