from lorelie.database import Database
from lorelie.expressions import Case, Q, When
from lorelie.fields import CharField
from lorelie.functions import Count, Length, Lower, Upper
from lorelie.tables import Table

table = Table(
    'celebrities',
    ordering=['firstname'],
    fields=[CharField('firstname')],
    str_field='firstname'
)
db = Database(table)
# db.many_to_many(table, table, related_name='my_name')
db.migrate()

db.objects.create('celebrities', firstname='Kendall')
db.objects.create('celebrities', firstname='Aurélie')
db.objects.create('celebrities', firstname='Kylie')
db.objects.create('celebrities', firstname='Jade')

# queryset = db.objects.all('celebrities')
# queryset = db.objects.filter('celebrities', firstname__contains='K')
# celebrity = db.objects.get('celebrities', firstname='Kendall')

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
# async def main():
#     queryset = await db.objects.async_all('celebrities')
#     print(queryset)

# asyncio.run(main())

# db.objects.annotate('celebrities', name_count=Count('firstname'), name_length=Length('firstname'))
# condition = When('firstname__eq=Kendall', 'KendallKendall')
# case = Case(condition)
# db.objects.annotate('celebrities', some_name=case)


# celebrity.firstname = 'Julie'
# celebrity['firstname'] = 'Julie'
# updated = celebrity.save()
# print(updated.id)
# values = db.objects.as_values('celebrities', 'firstname')
# print(values)


# a = Q(firstname='Kendall')
# b = Q(firstname='Kylie')
# c = Q(firstname='Jade')
# db.objects.filter('celebrities', a | b)
# db.objects.filter('celebrities', a | b | c)
# db.objects.filter('celebrities', a | b & c)
# db.objects.filter('celebrities', a, firstname='Jade')
# db.objects.filter('celebrities', a | b, firstname='Jade')
