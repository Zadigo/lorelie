from lorelie.database import Database
from lorelie.fields import CharField
from lorelie.functions import Lower, Upper, Count, Length
from lorelie.tables import Table

table = Table(
    'celebrities',
    ordering=['firstname'],
    fields=[CharField('firstname')],
    str_field='firstname'
)
db = Database(table)

db.migrate()

db.objects.create('celebrities', firstname='Kendall')
db.objects.create('celebrities', firstname='Aurélie')
db.objects.create('celebrities', firstname='Kylie')
db.objects.create('celebrities', firstname='Jade')

queryset = db.objects.all('celebrities')
queryset = db.objects.filter('celebrities', firstname__contains='K')
celebrity = db.objects.get('celebrities', firstname='Kendall')

# queryset = db.objects.annotate(
#     'celebrities',
#     lowered_firstname=Lower('firstname')
# )
# queryset = db.objects.annotate(
#     'celebrities',
#     uppered_firstname=Upper('firstname')
# )
queryset = db.objects.annotate(
    'celebrities',
    count_firstname=Count('firstname')
)
celebrity = queryset[1]
print(celebrity.count_firstname)


# celebrity.firstname = 'Julie'
# celebrity['firstname'] = 'Julie'
# updated = celebrity.save()
# print(updated.id)
# values = db.objects.as_values('celebrities', 'firstname')
# print(values)
