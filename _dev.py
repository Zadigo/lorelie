# from lorelie.database.functions.text import Concat

# Concat('something', 'another')

# # from lorelie.aggregation import Count
# # from lorelie.database.base import Database
# # from lorelie.expressions import F, Q, Case, Value, When
# # from lorelie.fields.base import IntegerField
# # from lorelie.tables import Table

# # table = Table('products', fields=[
# #     IntegerField('unit_price', default=0),
# #     IntegerField('price', default=0)
# # ])

# # db = Database(table)
# # db.migrate()

# # db.objects.create('products', unit_price=10, price=12)
# # db.objects.create('products', unit_price=12, price=16)

# # # Invalid usage
# # # db.objects.annotate('products', F('price'))
# # # db.objects.annotate('products', F('price') + F('price'))
# # # db.objects.annotate('products', Value(1))
# # # db.objects.annotate('products', Q(price__gt=1))

# # # Valid usage
# # qs = db.objects.annotate('products', my_price=F('price'))
# # # qs = db.objects.annotate('products', my_price=F('price'))
# # # qs = db.objects.annotate('products', my_price=F('price') + F('price'))
# # # qs = db.objects.annotate('products', my_price=F('price') + F('price') - 1)
# # # qs = db.objects.annotate('products', my_price=F('price') + 1)
# # # qs = db.objects.annotate('products', my_price=Value(1))
# # # qs = db.objects.annotate('products', my_price=Q(price__gt=1))

# # # case = Case(When('price__eq=10', then_case=1), default=30)
# # # qs = db.objects.annotate('products', my_price=case)

# # # qs = db.objects.annotate('products', Count('price'))
# # # qs = db.objects.annotate('products', count_price=Count('price'))
# # # qs = db.objects.annotate('products', Count('price'), Count('unit_price'))

# # print(qs)
# # print(qs.values())
