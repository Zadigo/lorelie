from lorelie.database.base import Database
from lorelie.fields.base import CharField, IntegerField
from lorelie.fields.relationships import ForeignKeyActions, ForeignKeyField
from lorelie.tables import Table
from lorelie import log_queries

table1 = Table('celebrities', fields=[
    CharField('firstname')
])

table2 = Table('pictures', fields=[
    CharField('url'),
    ForeignKeyField(table1, 'r_pictures', on_delete=ForeignKeyActions.CASCADE)
])

db = Database(table1, table2, name='foreign_db')
db.migrate()

table1.objects.create(firstname='Kendall Jenner')
table2.objects.create(url='https://example.com/1.jpg')

# qs = table2.objects.all()
# print(qs)
# names__age__eq = 1
# names__eq = 1
# names__age__another__eq = 1


# class D:
#     def __init__(self):
#         self.table = None

#     def __get__(self, instance, cls=None):
#         self.table = instance
#         return self

#     def create(self):
#         print('create', self.table)
#         self.table.validate()


# class A(type):
#     def __new__(cls, name, bases, attrs):
#         return super().__new__(cls, name, bases, attrs)


# class B(metaclass=A):
#     objects = D()

#     def __init__(self):
#         self.b = 1

#     def validate(self):
#         print('validate', self, self.fields_map)


# class C(B):
#     def __init__(self, name, fields=[]):
#         self.name = name
#         self.fields_map = {}
#         for i, field in enumerate(fields):
#             self.fields_map[field] = f'Great {i}'

#     def __repr__(self):
#         return f'<C: {self.name}>'


# c = C('right', fields=[1])
# d = C('left', fields=[2])

# print(c.objects)
# print(d.objects)
