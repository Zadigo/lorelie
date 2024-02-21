from lorelie.fields import CharField
from lorelie.functions import Length
from lorelie.tables import Table


table = Table('single_table', inline_build=True, fields=[
    CharField('name')
])
table.prepare()
table.create(name='Kendall')
q = table.first()
# q = table.annotate(length=Length('name'))
# print(q[0])
print(q)
