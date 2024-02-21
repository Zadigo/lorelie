
from kryptone.fields import Field
from kryptone.tables import Table


def create_table(create=False):
    table = Table('celebrities', fields=[
        Field('name')
    ])
    table.prepare()
    if create:
        table.create(name='Kendall Jenner')
    return table
