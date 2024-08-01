from lorelie.backends import SQLiteBackend
from lorelie.fields.base import IntegerField
from lorelie.tables import Table


def get_backend():
    backend = SQLiteBackend()
    table = Table('celebrities', fields=[IntegerField('age')])
    table.backend = backend
    backend.set_current_table(table)
    return backend
