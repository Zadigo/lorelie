from lorelie.backends import SQLiteBackend
from lorelie.fields.base import CharField, IntegerField
from lorelie.tables import Table


class FakeTable:
    name = 'fake_table'

# FIXME: Determine if we need the inline build
TEST_TABLE = Table('celebrities', inline_build=True, fields=[
    CharField('firstname'),
    CharField('lastname'),
    IntegerField('followers')
])
TEST_TABLE.backend = SQLiteBackend()
TEST_TABLE.prepare()
