import unittest

from lorelie.backends import SQLiteBackend
from lorelie.fields.base import IntegerField
from lorelie.tables import Table

backend = SQLiteBackend()
table = Table('celebrities', fields=[IntegerField('age')])
table.backend = backend
backend.set_current_table(table)


class TestMath(unittest.TestCase):
    pass

if __name__ == '__main__':
    unittest.main()
