import unittest

from lorelie.backends import BaseRow
from lorelie.database import Database
from lorelie.fields import CharField
from lorelie.queries import Query
from lorelie.tables import Table


class TestQuery(unittest.TestCase):
    def setUp(self):
        table = Table('celebrities', fields=[CharField('name')])
        db = Database(table)
        db.migrate()

        select_clause = table.backend.SELECT.format_map({
            'fields': table.backend.comma_join(['rowid', '*']),
            'table': table.backend.quote_value('celebrities')
        })
        self.instance = Query(table.backend, [select_clause], table=table)

    def test_run(self):
        self.instance.run()
        first_row = self.instance.result_cache[0]
        self.assertIsInstance(first_row, BaseRow)
        self.assertIn('Kendall Jenner', first_row)


if __name__ == '__main__':
    unittest.main()
