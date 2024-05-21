import unittest
from sqlite3 import OperationalError

from lorelie.backends import SQLiteBackend
from lorelie.database.base import Database
from lorelie.fields.base import CharField
from lorelie.queries import Query
from lorelie.tables import Table


class TestQueryStructure(unittest.TestCase):
    def setUp(self) -> None:
        self.query = Query(
            ['select * from celebrities'],
            backend=SQLiteBackend()
        )

    def test_sqlite_raises_error(self):
        # Query needs an existing table in the
        # database to execute a valid query
        with self.assertRaises(OperationalError):
            self.query.run()

    def test_structure(self):
        table = Table('celebrities', fields=[CharField('firstname')])
        db = Database(table)
        db.objects.create('celebrities', firstname='Kendall')
        db.migrate()

        query = Query(["select rowid, * from celebrities"], table=table)
        query.run()
        print(query.result_cache)


# class TestQuery(unittest.TestCase):
#     def setUp(self):
#         table = Table('celebrities', fields=[CharField('name')])
#         db = Database(table)
#         db.migrate()

#         select_clause = table.backend.SELECT.format_map({
#             'fields': table.backend.comma_join(['rowid', '*']),
#             'table': table.backend.quote_value('celebrities')
#         })
#         self.instance = Query(select_clause, table=table)

#     @unittest.expectedFailure
#     def test_cannot_run_if_not_evaluated(self):
#         self.instance.run()

#     @unittest.expectedFailure
#     def test_run(self):
#         first_row = self.instance.result_cache[0]
#         self.assertIsInstance(first_row, BaseRow)
#         self.assertIn('Kendall Jenner', first_row)

if __name__ == '__main__':
    unittest.main()
