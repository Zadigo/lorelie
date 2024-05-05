import unittest

from items import TEST_TABLE

from lorelie.backends import BaseRow, SQLiteBackend


class TestTable(unittest.TestCase):
    def setUp(self):
        self.table = TEST_TABLE

    def test_fields_map(self):
        self.assertIn('id', self.table.fields_map)
        self.assertIsInstance(self.table.fields_map, dict)
        self.assertTrue(self.table.has_field('id'))
        self.assertTrue(self.table.has_field('firstname'))
        self.assertIsInstance(self.table.backend, SQLiteBackend)

    def test_build_field_parameters(self):
        parameters = self.table.build_field_parameters()
        self.assertListEqual(
            parameters,
            [
                ['firstname', 'text', 'not null'],
                ['lastname', 'text', 'not null'],
                ['followers', 'integer', 'not null'],
                ['id', 'integer', 'primary key', 'autoincrement', 'not null']
            ]
        )

    def test_sqls(self):
        field_params = self.table.build_field_parameters()
        field_params = (
            self.table.backend.simple_join(params)
            for params in field_params
        )
        create_table_sql = self.table.create_table_sql(
            self.table.backend.comma_join(field_params)
        )
        self.assertListEqual(
            create_table_sql,
            ['create table if not exists celebrities (firstname text not null, lastname text not null, followers integer not null, id integer primary key autoincrement not null)']
        )

        drop_table_sql = self.table.drop_table_sql('')
        self.assertListEqual(
            drop_table_sql,
            ['drop table if exists celebrities']
        )

    # TODO: Remove, we do not construct
    # individual tables anymore but wrap
    # them in the Database class
    # def test_prepare_table(self):
    #     # 1. Build the field parameters
    #     # 2. Create the table in SQlite
    #     self.table.prepare()
    #     self.table.create(name='Kendall Jenner')
    #     result = self.table.filter(name__contains='Kendall')
    #     self.assertEqual(result[0].name, 'Kendall Jenner')

    #     # Test queries
    #     row = self.table.first()
    #     self.assertIsInstance(row, BaseRow)

    #     row = self.table.last()
    #     self.assertIsInstance(row, BaseRow)


if __name__ == '__main__':
    unittest.main()
