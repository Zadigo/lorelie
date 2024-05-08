import unittest

from lorelie.backends import SQLiteBackend
from lorelie.fields import AutoField, BooleanField, Field, IntegerField
from lorelie.tables import Table

table = Table('celebrities', fields=[])
table.backend = SQLiteBackend()
table.prepare()


class TestField(unittest.TestCase):
    def test_field_params(self):
        field = Field('name')
        field.prepare(table)

        # The default composition for the parameters
        # of a field should be the following:
        result = field.field_parameters()
        self.assertListEqual(result, ['name', 'text', 'not null'])

        field.null = True
        field.default = 'Kendall'
        result = field.field_parameters()
        self.assertListEqual(
            result,
            ['name', 'text', 'default', "'Kendall'", 'null']
        )

        field.unique = True
        result = field.field_parameters()
        self.assertListEqual(
            result,
            ['name', 'text', 'default', "'Kendall'", 'null', 'unique']
        )

        field.max_length = 100
        result = field.field_parameters()
        self.assertListEqual(
            result,
            ['name', 'varchar(100)', 'default', "'Kendall'", 'null', 'unique']
        )

    def test_to_database(self):
        field = Field('name')
        field.prepare(table)

        result = field.to_database('Kendall')
        self.assertEqual(result, 'Kendall')

    def test_desconstruction(self):
        field = Field('name')
        name, parameters = field.deconstruct()
        self.assertIsInstance(name, str)
        self.assertIsInstance(parameters, list)
        self.assertEqual(name, 'name')
        self.assertIn('name', parameters)


class TestIntegerField(unittest.TestCase):
    def test_result(self):
        field = IntegerField('age')
        field.prepare(table)

        params = field.field_parameters()
        self.assertListEqual(
            params,
            ['age', 'integer', 'not null']
        )

        result = field.to_database(1)

        self.assertIsInstance(result, int)
        self.assertEqual(result, 1)


class TestBooleanField(unittest.TestCase):
    def test_result(self):
        field = BooleanField('completed')
        field.prepare(table)

        params = field.field_parameters()
        self.assertListEqual(
            params,
            ['completed', 'text', 'not null']
        )

        result = field.to_database(0)
        self.assertEqual(result, 0)

        result = field.to_database(True)
        self.assertEqual(result, 1)

        result = field.to_database('true')
        self.assertEqual(result, 1)


class TestAutoField(unittest.TestCase):
    def test_result(self):
        field = AutoField()
        field.prepare(table)

        name, params = field.deconstruct()
        self.assertListEqual(
            params,
            ['id', 'integer', 'primary key', 'autoincrement', 'not null']
        )


if __name__ == '__main__':
    unittest.main()
