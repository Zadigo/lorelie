import unittest
import dataclasses
from lorelie.backends import BaseRow
from lorelie.exceptions import FieldExistsError
from lorelie.test.testcases import LorelieTestCase


class TestCreate(LorelieTestCase):
    def setUp(self):
        self.db = self.create_database()

    def test_simple_create(self):
        self.db.objects.create('celebrities', name='Addison Rae', height=178)
        item = self.db.objects.create(
            'celebrities',
            name='Kendall Jenner',
            height=184
        )

        # TODO: This only returns the ID field
        # when creating a new value in the database
        self.assertEqual(item.pk, 2)
        # self.assertIsInstance(item, BaseRow)
        # self.assertEqual(item.name, 'Kendall Jenner')
        # self.assertIn('Kendall Jenner', item)

    def test_create_invalid_fields(self):
        with self.assertRaises(FieldExistsError):
            self.db.objects.create('celebrities', age=34)

    def test_create_callable_parameter(self):
        # TODO: We should be able to create a value using a callable
        self.db.objects.create('celebrities', name=lambda: 'Aurélie Konaté')
        # print(self.db.objects.values('celebrities', 'name'))

    def test_create_editable_field(self):
        pass


class TestGetOrCreate(LorelieTestCase):
    def setUp(self):
        self.db = self.create_database()
    
    def test_simple_create(self):
        item = self.db.objects.get_or_create(
            'celebrities',
            name='Addison Rae', 
            height=178
        )
        self.assertIsInstance(item, BaseRow)

    def test_with_defaults_alone(self):
        # TODO: This raises an error when using
        # defaults alone
        item = self.db.objects.get_or_create(
            'celebrities',
            defaults={'name': 'Addison Rae'}
        )

    def test_both_defaults_and_fields(self):
        item = self.db.objects.get_or_create(
            'celebrities',
            defaults={'height': 194},
            name='Addison Rae'
        )


class TestUpdateOrCreate(LorelieTestCase):
    def setUp(self):
        self.db = self.create_database()

    def test_structure(self):
        qs = self.db.objects.update_or_create(
            'celebrities',
            create_defaults={'height': 130}, 
            name='Kendall Jenner'
        )
        # TODO: Does not return any data
        # self.assertEqual(len(qs), 1)
        len(qs)
        print(qs.sql_statement)

    def test_no_create_defaults(self):
        # If we have no create defaults,
        # use the parameters in the kwargs
        # to create the new value
        qs = self.db.objects.update_or_create(
            'celebrities',
            name='Kendall Jenner'
        )
        len(qs)
        self.assertEqual(
            qs.sql_statement,
            "insert into celebrities (name) values('Kendall Jenner');"
        )

    def test_create_defaults_alone(self):
        with self.assertRaises(ValueError):
            self.db.objects.update_or_create(
                'celebrities',
                create_defaults={'name': 'Kendall Jenner'}
            )


class TestBulkCreate(LorelieTestCase):
    def setUp(self):
        self.db = self.create_database()

    def setup_data(self):
        @dataclasses.dataclass
        class Celebrity:
            name: str
            height: int
        
        return [
            Celebrity('Kendall Jenner', 201),
            Celebrity('Taylor Swift', 156)
        ]

    def test_structure(self):
        test_data = self.setup_data()
        qs = self.db.objects.bulk_create('celebrities', test_data)
        self.assertEqual(
            qs.sql_statement,
            "insert into celebrities (name, height) values ('Kendall Jenner', 201), ('Taylor Swift', 156) returning id, height, name;"
        )
        self.assertTrue(len(qs) == 2)

    def test_invalid_columns(self):
        test_data = self.setup_data()
        
        @dataclasses.dataclass
        class Celebrity:
            name: str
            height: int
            age: int

        test_data.append(Celebrity('Kylie Jenner', 156, 34))

        with self.assertRaises(FieldExistsError):
            self.db.objects.bulk_create('celebrities', test_data)


# if __name__ == '__main__':
#     unittest.main()
