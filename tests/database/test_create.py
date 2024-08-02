import dataclasses
from lorelie.backends import BaseRow
from lorelie.exceptions import FieldExistsError
from lorelie.test.testcases import LorelieTestCase


class TestCreate(LorelieTestCase):
    def setUp(self):
        self.db = self.create_database()

    def test_simple_create(self):
        items = [
            {
                'name': 'Addison Rae',
                'height': 178
            },
            {
                'name': 'Kendall Jenner',
                'height': 184
            }
        ]

        for item in items:
            with self.subTest(item=item):
                obj = self.db.celebrities.objects.create(**item)
                self.assertIn(obj.pk, [1, 2])
                self.assertIsInstance(obj, BaseRow)
                self.assertIn(obj.name, ['Addison Rae', 'Kendall Jenner'])

    def test_create_invalid_fields(self):
        with self.assertRaises(FieldExistsError):
            self.db.celebrities.objects.create(age=34)

    def test_create_with_callable(self):
        self.db.celebrities.objects.create(name=lambda: 'Aurélie Konaté')

    def test_create_editable_field(self):
        pass


class TestGetOrCreate(LorelieTestCase):
    def setUp(self):
        self.db = self.create_database()

    def test_simple_create(self):
        items = [
            {
                'name': 'Addison Rae',
                'height': 178
            },
            {
                'name': 'Kendall Jenner',
                'height': 184
            }
        ]
        for item in items:
            obj = self.db.celebrities.objects.get_or_create(
                create_defaults=item,
                name=item['name']
            )
            self.assertIsInstance(obj, BaseRow)

    def test_with_defaults_alone(self):
        # FIXME: This raises an error when using
        # defaults alone
        obj = self.db.celebrities.objects.get_or_create(
            create_defaults={'name': 'Addison Rae'}
        )

    def test_both_defaults_and_fields(self):
        obj = self.db.celebrities.objects.get_or_create(
            create_defaults={'height': 194},
            name='Addison Rae'
        )
        self.assertIsInstance(obj, BaseRow)

    def test_dictionnary_values_as_defaults(self):
        # Case where {'defaults': {}, 'name': 'Addison'} is not
        # handled by the function
        self.db.celebrities.objects.get_or_create(defaults={}, name='Addison')


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
