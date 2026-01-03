import datetime
import uuid

from lorelie.exceptions import ValidationError
from lorelie.fields.base import (AutoField, BooleanField, CharField, DateField, DateTimeField, Field, FloatField,
                                 IntegerField, JSONField, UUIDField)
from lorelie.test.testcases import LorelieTestCase


class TestField(LorelieTestCase):
    def test_structure(self):
        field = Field('name')
        params = field.field_parameters()

        self.assertEqual(params, ['name', 'text', 'not null'])
        self.assertEqual(field.to_python('Kendall Jenner'), 'Kendall Jenner')
        self.assertEqual(field.to_database('Kendall Jenner'), 'Kendall Jenner')
        self.assertFalse(field.editable)
        self.assertFalse(field.unique)
        self.assertIsNone(field.max_length)
        self.assertIsNone(field.verbose_name)
        self.assertFalse(field.null)
        self.assertFalse(field.is_relationship_field)
        self.assertEqual(field.index, 0)
        self.assertDictEqual(field.base_field_parameters, {
            'primary key': False,
            'not null': True,
            'unique': False
        })
        self.assertEqual(field.field_type, 'text')
        self.assertTrue(field.is_standard_field_type)
        self.assertEqual(field.to_python('Kendall Jenner'), 'Kendall Jenner')

    def test_default(self):
        defaults = [
            ('Kendall', ['name', 'text', 'default', "'Kendall'", 'not null']),
            ('25', ['name', 'text', 'default', "'25'", 'not null']),
            ('3.14', ['name', 'text', 'default', "'3.14'", 'not null']),
            ('1', ['name', 'text', 'default', "'1'", 'not null']),
            ('0', ['name', 'text', 'default', "'0'", 'not null']),
            (
                lambda: 'Kendall',
                ['name', 'text', 'default', "'Kendall'", 'not null']
            )
        ]

        for default_value, expected in defaults:
            with self.subTest(default_value=default_value):
                field = Field('name', default=default_value)

                field.table = self.create_table()
                field.table.backend = self.create_connection()

                params = field.field_parameters()
                self.assertEqual(
                    params,
                    expected
                )

    def test_validate_field_name(self):
        pass

    def test_create(self):
        pass

    def test_run_validators(self):
        field = Field('name')

        def validate_name(name):
            if name == 'Kendall':
                raise ValidationError('Invalid')

        field.base_validators = [validate_name]
        with self.assertRaises(ValidationError):
            field.run_validators('Kendall')

    def test_constraints(self):
        field = Field('name', max_length=20)
        self.assertEqual(len(field.constraints), 1)

    def test_to_database(self):
        field = Field('name')
        self.assertEqual(field.to_database('Kendall Jenner'), 'Kendall Jenner')

        value = field.to_database(lambda: 'Kendall Jenner')
        self.assertEqual(value, 'Kendall Jenner')

        value = field.to_database(25)
        self.assertEqual(value, 25)

        value = field.to_database([1, 2, 3])
        self.assertListEqual(value, [1, 2, 3])

    def test_base_field_parameters_boolean(self):
        field = Field('name', null=True, primary_key=True, unique=True)
        result = field.field_parameters()

        self.assertDictEqual(
            field.base_field_parameters,
            {
                'primary key': True,
                'not null': False,
                'unique': True
            }
        )

        self.assertListEqual(
            result,
            ['name', 'text', 'primary key', 'unique']
        )

    def test_field_parameters_with_max_length(self):
        table = self.create_table()
        table.backend = self.create_connection()

        field = Field('name', max_length=100)
        field.table = table
        result = field.field_parameters()
        self.assertListEqual(
            result,
            ['name', 'varchar(100)', 'not null', 'check(length(name)>100)']
        )

    def test_to_database(self):
        field = Field('name')

        values = [
            ('Kendall', 'Kendall'),
            ('1', '1')
        ]
        for value, expected in values:
            with self.subTest(value=value):
                result = field.to_database(value)
                self.assertEqual(result, expected)

    def test_field_parameters_dictionnary(self):
        f1 = Field('name')
        self.assertDictEqual(
            f1.base_field_parameters,
            {
                'primary key': False,
                'not null': True,
                'unique': False
            }
        )

        f2 = Field('name', null=True, unique=True)
        f2.field_parameters()

        self.assertDictEqual(
            f2.base_field_parameters,
            {
                'not null': True,
                'primary key': False,
                'unique': True
            }
        )


class TestCharField(LorelieTestCase):
    def test_structure(self):
        f = CharField('name')
        self.assertEqual(f.to_database('Kendall Jenner'), 'Kendall Jenner')

    def test_invalid_values(self):
        f = CharField('name')
        self.assertEqual(f.to_database(1), '1')
        self.assertEqual(f.to_database(1.0), '1.0')
        self.assertEqual(f.to_database(lambda: {'a': 1}), "{'a': 1}")
        self.assertEqual(f.to_database(lambda: 1), '1')


class TestIntegerField(LorelieTestCase):
    def test_structure(self):
        f = IntegerField('age')
        self.assertEqual(f.to_database(1), 1)

    def test_invalid_values(self):
        f = IntegerField('age')
        self.assertEqual(f.to_database(1), 1)
        self.assertEqual(f.to_database(1.0), 1)

        with self.assertRaises(TypeError):
            f.to_database(lambda: {'a': 1})


class TestFloatField(LorelieTestCase):
    def test_structure(self):
        f = FloatField('followers')
        self.assertEqual(f.to_database(1.0), 1.0)

    def test_invalid_values(self):
        f = FloatField('followers')
        self.assertEqual(f.to_database(1.0), 1.0)

        with self.assertRaises(TypeError):
            f.to_database(1)

        with self.assertRaises(TypeError):
            f.to_database(lambda: {'a': 1})


class TestJsonField(LorelieTestCase):
    def test_structure(self):
        f = JSONField('followers')
        self.assertEqual(f.to_database({'a': 1}), '{"a": 1}')

    def test_invalid_values(self):
        f = JSONField('followers')
        self.assertEqual(f.to_database(['a', 1]), '["a", 1]')

        with self.assertRaises(TypeError):
            f.to_database(1)


class TestBooleanField(LorelieTestCase):
    def test_structure(self):
        f = BooleanField('is_active')
        self.assertEqual(f.to_database(False), 0)

    def test_invalid_values(self):
        f = BooleanField('is_active')
        self.assertEqual(f.to_database(True), 1)
        self.assertEqual(f.to_database(False), 0)

        self.assertEqual(f.to_database(1), 1)
        self.assertEqual(f.to_database(0), 0)

        self.assertEqual(f.to_database('1'), 1)
        self.assertEqual(f.to_database('0'), 0)

        self.assertEqual(f.to_database('t'), 1)
        self.assertEqual(f.to_database('f'), 0)

        with self.assertRaises(TypeError):
            f.to_database('Kendall')


class TestDateField(LorelieTestCase):
    def test_structure(self):
        f = DateField('created_on')
        d = datetime.datetime.now()
        self.assertEqual(f.to_database(d), str(d.date()))
        self.assertEqual(f.to_database(str(d)), str(d.date()))

    def test_invalid_values(self):
        f = DateField('created_on')
        self.assertEqual(f.to_database(1), '')


class TestDateTimeField(LorelieTestCase):
    def test_structure(self):
        f = DateTimeField('created_on')
        d = datetime.datetime.now()
        self.assertEqual(f.to_database(d), str(d))
        self.assertEqual(f.to_database(str(d)), str(d))

    def test_invalid_values(self):
        f = DateTimeField('created_on')
        self.assertEqual(f.to_database(1), '')


class TestEmailField(LorelieTestCase):
    pass


class TestCommaSeparatedField(LorelieTestCase):
    pass


class TestTimeField(LorelieTestCase):
    pass


class TestSlugField(LorelieTestCase):
    pass


class TestUUIDField(LorelieTestCase):
    def test_structure(self):
        f = UUIDField('product_id')
        value = f.to_database(uuid.uuid4())
        self.assertIsInstance(value, str)
        revert_value = f.to_python(value)
        self.assertIsInstance(revert_value, uuid.UUID)


class TestAutoField(LorelieTestCase):
    def setUp(self):
        self.field = AutoField()

    def test_result(self):
        params = self.field.field_parameters()
        self.assertListEqual(
            params,
            ['id', 'integer', 'primary key', 'autoincrement']
        )

    def test_deconstruct(self):
        field_type, name, params = self.field.deconstruct()
