import inspect
import datetime
import uuid

from lorelie.exceptions import ValidationError
from lorelie.fields.base import (AliasField, AutoField, BinaryField,
                                 BooleanField, CharField, CommaSeparatedField,
                                 DateField, DateTimeField, DecimalField,
                                 EmailField, Field, FilePathField, FloatField,
                                 IntegerField, JSONField, SlugField, TimeField,
                                 URLField, UUIDField)
from lorelie.test.testcases import LorelieTestCase


class TestField(LorelieTestCase):
    def test_structure(self):
        field = Field('name')
        params = field.field_parameters()

        self.assertEqual(params, ['name', 'text', 'not null'])
        self.assertEqual(field.to_python('Kendall Jenner'), 'Kendall Jenner')
        self.assertEqual(field.to_database('Kendall Jenner'), 'Kendall Jenner')

    def test_constraints(self):
        field = Field('name', max_length=20)
        self.assertEqual(len(field.constraints), 1)

    def test_base_field_parameters_boolean(self):
        field = Field('name', null=True, primary_key=True, unique=True)
        field.field_parameters()

        self.assertDictEqual(
            field.base_field_parameters,
            {
                'primary key': True,
                'null': True,
                'not null': False,
                'unique': True
            }
        )

    def test_validators(self):
        field = Field('name')

        def validate_name(name):
            if name == 'Kendall':
                raise ValidationError('Invalid')

        field.base_validators = [validate_name]
        with self.assertRaises(ValidationError):
            field.run_validators('Kendall')

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
                'null': False,
                'not null': True,
                'unique': False
            }
        )

        f2 = Field('name', null=True, unique=True)
        f2.field_parameters()
        self.assertDictEqual(
            f2.base_field_parameters,
            {
                'not null': False,
                'null': True,
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


# class TestAutoField(unittest.TestCase):
#     def test_result(self):
#         field = AutoField()
#         field.prepare(table)

#         name, params = field.deconstruct()
#         self.assertListEqual(
#             params,
#             ['id', 'integer', 'primary key', 'autoincrement', 'not null']
#         )
