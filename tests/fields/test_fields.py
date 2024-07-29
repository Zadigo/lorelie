import datetime

from lorelie.exceptions import ValidationError
from lorelie.fields.base import (BooleanField, CharField, DateField,
                                 DateTimeField, Field, FloatField,
                                 IntegerField, JSONField)
from lorelie.test.testcases import LorelieTestCase


class TestField(LorelieTestCase):
    def test_structure(self):
        field = Field('name')
        params = field.field_parameters()
        self.assertEqual(params, ['name', 'text', 'not null'])
        self.assertEqual(field.to_python('Kendall Jenner'), 'Kendall Jenner')
        self.assertEqual(field.to_database('Kendall Jenner'), 'Kendall Jenner')

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


# class TestField(unittest.TestCase):
#     def test_field_params(self):
#         field = Field('name')
#         field.prepare(table)

#         # The default composition for the parameters
#         # of a field should be the following:
#         result = field.field_parameters()
#         self.assertListEqual(result, ['name', 'text', 'not null'])

#         field.null = True
#         field.default = 'Kendall'
#         result = field.field_parameters()
#         self.assertListEqual(
#             result,
#             ['name', 'text', 'default', "'Kendall'", 'null']
#         )

#         field.unique = True
#         result = field.field_parameters()
#         self.assertListEqual(
#             result,
#             ['name', 'text', 'default', "'Kendall'", 'null', 'unique']
#         )

#         field.max_length = 100
#         result = field.field_parameters()
#         self.assertListEqual(
#             result,
#             ['name', 'varchar(100)', 'default', "'Kendall'", 'null', 'unique']
#         )

#     def test_to_database(self):
#         field = Field('name')
#         field.prepare(table)

#         result = field.to_database('Kendall')
#         self.assertEqual(result, 'Kendall')

#     def test_desconstruction(self):
#         field = Field('name')
#         name, parameters = field.deconstruct()
#         self.assertIsInstance(name, str)
#         self.assertIsInstance(parameters, list)
#         self.assertEqual(name, 'name')
#         self.assertIn('name', parameters)


# class TestCharField(unittest.TestCase):
#     def test_result(self):
#         field = CharField('firstname')
#         field.prepare(table)

#         params = field.field_parameters()
#         self.assertListEqual(
#             params,
#             ['firstname', 'text', 'not null']
#         )

#         result = field.to_database({'a': 1})
#         self.assertEqual(result, "{'a': 1}")
#         result = field.to_database(['1'])
#         self.assertEqual(result, "['1']")
#         result = field.to_database('1')
#         self.assertEqual(result, '1')
#         result = field.to_database(1)
#         self.assertEqual(result, '1')


# class TestIntegerField(unittest.TestCase):
#     def test_result(self):
#         field = IntegerField('age')
#         field.prepare(table)

#         params = field.field_parameters()
#         self.assertListEqual(
#             params,
#             ['age', 'integer', 'not null']
#         )

#         result = field.to_database(1)

#         self.assertIsInstance(result, int)
#         self.assertEqual(result, 1)


# class TestBooleanField(unittest.TestCase):
#     def test_result(self):
#         field = BooleanField('completed')
#         field.prepare(table)

#         params = field.field_parameters()
#         self.assertListEqual(
#             params,
#             ['completed', 'text', 'not null']
#         )

#         result = field.to_database(0)
#         self.assertEqual(result, 0)

#         result = field.to_database(True)
#         self.assertEqual(result, 1)

#         result = field.to_database('true')
#         self.assertEqual(result, 1)


# class TestAutoField(unittest.TestCase):
#     def test_result(self):
#         field = AutoField()
#         field.prepare(table)

#         name, params = field.deconstruct()
#         self.assertListEqual(
#             params,
#             ['id', 'integer', 'primary key', 'autoincrement', 'not null']
#         )


# class TestJsonField(unittest.TestCase):
#     def test_result(self):
#         field = JSONField('metadata')
#         field.prepare(table)

#         result = field.to_python('{"a": 1}')
#         self.assertDictEqual(result, {"a": 1})

#         result = field.to_database({'a': 1})
#         self.assertEqual(result, '{"a": 1}')


# class TestDateField(unittest.TestCase):
#     def test_result(self):
#         field = DateField('created_on')
#         field.prepare(table)

#         expected = datetime.datetime.now().date()
#         result = field.to_python(expected)
#         self.assertEqual(result, expected)


#         result = field.to_database(str(expected))
#         self.assertEqual(result, str(expected))


# class TestDateTimeField(unittest.TestCase):
#     def test_result(self):
#         field = DateTimeField('created_on')
#         field.prepare(table)

#         expected = datetime.datetime.now(tz=pytz.UTC)
#         result = field.to_python(expected)
#         self.assertEqual(result, expected)

#         result = field.to_database(str(expected))
#         self.assertEqual(result, str(expected))
