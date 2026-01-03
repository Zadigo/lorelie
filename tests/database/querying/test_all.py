import unittest

from lorelie.expressions import F, Q, Value
from lorelie.fields.base import CharField
from lorelie.queries import QuerySet
from lorelie.test.testcases import LorelieTestCase


class TestAll(LorelieTestCase):
    def setUp(self):
        self.db = db = self.create_database()
        self.table = table = db.get_table('celebrities')

        table.objects.create(name='Addison Rae', height=178)
        table.objects.create(name='Kendall Jenner', height=184)

    def test_structure(self):
        qs = self.table.objects.all()
        self.assertIsInstance(qs, QuerySet)
        self.assertTrue(len(qs) > 0)
        self.assertEqual(qs.count(), 2)

        for item in qs:
            self.assertTrue(item.height > 150)

    def test_can_edit_base_row(self):
        qs = self.db.celebrities.objects.all('celebrities')

        item = qs[0]
        self.assertTrue(item.name, 'Addison Rae')

        item['name'] = 'Addison Marie Rae'
        item.save()

# # Invalid usage
# # db.celebrities.objects.annotate('products', F('price'))
# # db.celebrities.objects.annotate('products', F('price') + F('price'))
# # db.celebrities.objects.annotate('products', Value(1))
# # db.celebrities.objects.annotate('products', Value(F('unit_price')))
# # db.celebrities.objects.annotate('products', Q(price__gt=1))
# # a = Value(Q(firstname='Kendall'))

# Valid usage
# db.celebrities.objects.filter('products', price=Value('price'))
# db.celebrities.objects.annotate('products', my_price=Value(1))
# db.celebrities.objects.annotate('products', my_price=F('price'))
# db.celebrities.objects.annotate('products', my_price=F('price') + F('price'))
# db.celebrities.objects.annotate('products', my_price=F('price') + F('price') - 1)
# db.celebrities.objects.annotate('products', my_price=F('price') + 1)
# # db.celebrities.objects.filter('products', price=F('name') + 'a')
# db.celebrities.objects.annotate('products', my_price=Q(price__gt=1))
# db.celebrities.objects.annotate(my_name=Value(F('name') + 'a', output_field=CharField()))

# Should raise error
# db.celebrities.objects.filter('products', price=Value('a') + Value('a'))
# db.celebrities.objects.filter('products', price=Value('a') + F('a'))

# # case = Case(When('price__eq=10', then_case=1), default=30)
# # db.celebrities.objects.annotate('products', my_price=case)

# # db.celebrities.objects.annotate('products', Count('price'))
# # db.celebrities.objects.annotate('products', count_price=Count('price'))
# # db.celebrities.objects.annotate('products', Count('price'), Count('unit_price'))


class TestAnnotate(LorelieTestCase):
    def setUp(self):
        self.db = self.create_database()
        self.table = table = self.db.get_table('celebrities')
        table.objects.create(name='Kendall Jenner')

    def test_annotation_with_value_function(self):
        result = self.table.objects.annotate(age=Value(22))
        self.assertEqual(result[0].age, 22)

        result = self.table.objects.annotate(description=Value('My text'))
        self.assertEqual(result[0].description, 'My text')

    def test_annotation_with_f_function(self):
        # The expected sqls in order of the
        # functions that were created below
        expected_sqls = [
            "select *, height as other from celebrities;",
            "select *, (height + height) as other from celebrities;",
            "select *, (height + height) - 1 as other from celebrities;",
            "select *, height + 1 as other from celebrities;"
        ]

        qs = self.table.objects.annotate(other=F('height'))
        self.assertEqual(qs[0].other, 152)
        self.assertIn(qs.sql_statement, expected_sqls)

        qs = self.table.objects.annotate(other=F('height') + F('height'))
        self.assertEqual(qs[0].other, 304)
        self.assertIn(qs.sql_statement, expected_sqls)

        result = self.table.objects.annotate(
            other=F('height') + F('height') - 1
        )
        self.assertEqual(result[0].other, 303)
        self.assertIn(qs.sql_statement, expected_sqls)

        result = self.table.objects.annotate(other=F('height') + 1)
        self.assertEqual(result[0].other, 153)
        self.assertIn(qs.sql_statement, expected_sqls)

        # FIXME: Using F + str returns 0
        # result = self.table.objects.annotate(other=F('name') + 'Price')
        # self.assertEqual(result[0].other, 'Kendall JennerPrice')

    def test_annotation_with_q_function(self):
        # The database returns either 0 or 1 as False or True
        qs = self.table.objects.annotate(other=Q(height__gt=150))
        self.assertEqual(qs[0].other, 1)
        self.assertEqual(
            qs.sql_statement,
            "select *, height>150 as other from celebrities;"
        )

    def test_annotation_with_mixed_field_types(self):
        # These kinds of mixed expressions should not be able
        # to be resolved and therefore be stringified
        combined = Value(F('name') + 'a', output_field=CharField)
        qs = self.table.objects.annotate(mixed=combined)
        self.assertEqual(
            qs.first().mixed,
            "<CombinedExpression: [F(name), '+', Value(a)]>"
        )

    def test_invalid_args(self):
        # Expressions are not made to be
        # used without an alias. Using them
        # in the args should raise a ValueError
        args = [
            F('height'),
            F('height') + F('height'),
            Q(height__gte=150)
        ]

        with self.assertRaises(ValueError):
            for arg in args:
                self.table.objects.annotate(arg)


class TestFilter(LorelieTestCase):
    pass
