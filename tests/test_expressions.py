# import unittest

# from lorelie.backends import SQLiteBackend
# from lorelie.expressions import Case, CombinedExpression, F, Q, Value, When

# # Invalid usage
# # db.objects.annotate('products', F('price'))
# # db.objects.annotate('products', F('price') + F('price'))
# # db.objects.annotate('products', Value(1))
# # db.objects.annotate('products', Value(F('unit_price')))
# # db.objects.annotate('products', Q(price__gt=1))
# # db.objects.filter('products', price=F('name') + 'a')
# # a = Value(Q(firstname='Kendall'))

# # Valid usage
# # db.objects.filter('products', price=Value('price'))
# # db.objects.annotate('products', my_price=F('price'))
# # db.objects.annotate('products', my_price=F('price'))
# # db.objects.annotate('products', my_price=F('price') + F('price'))
# # db.objects.annotate('products', my_price=F('price') + F('price') - 1)
# # db.objects.annotate('products', my_price=F('price') + 1)
# # db.objects.annotate('products', my_price=Value(1))
# # db.objects.annotate('products', my_price=Q(price__gt=1))
# # db.objects.annotate(nameb=Value(F('name') + 'a', output_field=CharField()))

# # case = Case(When('price__eq=10', then_case=1), default=30)
# # db.objects.annotate('products', my_price=case)

# # db.objects.annotate('products', Count('price'))
# # db.objects.annotate('products', count_price=Count('price'))
# # db.objects.annotate('products', Count('price'), Count('unit_price'))


# class TestQ(unittest.TestCase):
#     def create_backend(self):
#         return SQLiteBackend()

#     def test_structure(self):
#         instance = Q(firstname='Kendall')
#         sql = instance.as_sql(self.create_backend())
#         self.assertIsInstance(sql, list)
#         self.assertListEqual(sql, ["firstname='Kendall'"])
#         self.assertRegex(
#             sql[0],
#             r"^firstname\=\'Kendall\'$"
#         )

#     def test_and(self):
#         a = Q(firstname='Kendall')
#         b = Q(firstname='Kylie')
#         c = a & b
#         self.assertIsInstance(c, CombinedExpression)
#         sql = c.as_sql(self.create_backend())
#         self.assertIsInstance(sql, list)
#         self.assertListEqual(
#             sql,
#             ["(firstname='Kendall' and firstname='Kylie')"]
#         )

#         self.assertRegex(
#             sql[0],
#             r"^\(firstname\=\'Kendall'\sand\sfirstname\=\'Kylie\'\)$"
#         )

#     def test_or(self):
#         a = Q(firstname='Kendall')
#         b = Q(firstname='Kylie')
#         c = a | b
#         self.assertIsInstance(c, CombinedExpression)
#         sql = c.as_sql(self.create_backend())
#         self.assertIsInstance(sql, list)
#         self.assertListEqual(
#             sql,
#             ["(firstname='Kendall' or firstname='Kylie')"]
#         )

#     def test_multiple_filters(self):
#         logic = Q(firstname='Kendall', age__gt=20, age__lte=50)
#         result = logic.as_sql(self.create_backend())
#         self.assertListEqual(
#             result,
#             ["firstname='Kendall' and age>20 and age<=50"]
#         )

#     def test_multioperators(self):
#         multi = (
#             Q(firstname='Kendall') |
#             Q(lastname='Jenner') &
#             Q(age__gt=25, age__lte=56)
#         )
#         result = multi.as_sql(self.create_backend())
#         self.assertListEqual(
#             result,
#             ["(firstname='Kendall' or (lastname='Jenner' and age>25 and age<=56))"]
#         )


# class TestCombinedExpression(unittest.TestCase):
#     def create_backend(self):
#         return SQLiteBackend()

#     def test_structure(self):
#         a = Q(firstname='Kendall')
#         b = Q(firstname='Kylie')

#         instance = CombinedExpression(a, b)
#         instance.build_children()

#         self.assertTrue(len(instance.children) == 3)
#         self.assertIsInstance(instance.children[0], Q)
#         self.assertIsInstance(instance.children[1], str)
#         self.assertTrue(instance.children[1] == 'and')
#         self.assertIsInstance(instance.children[-1], Q)

#         self.assertListEqual(
#             instance.as_sql(self.create_backend()),
#             ["(firstname='Kendall' and firstname='Kylie')"]
#         )


# class TestWhen(unittest.TestCase):
#     def create_backend(self):
#         return SQLiteBackend()

#     def test_structure(self):
#         instance = When('firstname=Kendall', 'kendall')
#         sql = instance.as_sql(self.create_backend())
#         self.assertRegex(
#             sql,
#             r"^when\sfirstname\=\'Kendall\'\sthen\s\'kendall\'$"
#         )


# class TestCase(unittest.TestCase):
#     def create_backend(self):
#         return SQLiteBackend()

#     @unittest.expectedFailure
#     def test_no_alias_name(self):
#         condition = When('firstname=Kendall', 'kendall')
#         case = Case(condition)
#         case.as_sql(self.create_backend())

#     def test_structure(self):
#         condition = When('firstname=Kendall', 'Kylie')
#         case = Case(condition, default='Aurélie')
#         case.alias_field_name = 'firstname_alias'

#         self.assertEqual(
#             case.as_sql(self.create_backend()),
#             "case when firstname='Kendall' then 'Kylie' else 'Aurélie' end firstname_alias"
#         )


# class TestF(unittest.TestCase):
#     def create_backend(self):
#         return SQLiteBackend()

#     def test_structure(self):
#         result = F('age') + F('age')
#         self.assertIsInstance(result, CombinedExpression)

#         sql = result.as_sql(self.create_backend())
#         self.assertEqual(
#             sql,
#             ['(age + age)']
#         )

#         result = F('age') + F('age') - 1
#         sql = result.as_sql(self.create_backend())
#         self.assertEqual(
#             sql,
#             ['(age + age) - 1']
#         )

#         result = F('age') - F('age')
#         sql = result.as_sql(self.create_backend())
#         self.assertEqual(
#             sql,
#             ['(age - age)']
#         )


# class TestValue(unittest.TestCase):
#     def create_backend(self):
#         return SQLiteBackend()

#     def test_structure(self):
#         a = Value(1)
#         print(a.as_sql(self.create_backend()))

#         # a = Value(Q(firstname='Kendall'))
#         # print(a.as_sql(backend))


# if __name__ == '__main__':
#     unittest.main()
