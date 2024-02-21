import unittest

from lorelie.backends import SQL
from lorelie.functions import (Count, ExtractYear, Length, Lower, Max, Min,
                               Upper)
from tests.items import FakeTable


class TestSQL(unittest.TestCase):
    def setUp(self):
        self.instance = SQL()

    def test_quote_value(self):
        values = ['name', 'surname', 'key intelligence']
        for value in values:
            with self.subTest(value=value):
                result = self.instance.quote_value(value)
                self.assertTrue(result.startswith("'"))
                self.assertTrue(result.endswith("'"))
        self.assertTrue(isinstance(self.instance.quote_value(1), int))

    def test_comma_join(self):
        values = ['a', 'b', 'c']
        self.assertTrue(self.instance.comma_join(values) == 'a, b, c')

    def test_operator_join(self):
        expected_result = "name = 'Kendall' and surname = 'Jenner'"
        decomposed_condition = self.instance.decompose_filters(
            name__eq='Kendall',
            surname__eq='Jenner'
        )
        built_filters = self.instance.build_filters(decomposed_condition)
        final_result = self.instance.operator_join(built_filters)
        self.assertTrue(final_result == expected_result)

    def test_simple_join(self):
        values = ['a', 'b', 'c']
        self.assertTrue(self.instance.simple_join(values) == 'a b c')

    def test_finalize_sql(self):
        self.assertTrue(self.instance.finalize_sql('a').endswith(';'))

    def test_de_sqlize_statement(self):
        result = self.instance.de_sqlize_statement('a;')
        self.assertTrue(not result.endswith(';'))

    def test_quote_wildcard(self):
        self.assertTrue(self.instance.quote_startswith('name') == "'name%'")
        self.assertTrue(self.instance.quote_endswith('name') == "'%name'")
        self.assertTrue(self.instance.quote_like('name') == "'%name%'")

    def test_build_script(self):
        values = ['a', 'b']
        result = self.instance.build_script(*values)
        self.assertTrue(result, """a;\nb;""")

    def test_decompose_filters(self):
        arguments = [
            [{'name__eq': 'Kendall'}, [('name', '=', 'Kendall')]],
            [{'name__lt': 'Kendall'}, [('name', '<', 'Kendall')]],
            [{'name__gt': 'Kendall'}, [('name', '>', 'Kendall')]],
            [{'name__lte': 'Kendall'}, [('name', '<=', 'Kendall')]],
            [{'name__gte': 'Kendall'}, [('name', '>=', 'Kendall')]],
            [{'name__contains': 'Kendall'}, [('name', 'like', 'Kendall')]],
            [{'name__startswith': 'Kendall'}, [
                ('name', 'startswith', 'Kendall')]],
            [{'name__endswith': 'Kendall'}, [('name', 'endswith', 'Kendall')]],
            [{'name__range': 'Kendall'}, [('name', 'between', 'Kendall')]],
            [{'name__ne': 'Kendall'}, [('name', '!=', 'Kendall')]],
            [{'name__in': 'Kendall'}, [('name', 'in', 'Kendall')]],
            [{'name__isnull': 'Kendall'}, [('name', 'isnull', 'Kendall')]]
        ]

        for argument in arguments:
            with self.subTest(argument=argument):
                result = self.instance.decompose_filters(**argument[0])
                self.assertListEqual(result, argument[1])

    def test_build_filters(self):
        # left: argument, right: expected sql
        arguments = [
            [[('name', '=', 'Kendall')], ["name = 'Kendall'"]],
            [[('name', '!=', 'Kendall')], ["name != 'Kendall'"]],
            [[('name', '<', '1')], ["name < '1'"]],
            [[('name', '>', '1')], ["name > '1'"]],
            [[('name', '<=', '1')], ["name <= '1'"]],
            [[('name', '>=', '1')], ["name >= '1'"]],
            [[('name', 'like', 'Kendall')], ["name like '%Kendall%'"]],
            [[('name', 'startswith', 'Kendall')], ["name like 'Kendall%'"]],
            [[('name', 'endswith', 'Kendall')], ["name like '%Kendall'"]],
            [[('name', 'between', [1, 2])], ["name between 1 and 2"]],
            [[('name', 'in',  ['Kendall', 'Kylie'])],
             ["name in ('Kendall', 'Kylie')"]]
            # TODO: Implement the isnull check
            # [[('name', 'isnull', 'Kendall')], ["name = '1'"]]
        ]

        for argument in arguments:
            with self.subTest(argument=argument):
                result = self.instance.build_filters(argument[0])
                self.assertListEqual(result, argument[1])

    def test_build_annotations(self):
        # left: arguments, right: expected sql
        arguments = [
            [{'name': Lower('name')}, ['lower(name) as name']]
        ]
        for argument in arguments:
            with self.subTest(argument=argument):
                result = self.instance.build_annotation(**argument[0])
                sql_functions_dict, special_function_fields, fields = result
                self.assertListEqual(fields, argument[1])
                self.assertIsInstance(sql_functions_dict, dict)
                self.assertIsInstance(special_function_fields, list)

        # The Count function should return the list of fields
        # that we will be using the group by
        result = self.instance.build_annotation(count_name=Count('name'))
        _, special_function_fields, _ = result
        self.assertListEqual(special_function_fields, ['name'])

    def test_dict_to_sql(self):
        conditions = [
            # expected: (['name__eq'], ["'Kendall'"])
            {'name__eq': 'Kendall'}
        ]
        for condition in conditions:
            with self.subTest(condition=condition):
                result = self.instance.dict_to_sql(condition)
                self.assertIsInstance(result, tuple)
                self.assertListEqual(result[0], ['name__eq'])
                self.assertTrue(result[1][0].startswith("'"))

    def test_filter_decomposition_process(self):
        conditions = {
            'name__eq': 'Kendall',
            'surname': 'Jenner'
        }
        decomposed_filters = self.instance.decompose_filters(**conditions)
        built_filters = self.instance.build_filters(decomposed_filters)
        statement = self.instance.operator_join(built_filters)
        expected_result = "name = 'Kendall' and surname = 'Jenner'"
        self.assertTrue(statement == expected_result)


class TestFunctionsSQL(unittest.TestCase):
    def setUp(self):
        instance = SQL()
        setattr(instance, 'table', FakeTable())
        self.sql_backend = instance
        # Just for the purpose of testing,
        # implement a FakeTable on the SQL
        # mixin class. Technically the table
        # would be on SQLiteBackend class

    def test_lower_sql(self):
        instance = Lower('name')
        instance.backend = self.sql_backend
        result = instance.function_sql()
        self.assertTrue(result == 'lower(name)')

    def test_upper_sql(self):
        instance = Upper('name')
        instance.backend = self.sql_backend
        result = instance.function_sql()
        self.assertTrue(result == 'upper(name)')

    def test_length_sql(self):
        instance = Length('name')
        instance.backend = self.sql_backend
        result = instance.function_sql()
        self.assertTrue(result == 'length(name)')

    def test_max_sql(self):
        instance = Max('id')
        instance.backend = self.sql_backend
        result = instance.function_sql()
        expected_result = 'select rowid, * from fake_table where id=(select max(id) from fake_table)'
        self.assertTrue(result == expected_result)

    def test_min_sql(self):
        instance = Min('id')
        instance.backend = self.sql_backend
        result = instance.function_sql()
        expected_result = 'select rowid, * from fake_table where id=(select min(id) from fake_table)'
        self.assertTrue(result == expected_result)

    def test_extract_year(self):
        instance = ExtractYear('date_of_birth')
        instance.backend = self.sql_backend
        result = instance.function_sql()
        expected_result = "strftime('%Y', date_of_birth)"
        self.assertTrue(result == expected_result)

    def test_count_sql(self):
        instance = Count('name')
        instance.backend = self.sql_backend
        result = instance.function_sql()
        self.assertTrue(result == 'count(name)')


if __name__ == '__main__':
    unittest.main()
