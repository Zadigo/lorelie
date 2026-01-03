import dataclasses
from lorelie.database.expressions.mixins import SQL
from lorelie.test.testcases import LorelieTestCase
from lorelie.database.functions.text import Lower


class TestSQL(LorelieTestCase):
    def setUp(self):
        instance = SQL()
        instance.current_table = self.create_table()
        instance.current_table.backend = self.create_connection()
        self.instance = instance

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
        for value, expected in arguments:
            with self.subTest(value=value):
                result = self.instance.build_annotation(value)
                self.assertTrue(dataclasses.is_dataclass(result))

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

    def test_decompose_filters_from_string(self):
        conditions = [
            ('name__eq=Kendall', [('name', '=', 'Kendall')])
        ]
        for condition in conditions:
            with self.subTest(condition=condition):
                lhv, expected = condition
                result = self.instance.decompose_filters_from_string(lhv)
                self.assertListEqual(result, expected)
