import re
from typing import Any, Sequence
from functools import cached_property
import dataclasses
import re
from collections import defaultdict
from typing import Any, Sequence


from lorelie.database.nodes import (AnnotationMap)
from lorelie.lorelie_typings import (TypeFunction, TypeLogicalOperators)

from lorelie.lorelie_typings import OperatorType, TranslatedOperatorType, TypeDecomposedFilterTuple


BASE_FILTERS = {
    'eq': '=',
    'lt': '<',
    'gt': '>',
    'lte': '<=',
    'gte': '>=',
    'contains': 'like',
    'startswith': 'startswith',
    'endswith': 'endswith',
    'range': 'between',
    'ne': '!=',
    'in': 'in',
    'isnull': 'isnull',
    'regex': 'regexp',
    'day': '',
    'month': '',
    'iso_year': '',
    'year': '',
    'minute': '',
    'second': '',
    'hour': '',
    'time': ''
}

BASE_OPERATORS = ['<>', *list(BASE_FILTERS.values())]


class QuoteValueMixin:
    @staticmethod
    def quote_value(value: Any):
        """Quote a given value to be used in an SQL statement.
        Numbers are not quoted. None is converted to ''."""
        if value is None:
            return "''"

        if callable(value):
            value = value()

        if isinstance(value, (list, tuple)):
            value = str(value)

        if isinstance(value, (int, float)):
            return value

        if isinstance(value, str) and value.startswith("'"):
            return value

        # To handle special characters like
        # single quotes ('), we have to escape
        # them by doubling them up for the final
        # sql string
        if isinstance(value, str) and "'" in value:
            value = value.replace("'", "''")

        # By default, quote the value
        return f"'{value}'"


class ExpressionFiltersMixin(QuoteValueMixin):
    """This mixin class is used to decompose filter
    expressions used for example in query methods
    e.g. filter(age=1) and can be implemented in 
    any class that requires the analysis and decomposition
    of these kinds of values

    """
    base_filters = BASE_FILTERS

    @cached_property
    def list_of_operators(self) -> list[TranslatedOperatorType]:
        operators = list(self.base_filters.values())
        operators.append('<>')
        return operators

    def is_query_filter(self, value_or_values: OperatorType | list[OperatorType | Any]) -> bool:
        """Checks that the last value or that a single
        value is a query filtering element. 

        For example `eq` in `['name', 'eq']`

        >>> self.is_query_filter(['name', 'eq'])
        ... True

        >>> self.is_query_filter('eq')
        ... True
        """
        if isinstance(value_or_values, list):
            value_or_values = value_or_values[-1]
        return value_or_values in list(self.base_filters.keys())

    def translate_operator_from_tokens(self, tokens: list[OperatorType | Any]):
        """Translates a string filter in a list of tokens
        to a valid mathematical operator

        >>> tokens = ['age', 'eq', 1] 
        ... self.translate_operator_from_tokens(tokens)
        ... ['age', '=', 1]
        """
        translated: list[str | TranslatedOperatorType] = []
        for item in tokens:
            if self.is_query_filter(item):
                translated.append(self.base_filters[item])
                continue

            translated.append(item)
        return translated

    def translate_operators_from_tokens(self, tokens: Sequence[list[str | OperatorType | Any]]):
        """Translates a string filter in a list of tokens
        to a valid mathematical operator

        >>> tokens = [['age', 'eq', 1]]
        ... self.translate_operators_from_tokens(tokens)
        ... ['name', '=', 1]
        """
        translated: list[list[str | TranslatedOperatorType | Any]] = []
        for item in tokens:
            if not isinstance(item, (list, tuple)):
                raise ValueError(f"{item} should be of type list/tuple")
            translated.append(self.translate_operator_from_tokens(item))
        return translated

    def decompose_filters_columns(self, value: str | dict) -> list[str]:
        """Return only the column parts of a filter element.

        For example returning `['rowid']` from `rowid__eq=1` or `{'rowid__eq': 1}`

        >>> example_filter = 'rowid__eq=1'
        ... self.decompose_filters_from_string(example_filter)
        ... ['rowid']
        """
        if isinstance(value, str):
            result = self.decompose_filters_from_string(value)
        elif isinstance(value, dict):
            result = self.decompose_filters(**value)
        return list(map(lambda x: x[0], result))

    def decompose_filters_from_string(self, value: str):
        """Decompose a set of filters to a list of
        key, operator and value list from a filter 
        passed as a string

        >>> element = 'rowid__eq=1'
        ... self.decompose_filters_from_string(element)
        ... [('rowid', '=', '1')]
        """
        identified_operator = False
        operators = self.base_filters.keys()
        for operator in operators:
            operator_to_identify = f'__{operator}='
            if operator_to_identify in value:
                identified_operator = True
                break

        if not identified_operator:
            # Use regex to identify the unique
            # possible existing pattern if the
            # user does not use a "__" filter
            result = re.match(r'\w+\=', value)
            if not result:
                raise ValueError(
                    "Could not identify the operator "
                    "to use for the provided filter"
                )

        tokens = value.split('=')
        return self.decompose_filters(**{tokens[0]: tokens[1]})

    def decompose_filters(self, **kwargs: Any) -> list[TypeDecomposedFilterTuple]:
        """Decompose a set of filters to a list of
        key, operator and value list from a dictionnary

        >>> element = {'id__eq': 1} 
        ... self.decompose_filters(**element)
        ... [('id', '=', '1')]

        >>> element = {'followers__users__id__eq': 1}
        ... self.decompose_filters(**element)
        ... [('followers', 'users', 'id', '=', 1)]
        """
        filters_map = []
        for key, value in kwargs.items():
            tokens = key.split('__')
            if not self.is_query_filter(tokens):
                # Case for rowid=1 which should
                # be by default: rowid__eq
                tokens.append('eq')

            if len(tokens) == 2:
                # Normal sequence: rowid__eq
                lhv, rhv = tokens

                operator = self.base_filters.get(rhv)
                if operator is None:
                    raise ValueError(
                        f'Operator is not recognized. Got: {key}'
                    )
                filters_map.append((lhv, operator, value))
            elif len(tokens) > 2:
                # Foreign key sequence:
                # foreignkeyfield__field__eq=1
                # foreignkeyfield1__foreignkeyfield2__field__eq=1
                rebuilt_tokens = []
                for token in tokens:
                    if self.is_query_filter(token):
                        operator = self.base_filters.get(token)
                        rebuilt_tokens.append(operator)
                        continue
                    rebuilt_tokens.append(token)
                rebuilt_tokens.append(value)
                filters_map.append(tuple(rebuilt_tokens))
        return filters_map

    def build_filters(self, items: list[tuple[str | TranslatedOperatorType]], space_characters: bool = True) -> list[str]:
        """Tranform a list of decomposed filters to
        a usable sql-condition for an sql statement

        Simple condition:

        >>> element = [('rowid', '=', '1')]
        ... self.build_filters(element)
        ... ["rowid = '1'"]

        Condition with IN operator:

        >>> element = [('rowid', 'startswith', '1')]
        ... self.build_filters(element)
        ... ["rowid like '1%'"]

        Condition with LOWER function (or functions in general):

        >>> element = [('url', '=', Lower('url'))]
        ... self.build_filters(element)
        ... ["lower(url)"]
        """
        built_filters = []
        for item in items:
            field, operator, value = item

            # TODO: Implement a check tha raises an
            # error if the operator is not a valid
            # existing one aka: =,!=, <,>,<=,>=,in,
            # endswith, startswith, between, isnull, like

            if operator == 'in':
                # TODO: Allow the user to check that a
                # value would also exist in a queryset or
                # an iterable in general
                if not isinstance(value, (tuple, list)):
                    raise ValueError(
                        'The value when using "in" '
                        f'should be a tuple or a list. Got: {value}'
                    )

                quoted_list_values = (self.quote_value(item) for item in value)
                operator_and_value = self.IN.format_map({
                    'field': field,
                    'values': self.comma_join(quoted_list_values)
                })
                built_filters.append(operator_and_value)
                continue

            if operator == 'like':
                operator_and_value = self.LIKE.format_map({
                    'field': field,
                    'conditions': self.quote_like(value)
                })
                built_filters.append(operator_and_value)
                continue

            if operator == 'startswith':
                operator_and_value = self.LIKE.format_map({
                    'field': field,
                    'conditions': self.quote_startswith(value)
                })
                built_filters.append(operator_and_value)
                continue

            if operator == 'endswith':
                operator_and_value = self.LIKE.format_map({
                    'field': field,
                    'conditions': self.quote_endswith(value)
                })
                built_filters.append(operator_and_value)
                continue

            if operator == 'between':
                if not isinstance(value, (list, tuple)):
                    raise ValueError(
                        'The value when using "between" '
                        f'should be a tuple or a list. Got: {value}'
                    )

                lhv, rhv = value
                operator_and_value = self.BETWEEN.format_map({
                    'field': field,
                    'lhv': lhv,
                    'rhv': rhv
                })
                built_filters.append(operator_and_value)
                continue

            if operator == 'isnull':
                if value:
                    operator_and_value = f'{field} is null'
                else:
                    operator_and_value = f'{field} is not null'

                built_filters.append(operator_and_value)
                continue

            value = self.quote_value(value)
            built_filters.append(
                self.simple_join(
                    (field, operator, value),
                    space_characters=space_characters
                )
            )

        return built_filters


class SQL(ExpressionFiltersMixin):
    """Base SQL compiler"""

    ALTER_TABLE = 'alter table {table} add column {params}'
    CREATE_TABLE = 'create table if not exists {table} ({fields})'
    CREATE_INDEX = 'create index {name} on {table} ({fields})'
    DROP_TABLE = 'drop table if exists {table}'
    DROP_INDEX = 'drop index if exists {value}'
    DELETE = 'delete from {table}'
    INSERT = 'insert into {table} ({fields}) values({values})'
    SELECT = 'select {fields} from {table}'
    UPDATE = 'update {table}'
    UPDATE_SET = 'set {params}'
    REPLACE = 'replace into {table} ({fields}) values({values})'

    AND = 'and {rhv}'
    OR = 'or {rhv}'

    CONDITION = '{field}{operator}{value}'
    EQUALITY = '{field}={value}'
    LIKE = '{field} like {conditions}'
    BETWEEN = '{field} between {lhv} and {rhv}'
    IN = '{field} in ({values})'
    NOT_LIKE = '{field} not like {wildcard}'
    WHERE_CLAUSE = 'where {params}'
    WHERE_NOT = 'where not ({params})'

    WILDCARD_MULTIPLE = '%'
    WILDCARD_SINGLE = '_'

    ASCENDING = '{field} asc'
    DESCENDING = '{field} desc'

    ORDER_BY = 'order by {conditions}'
    GROUP_BY = 'group by {conditions}'

    AVERAGE = 'avg({field})'
    COUNT = 'count({field})'
    STRFTIME = 'strftime({format}, {value})'

    CHECK_CONSTRAINT = 'check({conditions})'

    CASE = 'case {field} {conditions} end {alias}'
    WHEN = 'when {condition} then {value}'

    LIMIT = 'limit {value}'
    OFFSET = 'offset {value}'

    SQL_REGEXES = [
        re.compile(r'^select\s(.*)\sfrom\s(.*)\s(where)?\s(.*);?$')
    ]

    @staticmethod
    def comma_join(values: list[Any]):
        def check_value_type(value):
            if callable(value):
                return str(value())

            if isinstance(value, (int, float, list, tuple)):
                return str(value)
            return value

        return ', '.join(map(check_value_type, values))

    @staticmethod
    def operator_join(values: list[str], operator: TypeLogicalOperators = 'and'):
        """Joins a set of values using a valid
        operator: and, or

        >>> self.operator_join(["name='Kendall'", "surname='Jenner'"])
        ... "name='Kendall' and surname='Jenner'"
        """
        return f' {operator} '.join(values)

    @staticmethod
    def simple_join(values: list[str | Any], space_characters: bool = True):
        """Joins a set of tokens with a simple space

        >>> self.simple_join(["select * from table", "where name = 'Kendall'"])
        ... "select * from table where name = 'Kendall'"
        """
        def check_integers(value):
            if isinstance(value, (int, float)):
                return str(value)

            if getattr(value, 'to_python', None) is not None:
                return value.to_python()
            return value
        values = map(check_integers, values)

        if space_characters:
            return ' '.join(values)
        return ''.join(values)

    @staticmethod
    def finalize_sql(sql: str):
        """Ensures that a statement ends
        with `;` to be considered valid"""
        if sql.endswith(';'):
            return sql
        return f'{sql};'

    @staticmethod
    def de_sqlize_statement(sql: str):
        """Returns an sql statement without 
        `;` at the end"""
        if sql.endswith(';'):
            return sql.removesuffix(';')
        return sql

    @staticmethod
    def wrap_parenthentis(value: str):
        """Wraps a given value in parenthentis"""
        return f"({value})"

    @staticmethod
    def build_alias(condition: str, alias: str):
        """Returns the alias statement for a given sql
        statement like in `count(name) as top_names`"""
        return f'{condition} as {alias}'

    def build_dot_notation(self, values: Sequence[Sequence[str]]):
        """Transforms a set of values to a valid
        SQL dot notation

        >>> self.build_dot_notation([('followers', 'id', '=', '1')])
        ... 'followers.id = 1'
        """
        notations = []
        for sub_items in values:
            if not isinstance(sub_items, (list, tuple)):
                raise ValueError(
                    f"Expected list or array. Got: {sub_items}"
                )

            # The tuple needs exactly four
            # elements in order to create a
            # valid notation: el1.el2=el4
            # the operator is el3
            if len(sub_items) != 4:
                raise ValueError(
                    f"Invalid number of items in '{sub_items}' "
                    "in order to create a valid dot notation"
                )

            dot_notation = []
            for i, sub_value in enumerate(sub_items):
                # As soon as we get the operator, stop the
                # dotting. Get the remaing items from the
                # array that were not parsed starting from
                # the iteration index i
                if sub_value in list(self.base_filters.values()):
                    remaining_bits = list(sub_items[i:])
                    remaining_bits[-1] = self.quote_value(sub_items[-1])
                    dot_notation.extend(remaining_bits)
                    break

                if i > 0:
                    dot_notation.append('.')
                dot_notation.append(sub_value)

            final_notation = self.simple_join(
                dot_notation,
                space_characters=False
            )
            notations.append(final_notation)
        return notations

    def parameter_join(self, data: dict[str, Any]):
        """Takes a list of fields and values
        and returns string of key/value parameters
        ready to be used in an sql statement

        >>> self.parameter_join(['firstname', 'lastname'], ['Kendall', 'Jenner'])
        ... "firstname='Kendall', lastname='Jenner'"
        """
        fields, values = self.dict_to_sql(data)
        result = []
        for i, field in enumerate(fields):
            equality = self.EQUALITY.format(field=field, value=values[i])
            result.append(equality)
        return self.comma_join(result)

    def quote_values(self, values: list[Any]):
        """Quotes multiple values at once"""
        return list(map(lambda x: self.quote_value(x), values))

    def quote_startswith(self, value: str):
        """Creates a startswith wildcard and returns
        the quoted condition

        >>> self.quote_startswith(self, 'kendall')
        ... "'kendall%'"
        """
        value = str(value) + '%'
        return self.quote_value(value)

    def quote_endswith(self, value: str):
        """Creates a endswith wildcard and returns
        the quoted condition

        >>> self.quote_endswith(self, 'kendall')
        ... "'%kendall'"
        """
        value = '%' + str(value)
        return self.quote_value(value)

    def quote_like(self, value: str):
        """Creates a like wildcard and returns
        the quoted condition

        >>> self.quote_like(self, 'kendall')
        ... "'%kendall%'"
        """
        value = f'%{value}%'
        return self.quote_value(value)

    def dict_to_sql(self, data: dict[str, Any], quote_values: bool = True):
        """Convert a dictionnary containing a key
        pair of columns and values into a tuple
        of columns and value list. The values from
        the dictionnary are quoted by default

        >>> self.dict_to_sql({'name': 'Kendall'})
        ... (['name'], ["'Kendall'"])
        """
        fields = list(data.keys())
        if quote_values:
            quoted_value = list(
                map(
                    lambda x: self.quote_value(x),
                    data.values()
                )
            )
            return fields, quoted_value
        return fields, data.values()

    def build_script(self, *sqls: str):
        return '\n'.join(map(lambda x: self.finalize_sql(x), sqls))

    def build_annotation(self, conditions: dict[str, TypeFunction]):
        """For each database function, creates a special
        statement as in `count(column_name) as my_special_name`.
        If we have Count function in our conditions, we have to
        identify it since it requires a special """
        annotation_map = AnnotationMap()

        for alias, func in conditions.items():
            annotation_map.alias_fields.append(alias)
            annotation_map.annotation_type_map[alias] = func.__class__.__name__

            internal_type = getattr(func, 'internal_type')
            if internal_type == 'expression':
                func.alias_field_name = alias

            sql_resolution = func.as_sql(self.current_table.backend)

            # Expressions return an array. Maybe in
            # future iterations we should normalize
            # returning a string or ensure that functions
            # return a list
            if isinstance(sql_resolution, list):
                sql_resolution = self.comma_join(sql_resolution)

            annotation_map.sql_statements_dict[alias] = sql_resolution

        # TODO: Functions returns strings and expressions
        # returns arrays - We need to normalize that.
        # TODO: Also CombinedExpressions vs Functions
        # TODO: Put resolve_expression to detect expressions
        # and resolve_functions to detect functions
        return annotation_map

    def decompose_sql_statement(self, sql: str):
        regexes = {
            'select': {
                'columns': re.compile(r'^select\s(.*)\sfrom'),
                'table': re.compile(r'from\s(\w+)'),
                'where': re.compile(r'where\s(.*)\s?'),
                'group_by': re.compile(r'group\sby\s(.*)\s?'),
                'order_by': re.compile(r'order\sby\s(.*)\s?'),
                'limit': re.compile(r'limit\s(\d+)\s?')
            }
        }

        @dataclasses.dataclass
        class StatementMap:
            columns: list = dataclasses.field(default_factory=list)
            table: str = None
            where: list = dataclasses.field(default_factory=list)
            group_by: list = dataclasses.field(default_factory=list)
            order_by: list = dataclasses.field(default_factory=list)
            limit: int = None

            def __setitem__(self, key, value):
                setattr(self, key, value)

        sql_bits = defaultdict(list)
        for name, values in regexes.items():
            if not name in sql:
                continue

            bits = sql_bits[name]
            for key, regex in values.items():
                result = regex.match(sql)

                if not result:
                    result = regex.search(sql)

                if not result:
                    continue

                statement_map = StatementMap()
                tokens = result.group(1).split(',')
                tokens = list(map(lambda x: x.strip(), tokens))

                statement_map[key] = tokens
                bits.append((key, tokens))
        return sql_bits
