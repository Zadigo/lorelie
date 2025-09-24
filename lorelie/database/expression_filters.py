import re
from dataclasses import dataclass, field
from typing import Any, Union
from functools import cached_property


_T_DecomposedFilter = tuple[str, str, str | int | Any]

class ExpressionFiltersMixin:
    """This mixin class is used to decompose filter
    expressions used for example in query methods
    e.g. filter(age=1) and can be implemented in 
    any class that requires the analysis and decomposition
    of these kinds of values

    """
    base_filters = {
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

    @cached_property
    def list_of_operators(self):
        operators = list(self.base_filters.values())
        operators.append('<>')
        return operators

    def is_query_filter(self, value_or_values):
        """Checks that the last value or that a single
        value is a query filtering value. For example
        `eq` in `['name', 'eq']`

        >>> self.is_query_filter(['name', 'eq'])
        ... True
        """
        if isinstance(value_or_values, list):
            value_or_values = value_or_values[-1]
        return value_or_values in list(self.base_filters.keys())

    def translate_operator_from_tokens(self, tokens):
        """Translates a string filter in a list of tokens
        to a valid mathematical operator

        >>> translate_operator_from_tokens(['age', '=', 1])
        """
        translated = []
        for item in tokens:
            if self.is_query_filter(item):
                translated.append(self.base_filters[item])
                continue

            translated.append(item)
        return translated

    def translate_operators_from_tokens(self, tokens):
        """Translates a string filter in a list of tokens
        to a valid mathematical operator

        >>> translate_operator_from_tokens([['age', 'eq', 1], ['name', '=', 1]])
        """
        translated = []
        for item in tokens:
            if not isinstance(item, (list, tuple)):
                raise ValueError(f"{item} should be of type list/tuple")
            translated.append(self.translate_operator_from_tokens(item))
        return translated

    def decompose_filters_columns(self, value):
        """Return only the column parts of the filters
        that were passed

        >>> self.decompose_filters_from_string('rowid__eq')
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

        >>> self.decompose_filters_from_string('rowid__eq=1')
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

    def decompose_filters(self, **kwargs) -> list[_T_DecomposedFilter]:
        """Decompose a set of filters to a list of
        key, operator and value list from a dictionnary

        >>> self.decompose_filters(id__eq=1)
        ... [('id', '=', '1')]

        >>> self.decompose_filters(followers__users__id__eq=1)
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

    def build_filters(self, items: list[_T_DecomposedFilter], space_characters: bool=True) -> list[str]:
        """Tranform a list of decomposed filters to
        usable string conditions for an sql statement

        >>> self.build_filters([('rowid', '=', '1')])
        ... ["rowid = '1'"]

        >>> self.build_filters([('rowid', 'startswith', '1')])
        ... ["rowid like '1%'"]


        >>> self.build_filters([('url', '=', Lower('url')])
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


@dataclass
class ExpressionMap:
    """An ExpressionMap is the Python dataclass
    representation of a filter expression used
    to create the SQL tokens for querying the
    database. An expression can consist of a
    simple column `age=1`, a column followed by
    an operator `age__eq=1` or multiple columns
    followed or not by an operator `ages__id=1`
    or `ages__id__eq=1`. The latter is used especially
    when we need to follow foreign keys

    >>> 'ages__id__eq=1'
    ... ExpressionMap(
            column=None, 
            columns=['ages', 'id'], 
            operator='eq', value='1', 
            tokens=('ages', 'id', 'eq', '1')
        )
    """

    column: str = None
    columns: list = field(default_factory=list)
    operator: str = None
    value: Union[str, list, int, float, dict] = None
    tokens: list = field(default_factory=list)

    def __post_init__(self):
        operators = ExpressionFiltersMixin().list_of_operators
        operator_identified = False

        for i, value in enumerate(self.tokens):
            if value in operators:
                operator_identified = True
                self.operator = self.tokens[i]
                continue

            if operator_identified:
                self.value = self.tokens[i]
            else:
                self.columns.append(self.tokens[i])

        if len(self.columns) == 1:
            self.column = self.columns[0]

    @property
    def expands_foreign_key(self):
        if len(self.tokens) == 1 or len(self.tokens == 2):
            return False
        return True

    def __contains__(self, value):
        return any([
            value in self.column,
            value in self.columns,
            value in self.value
        ])

    def __hash__(self):
        return hash((self.columns, self.operator, self.value))


class ExpressionFilter(ExpressionFiltersMixin):
    """An ExpressionFilter parses expressions in order
    to be used efficiently by other parts of program

    >>> connection = SQLiteBackend()
    ... ExpressionFilter('age__eq=1', connection)
    ... ExpressionFilter({'age__eq': 1}, connection)
    ... ExpressionFilter([('age', 'eq', 1)], connection)
    """

    def __init__(self, expression, table=None):
        self.parsed_expressions = []
        self.table = table

        if isinstance(expression, str):
            result = self.decompose_filters_from_string(expression)
            self.parsed_expressions.extend(result)
            # tokens = expression.split('__')

            # # Case where a single word is passed to
            # # the __init__ e.g. age instead or age=1
            # if len(tokens) == 1 and '=' not in tokens[-1]:
            #     raise ValueError(
            #         "An expression should contain at least "
            #         f"a column, an operator and value. Got: {tokens}"
            #     )

            # lhv, rhv = tokens[-1].split('=')
            # result = [*tokens[:-1], lhv, rhv]

            # if len(result) == 2:
            #     result.insert(1, 'eq')

            # self.check_tokens(result)
            # self.parsed_expressions.append(result)
        elif isinstance(expression, dict):
            result = self.decompose_filters(**expression)
            self.parsed_expressions.extend(result)
            # for key, value in expression.items():
            #     tokens = key.split('__')
            #     tokens.append(value)
            #     self.parsed_expressions.append(tokens)
        elif isinstance(expression, (list, tuple)):
            for item in expression:
                if not isinstance(item, (list, tuple)):
                    raise ValueError(
                        f"{item} should be an "
                        "instance of list/tuple"
                    )
                self.parsed_expressions.append(item)

        self.expressions_maps = []
        for parsed_expression in self.parsed_expressions:
            self.expressions_maps.append(
                ExpressionMap(tokens=parsed_expression)
            )

    def __str__(self):
        return str(self.parsed_expressions)

    def __getitem__(self, index):
        return self.expressions_map[index]

    def __iter__(self):
        return iter(self.expressions_maps)

    @staticmethod
    def check_tokens(tokens):
        # If the user uses too much underscores
        # we can have cases where tokens will
        # start with a _ like in ages___eq=1
        # where instead of eq we'll get _eq
        for token in tokens:
            if token.startswith('_'):
                raise ValueError(
                    "Ensure that your expression contains "
                    "a maximum of two underscores when trying to "
                    f"expand a foreign key column eg. __. Got: {token}"
                )
