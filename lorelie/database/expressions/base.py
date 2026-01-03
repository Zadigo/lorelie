from typing import Any, Union
from dataclasses import dataclass, field
from lorelie.database.expressions.mixins import BASE_OPERATORS
from lorelie.database.expressions.mixins import SQL
from lorelie.lorelie_typings import TypeDecomposedFilterTuple, TypeTable


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
    ... ExpressionMap(column=None, columns=['ages', 'id'],  operator='eq', value='1', tokens=('ages', 'id', 'eq', '1'))
    """

    column: str = None
    columns: list = field(default_factory=list)
    operator: str = None
    value: Union[str, list, int, float, dict] = None
    tokens: list = field(default_factory=list)

    def __post_init__(self):
        operator_identified = False

        for i, value in enumerate(self.tokens):
            if value in BASE_OPERATORS:
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


class Expression(SQL):
    """An expression is defined as a set of tokens that that
    are translated into SQL usable conditions. An expression
    can be passed as a string, a dictionnary or a list/tuple
    of tokens.

    For example, `age__eq=1` is an string expression which can be translated
    to `age = 1` in SQL. Another example is `{'age__eq': 1}` and finally a list/tuple 
    expression can be `('age', 'eq', 1)` which can also be translated to `age = 1` in SQL.

    >>> connection = SQLiteBackend()
    ... Expression('age__eq=1', connection)
    ... Expression({'age__eq': 1}, connection)
    ... Expression([('age', 'eq', 1)], connection)

    This class will decompose the Python representation of the expression
    into its tokens will provide an `ExpressionMap` which contains all the
    parsed components.

    Args:
        expression (Union[str, dict[str, Any], list]): The expression to parse
        table (TypeTable, optional): The table on which the expression is applied. Defaults to None.
    """

    def __init__(self, expression: Union[str, dict[str, Any], list], table: TypeTable = None):
        self.parsed_expressions: list[TypeDecomposedFilterTuple] = []
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
            expression_map = ExpressionMap(tokens=parsed_expression)
            self.expressions_maps.append(expression_map)

    # def __str__(self):
    #     return str(self.parsed_expressions)

    def __repr__(self):
        tokens = (f"<{token}>" for token in self.parsed_expressions)
        return f"Expression({' '.join(tokens)})"

    def __getitem__(self, index):
        return self.expressions_map[index]

    def __iter__(self):
        return iter(self.expressions_maps)

    @staticmethod
    def check_tokens(tokens: list[str]):
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
