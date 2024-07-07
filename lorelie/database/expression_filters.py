from dataclasses import dataclass, field
from typing import Union


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
    ... ExpressionMap(column=None, columns=['ages', 'id'], operator='eq', value='1', tokens=('ages', 'id', 'eq', '1'))
    """

    column: str = None
    columns: list = field(default_factory=list)
    operator: str = None
    value: Union[str, list, int, float, dict] = None
    tokens: list = field(default_factory=list)

    def __post_init__(self):
        operators = ['eq']
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

    def __hash__(self):
        return hash((self.columns, self.operator, self.value))


class ExpressionFilter:
    """An ExpressionFilter parses expressions in order
    to be used efficiently by other parts of program"""

    def __init__(self, expression, table=None):
        self.expressions = []
        self.table = table

        if isinstance(expression, str):
            tokens = expression.split('__')

            # Case where a single word is passed to
            # the __init__ e.g. age instead or age=1
            if len(tokens) == 1 and '=' not in tokens[-1]:
                raise ValueError(
                    "An expression should contain at least "
                    f"a column, an operator and value. Got: {tokens}"
                )

            lhv, rhv = tokens[-1].split('=')
            result = [*tokens[:-1], lhv, rhv]

            if len(result) == 2:
                result.insert(1, 'eq')

            self.check_tokens(result)
            self.expressions.extend(result)
        elif isinstance(expression, dict):
            for key, value in expression.items():
                tokens = key.split('__')
                tokens.append(value)
                self.expressions.append(tokens)

        self.expressions_maps = []
        for expression in self.expressions:
            self.expressions_maps.append(ExpressionMap(tokens=expression))

    def __str__(self):
        return str(self.expressions)

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
