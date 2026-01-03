from abc import ABC, abstractmethod
from typing import Any, ClassVar, Optional, override
import re
from lorelie.lorelie_typings import TypeAny, TypeField, TypeSQLiteBackend


class BaseExpression(ABC):
    template_sql: Optional[str] = None
    # The columns of the table on which
    # the function operates on - these are
    # the keys of the dictionary passed
    # to the function and are not necessarily
    # the actual columns in the database
    # but they should map to actual columns
    # in the database table
    # e.g. in the case of F('age') + 1
    # the function column is 'age'
    _function_columns: list[str] = []

    @property
    def internal_type(self):
        return 'expression'

    def get_function_columns(self, expression: dict[str, Any]):
        self._function_columns = list(expression.keys())

    @abstractmethod
    def as_sql(self, backend: TypeSQLiteBackend):
        raise NotImplemented

    def deconstruct(self):
        return {'type': self.__class__.__name__}


class Value(BaseExpression):
    """This class is not an expressioon per se but can be used
    to wrap values that will be used in expressions. The output
    field is guessed based on the type of value passed and then
    used to convert the value to a database-compatible format.

    >>> Value(5) + F('age')
    ... F('price') * Value(1.2)

    In the first example above, the integer value `5` is wrapped
    in a `Value` instance before being added to the field `age`.
    This ensures that the ORM correctly interprets `5` as a value
    to be used in the expression rather than as a field name.

    In the second example, the float value `1.2` is wrapped
    in a `Value` instance before being multiplied with the field
    `price`.

    Args:
        value (Any): The value to be wrapped.
        output_field (Optional[TypeField]): The field type of the value.
    """

    def __init__(self, value: Any, output_field: Optional[TypeField] = None):
        self.value = value
        self.internal_name = 'value'
        self.output_field = output_field
        self.get_output_field()

    def __repr__(self):
        return f'{self.__class__.__name__}({self.to_database()})'

    def get_output_field(self):
        from lorelie.fields.base import (CharField, FloatField, IntegerField,
                                         JSONField)
        if self.output_field is None:
            if isinstance(self.value, int):
                self.output_field = IntegerField(self.internal_name)
            elif isinstance(self.value, float):
                self.output_field = FloatField(self.internal_name)
            elif isinstance(self.value, str):
                if self.value.isdigit() or self.value.isnumeric():
                    self.output_field = IntegerField(self.internal_name)
                else:
                    self.output_field = CharField(self.internal_name)
            elif isinstance(self.value, dict):
                self.output_field = JSONField(self.internal_name)

    def to_python(self, value: Any):
        return None

    def to_database(self):
        if self.output_field is None:
            return self.value
        return self.output_field.to_database(self.value)

    @override
    def as_sql(self, backend: TypeSQLiteBackend):
        if callable(self.value):
            self.value = str(self.value)

        if hasattr(self.value, 'internal_type'):
            self.value = str(self.value)

        return [backend.quote_value(self.value)]


class NegatedExpression(BaseExpression):
    """A negated expression is used to represent
    the logical negation of another expression.

    >>> NegatedExpression(Q(firstname="Kendall"))
    ... ~Q(lastname="Jenner")
    ... "not firstname='Kendall'"

    Negated expressions can be combined with other expressions
    using logical operators such as AND and OR.

    >>> NegatedExpression(Q(age=21)) & Q(age=34)
    ... NegatedExpression(F('age') + 1)

    These are equivalent to SQL statements such as:
    * `not (age=21 and age=34)`
    * `not (age + 1)`
    """
    template_sql = 'not {expression}'

    def __init__(self, expression: 'F | Q'):
        self.children = [expression]
        self.seen_expressions = []

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.expression}>'

    def __and__(self, other: 'F | Q | NegatedExpression'):
        self.children.append('and')
        if isinstance(other, NegatedExpression):
            self.children.extend(other.children)
            return self

        if isinstance(other, (F, Q)):
            self.seen_expressions.append(other.__class__.__name__)

        # We should not be able to combine certain
        # types of expressions ex. F & Q, F | Q
        # self.seen_expressions = [
        #     child.__class__.__name__
        #         for child in self.children
        #             if not isinstance(child, str)
        # ]

        # unique_children = set(self.seen_expressions)
        # if len(unique_children) > 1:
        #     pass

        # print(self.seen_expressions)

        self.children.append(other)
        return self

    def as_sql(self, backend: TypeSQLiteBackend):
        seen_expressions = []

        def map_children(node: 'F | Q | str'):
            if isinstance(node, str):
                return node
            return backend.simple_join(node.as_sql(backend))

        sql = map(map_children, self.children)

        return self.template_sql.format_map({
            'expression': backend.simple_join(sql)
        })


class When(BaseExpression):
    """Represents a conditional expression in an SQL query. 
    It defines a condition and the corresponding value when 
    the condition is met. When are typically used within
    Case expressions to create complex conditional logic.

    >>> logic = When('firstname=Kendall', 'Kylie')
    ... case = Case(logic)
    ... db.objects.annotate(case)
    """

    def __init__(self, condition: 'Q | CombinedExpression | str', then_case: Any, **kwargs):
        self.condition = condition
        self.then_case = then_case
        self.field_name = None

    def __repr__(self):
        return f'When({self.field_name} -> {self.then_case})'

    @override
    def as_sql(self, backend: TypeSQLiteBackend):
        list_of_filters = []
        if isinstance(self.condition, (Q, CombinedExpression)):
            complex_filter = self.condition.as_sql(backend)
            list_of_filters.extend(complex_filter)
        else:
            decomposed_filter = backend.decompose_filters_from_string(
                self.condition
            )
            built_filters = backend.build_filters(
                decomposed_filter,
                space_characters=False
            )
            self.field_name = decomposed_filter[0]
            list_of_filters.extend(built_filters)

        condition = backend.simple_join(list_of_filters)
        sql = backend.WHEN.format_map({
            'condition': condition,
            'value': backend.quote_value(self.then_case)
        })
        return sql


class Case(BaseExpression):
    """Represents a conditional expression in an SQL query. 
    It evaluates multiple conditions and returns a value 
    based on the first condition that is met

    >>> logic = When('firstname=Kendall', 'Kylie')
    ... case = Case(condition1)
    ... db.objects.annotate(case)

    Args:
        *cases (When): A variable number of When instances representing the conditions and their corresponding values.
        default (Optional[TypeAny]): The default value to return if none of the conditions are met.
    """

    CASE: ClassVar[str] = 'case {conditions}'
    CASE_ELSE: ClassVar[str] = 'else {value}'
    CASE_END: ClassVar[str] = 'end {alias}'

    def __init__(self, *cases: 'When', default: Optional[TypeAny] = None):
        self.field_name = None
        self.alias_field_name = None
        self.default = default

        for case in cases:
            if not isinstance(case, When):
                raise ValueError('Value should be an instance of When')
        self.cases = list(cases)

    def __repr__(self):
        cases_repr = ', '.join(repr(case) for case in self.cases)
        return f'{self.__class__.__name__}({cases_repr})'

    @override
    def as_sql(self, backend: TypeSQLiteBackend):
        fields = set()
        statements_to_join = []

        for case in self.cases:
            fields.add(case.field_name)
            statements_to_join.append(case.as_sql(backend))

        case_sql = self.CASE.format_map({
            'conditions': backend.simple_join(statements_to_join)
        })

        if self.default is None:
            self.default = ''

        case_else_sql = self.CASE_ELSE.format(
            value=backend.quote_value(self.default)
        )

        if self.alias_field_name is None:
            raise ValueError(
                "Case annotation does not have an "
                f"alias name. Got: {self.alias_field_name}"
            )

        # TODO: We should check that the alias name does not
        # conflict with a field in the original table

        case_end_sql = self.CASE_END.format(alias=self.alias_field_name)
        return backend.simple_join([case_sql, case_else_sql, case_end_sql])


class CombinedExpression:
    """A combined expression is a combination
    of multiple expressions in order to create
    complex SQL statements.

    >>> CombinedExpression(Q(age=21), Q(age=34))
    ... CombinedExpression(F('age'))

    These are equivalent to SQL statements such as:
    * `(age=21 and age=34)`
    * `0 + age`

    Args:
        *funcs (F | Q): A variable number of expressions to be combined.
        auto_build (bool): Whether to automatically build the children expressions upon initialization.
    """

    template_sql: ClassVar[str] = '({inner}) {outer}'

    def __init__(self, *funcs: 'F | Q', auto_build: bool = True):
        self.others = list(funcs)
        self.children: list['F | Q | str'] = []
        # Indicates that the expression
        # will be a mathematical one
        # e.g. F('age') + 1
        self.is_math = False
        # These expressions require an alias
        # field name in order to be rendered
        # correctly in sqlite
        self.alias_field_name = None
        # NOTE: Technically we will never be
        # using the combined expression as a
        # standalone class but we might need
        # to build the existing children on
        # init to avoid arrays like
        # a = CombinedExpression(Q(firstname='Kendall'))
        # b = Q(age__gt=26)
        # c = a & b -> ['(and age>26)']
        if auto_build:
            self.build_children()

    # def __repr__(self):
    #     return f'<{self.__class__.__name__} :: {self.children}>'

    def __or__(self, other):
        self.children.append('or')
        self.children.append(other)
        return self

    def __and__(self, other):
        self.children.append('and')
        self.children.append(other)
        return self

    def __add__(self, other):
        self.children.append('+')
        self.children.append(Value(other))
        return self

    def __sub__(self, other):
        self.children.append('-')
        self.children.append(Value(other))
        return self

    def __truediv__(self, other):
        self.children.append('/')
        self.children.append(Value(other))
        return self

    def __mul__(self, other):
        self.children.append('*')
        self.children.append(Value(other))
        return self

    @property
    def internal_type(self):
        return 'expression'

    def build_children(self, operator='and'):
        """Transfers items from `others` to `children` inserting
        the operator in between each item."""
        index = 0
        while self.others:
            other = self.others.pop(0)
            self.children.insert(0, other)

            if self.others:
                self.children.insert(index, operator)

            index += 1

        self.children = list(reversed(self.children))

    @override
    def as_sql(self, backend: TypeSQLiteBackend):
        sql_statement: list[Any] = []

        for child in self.children:
            if isinstance(child, (str, int, float)):
                sql_statement.append(child)
                continue

            child_sql = child.as_sql(backend)
            sql_statement.extend(child_sql)

        # Since items such as (age)
        # should be simplified to "age"
        # by removing the parentheses
        for index, item in enumerate(sql_statement):
            result = re.match(r'^\((\w+)\)$', str(item))
            if result:
                sql_statement[index] = result.group(1)

        inner_items = []
        outer_items = []
        seen_operator = None
        is_inner = True

        for item in sql_statement:
            if item == 'and' or item == 'or' or item in ['+', '-', '/', '*']:
                if seen_operator is None:
                    is_inner = True
                elif seen_operator != item:
                    is_inner = False
                seen_operator = item

            if isinstance(item, (str, int, float)):
                item = [item]

            if is_inner:
                inner_items.extend(item)
                continue
            else:
                outer_items.extend(item)
                continue

        inner = backend.simple_join(inner_items)
        outer = backend.simple_join(outer_items)
        sql = self.template_sql.format(inner=inner, outer=outer).strip()
        # TODO: Ensure that the SQL returned from these types
        # of expressions all return a list of strings
        return [sql]


class Q(BaseExpression):
    """Represents a filter expression within SQL queries, 
    employed within the `WHERE` clause for example
    to define complex filter conditions.

    >>> Q(firstname="Kendall") & Q(lastname="Jenner")

    The expression above expression results in a logical AND operation
    between two Q instances, where each instance represents a filter condition.

    >>> Q(firstname="Kendall") | Q(lastname="Jenner")

    The above expression results in a logical OR operation.

    Args:
        **expressions (Any): Keyword arguments representing field lookups and their corresponding values for filtering.
    """

    def __init__(self, **expressions: Any):
        self.expressions = expressions

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.expressions}>'

    def __and__(self, other):
        instance = CombinedExpression(self, other, auto_build=False)
        instance.build_children()
        return instance

    def __or__(self, other):
        instance = CombinedExpression(self, other, auto_build=False)
        instance.build_children(operator='or')
        return instance

    def __invert__(self):
        return NegatedExpression(self)

    @override
    def as_sql(self, backend: TypeSQLiteBackend):
        filters = backend.decompose_filters(**self.expressions)
        built_filters = backend.build_filters(filters, space_characters=False)
        return [backend.operator_join(built_filters)]

    def deconstruct(self):
        value = super().deconstruct()
        value.update({'expressions': self.expressions})
        return value


class F(BaseExpression):
    """The F function allows us to make operations
    directly on the value of a field in the database.

    >>> F('age') + 1
    ... F('price') * 1.2
    ... F('price') + F('price')

    These operations will translate directly to database
    operations. For example in the case of `F('price') * 1.2`,
    the ORM will generate an SQL statement that multiplies
    the value of the `price` field by `1.2` directly in the database.
    """

    ADD: ClassVar[str] = '+'
    SUBSRACT: ClassVar[str] = '-'
    MULTIPLY: ClassVar[str] = 'x'
    DIVIDE: ClassVar[str] = '/'

    def __init__(self, field: str):
        self.field = field
        self._function_columns = [field]

    def __repr__(self):
        return f'{self.__class__.__name__}({self.field})'

    def __add__(self, other):
        if not isinstance(other, (F, str, int)):
            return NotImplemented

        if isinstance(other, (int, str, float)):
            other = Value(other)

        combined = CombinedExpression(self, other, auto_build=False)
        combined.build_children(operator=self.ADD)
        return combined

    def __mul__(self, other):
        if not isinstance(other, (F, str, int)):
            return NotImplemented

        if isinstance(other, (int, str, float)):
            other = Value(other)

        combined = CombinedExpression(self, other, auto_build=False)
        combined.build_children(operator=self.MULTIPLY)
        return combined

    def __truediv__(self, other):
        if not isinstance(other, (F, str, int)):
            return NotImplemented

        if isinstance(other, (int, str, float)):
            other = Value(other)

        combined = CombinedExpression(self, other, auto_build=False)
        combined.build_children(operator=self.DIVIDE)
        return combined

    def __sub__(self, other):
        if not isinstance(other, (F, str, int)):
            return NotImplemented

        if isinstance(other, (int, str, float)):
            other = Value(other)

        combined = CombinedExpression(self, other, auto_build=False)
        combined.build_children(operator=self.SUBSRACT)
        return combined

    def __neg__(self):
        return NegatedExpression(self)

    @override
    def as_sql(self, backend: TypeSQLiteBackend):
        return [self.field]
