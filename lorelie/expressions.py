class BaseExpression:
    template_sql = None

    def as_sql(self, backend):
        return NotImplemented


class NegatedExpression(BaseExpression):
    template_sql = 'not {expression}'


class When(BaseExpression):
    """Represents a conditional expression in an SQL query. 
    It defines a condition and the corresponding value when 
    the condition is met.
    """
    
    def __init__(self, condition, then_case, **kwargs):
        self.condition = condition
        self.then_case = then_case
        self.field_name = None

    def as_sql(self, backend):
        decomposed_filter = backend.decompose_filters_from_string(
            self.condition
        )
        self.field_name = decomposed_filter[0]
        condition = backend.simple_join(
            backend.build_filters(
                decomposed_filter,
                space_characters=False
            )
        )
        sql = backend.WHEN.format_map({
            'condition': condition,
            'value': backend.quote_value(self.then_case)
        })
        return sql


class Case(BaseExpression):
    """Represents a conditional expression in an SQL query. 
    It evaluates multiple conditions and returns a value 
    based on the first condition that is met."""

    CASE = 'case {conditions}'
    CASE_ELSE = 'else {value}'
    CASE_END = 'end {alias}'

    def __init__(self, *cases, default=None):
        self.field_name = None
        self.alias_name = None
        self.default = default

        for case in cases:
            if not isinstance(case, When):
                raise ValueError('Value should be an instance of When')
        self.cases = list(cases)

    def as_sql(self, backend):
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

        if self.alias_name is None:
            raise ValueError(
                "Case annotation does not have an "
                f"alias name. Got: {self.alias_name}"
            )

        # TODO: We should check that the alias name does not
        # conflict with a field in the original table

        case_end_sql = self.CASE_END.format(alias=self.alias_name)
        return backend.simple_join([case_sql, case_else_sql, case_end_sql])


class OrderBy(BaseExpression):
    """Represents an ORDER BY SQL clause 
    to specify the sorting of query results."""

    template_sql = 'order by {conditions}'

    def __init__(self, fields):
        self.ascending = set()
        self.descending = set()

        if not isinstance(fields, (list, tuple)):
            raise ValueError(
                "Ordering fields should be a list "
                "of field names on your table"
            )
        self.fields = list(fields)
        self.map_fields()

    def __bool__(self):
        return len(self.fields) > 0

    @classmethod
    def new(cls, fields):
        return cls(fields)

    def map_fields(self):
        for field in self.fields:
            if field.startswith('-'):
                field = field.removeprefix('-')
                self.descending.add(field)
            else:
                self.ascending.add(field)

    def as_sql(self, backend):
        conditions = []

        for field in self.ascending:
            conditions.append(
                backend.ASCENDING.format_map({'field': field})
            )

        for field in self.descending:
            conditions.append(
                backend.DESCENDING.format_map({'field': field})
            )

        fields = backend.comma_join(conditions)
        ordering_sql = backend.ORDER_BY.format_map({'conditions': fields})
        return [ordering_sql]


class CombinedExpression:
    EXPRESSION = '({inner}) {outer}'

    def __init__(self, *funcs):
        self.others = list(funcs)
        self.children = []

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.children}>'

    def __or__(self, other):
        self.children.append('or')
        self.children.append(other)
        return self

    def __and__(self, other):
        self.children.append('and')
        self.children.append(other)
        return self

    def build_children(self, operator='and'):
        for other in self.others:
            self.children.append(other)
            self.children.append(operator)

        if self.children[-1] == 'and' or self.children[-1] == 'or':
            self.children[-1] = ''

        self.children = list(filter(lambda x: x != '', self.children))

    def as_sql(self, backend):
        sql_statement = []
        for child in self.children:
            if isinstance(child, str):
                sql_statement.append(child)
                continue
            child_sql = child.as_sql(backend)
            sql_statement.append(child_sql)

        inner_items = []
        outer_items = []
        seen_operator = None
        is_inner = True

        for item in sql_statement:
            if item == 'and' or item == 'or':
                if seen_operator is None:
                    is_inner = True
                elif seen_operator != item:
                    is_inner = False
                seen_operator = item

            if is_inner:
                if isinstance(item, str):
                    item = [item]
                inner_items.extend(item)
                continue
            else:
                if isinstance(item, str):
                    item = [item]
                outer_items.extend(item)
                continue

        inner = backend.simple_join(inner_items)
        outer = backend.simple_join(outer_items)
        sql = self.EXPRESSION.format(inner=inner, outer=outer).strip()
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
    """

    def __init__(self, **expressions):
        self.expressions = expressions

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.expressions}>'

    def __and__(self, other):
        instance = CombinedExpression(self, other)
        instance.build_children()
        return instance

    def __or__(self, other):
        instance = CombinedExpression(self, other)
        instance.build_children(operator='or')
        return instance

    def as_sql(self, backend):
        filters = backend.decompose_filters(**self.expressions)
        built_filters = backend.build_filters(filters, space_characters=False)
        return [backend.operator_join(built_filters)]


# class F:
#     ADD = '+'

#     def __init__(self, field):
#         self.field = field
#         self.children = [self]
#         self.sql = None

#     def __repr__(self):
#         return f'{self.__class__.__name__}({self.field})'

#     def __add__(self, obj):
#         if isinstance(obj, F):
#             true_represention = obj.field
#         elif isinstance(obj, (float, int, str)):
#             true_represention = Value(obj)
#         self.children.append(true_represention)
#         sql_tokens = [self.field, self.ADD, true_represention]
#         self.sql = backend.simple_join(sql_tokens)
#         return CombinedExpression(*self.children, operator=self.ADD)


# result = F('age') + F('age')
# print(result)
# print(result.as_sql())
