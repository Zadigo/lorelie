# from lorelie.backends import SQLiteBackend
# from lorelie.backends import SQL

# backend = SQL()


class BaseExpression:
    template = None

    def __init__(self):
        self.backend = None
        self.sql_statement = None

    def as_sql(self):
        pass


class When(BaseExpression):
    def __init__(self, condition, then_case, **kwargs):
        super().__init__()
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
    CASE = 'case {conditions}'
    CASE_ELSE = 'else {value}'
    CASE_END = 'end {alias}'

    def __init__(self, *cases, default=None):
        super().__init__()
        self.field_name = None
        self.alias_name = None
        self.default = default

        for case in cases:
            if not isinstance(case, When):
                raise ValueError('Value should be an instance of When')
        self.cases = list(cases)

    def as_sql(self, backend):
        # sql = backend.CASE.format_map({
        #     'field': self.field_name,
        #     'conditions': backend.simple_join(statements_to_join),
        #     'alias': self.alias_name
        # })

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
        case_end_sql = self.CASE_END.format(alias=self.alias_name)
        return backend.simple_join([case_sql, case_else_sql, case_end_sql])


class OrderBy(BaseExpression):
    """Creates an order by SQL clause
    for an existing expression"""

    template = 'order by {conditions}'

    def __init__(self, fields):
        self.ascending = set()
        self.descending = set()

        for field in fields:
            if field.startswith('-'):
                field = field.removeprefix('-')
                self.descending.add(field)
            else:
                self.ascending.add(field)

        self.fields = fields
        super().__init__()

    def __bool__(self):
        return len(self.fields) > 0

    def __call__(self, fields):
        self.fields = fields
        return self

    def as_sql(self, backend):
        conditions = []

        for field in self.ascending:
            conditions.append(
                backend.ASCENDING.format_map({'field': field})
            )

        for field in self.descending:
            conditions.append(
                backend.DESCENDNIG.format_map({'field': field})
            )

        fields = backend.comma_join(conditions)
        ordering_sql = backend.ORDER_BY.format_map({'conditions': fields})
        return ordering_sql


# instance = OrderBy(['firstname', '-lastname'])
# print(instance.as_sql(SQLiteBackend()))


# class CombinedExpression(BaseExpression):
#     def __init__(self, *funcs, operator='and'):
#         self.selected_operator = operator
#         self.functions = list(funcs)

#     def __repr__(self):
#         expressions = [(repr(func), func.selected_operator) for func in self.functions]
#         return f'<{self.__class__.__name__}: {expressions}>'

#     def __and__(self, obj):
#         self.functions.append(obj)
#         return CombinedExpression(*self.functions)

#     def __or__(self, obj):
#         self.functions.append(obj)
#         return CombinedExpression(*self.functions, operator='or')

#     def build_operator_list(self):
#         return [(func, func.selected_operator) for func in self.functions]

#     def as_sql(self, backend):
#         # sql_tokens = [func.as_sql(backend) for func in self.functions]
#         # return backend.operator_join(sql_tokens)
#         sql_tokens = []
#         for func, operator in self.build_operator_list():
#             sql = func.as_sql(backend)
#             sql_tokens.append([sql, operator])
#         sql_tokens[-1][-1] = ''
#         sql_tokens = itertools.chain(*sql_tokens)
#         sql = backend.simple_join(sql_tokens)
#         return sql


# class Q(BaseExpression):
#     def __init__(self, **expression):
#         self.expression = expression
#         self.selected_operator = 'and'

#     def __repr__(self):
#         return f'<{self.__class__.__name__}({self.expression})>'

#     def __and__(self, obj):
#         if not isinstance(obj, (Q, CombinedExpression)):
#             raise ValueError('')
#         return CombinedExpression(self, obj)

#     def __or__(self, obj):
#         if not isinstance(obj, (Q, CombinedExpression)):
#             raise ValueError('')
#         self.selected_operator = 'or'
#         return CombinedExpression(self, obj, operator='or')

#     def as_sql(self, backend):
#         from lorelie.backends import SQLiteBackend
#         if not isinstance(backend, SQLiteBackend):
#             raise ValueError()
#         filters = backend.decompose_filters(**self.expression)
#         filters = backend.build_filters(filters, space_characters=False)
#         return backend.operator_join(filters)


# condition1 = When('name__eq=Kendall', 'Kylie', else_case=None)
# case = Case(condition1)
# condition2 = When('name__eq=Kylie', 'Julie', else_case=None)
# case = Case(condition1, condition2)
# case.field_name = 'something'
# case.as_sql()
# print(case)

# from lorelie.backends import SQLiteBackend
# backend = SQLiteBackend()
# a = Q(name='Kendall', height__gte=145)
# b = Q(age__gte=15)
# c = Q(height=145)
# d = a & b | c
# print(d)
# print(d.as_sql(backend))
# from django.db.models import F


# class CombinedExpression:
#     def __init__(self, *funcs, operator=''):
#         self.funcs = list(funcs)
#         self.operator = operator

#     def __repr__(self):
#         expresssions = f' {self.operator} '.join([repr(func) for func in self.funcs])
#         return f'<{self.__class__.__name__}: {expresssions}>'

#     def as_sql(self):
#         sql_tokens = []
#         for func in self.funcs:
#             sql_tokens.append(func.sql)
#         return sql_tokens


# class Value:
#     def __init__(self, value):
#         self.value = value
#         self.sql = None

#     def __repr__(self):
#         return f'{self.__class__.__name__}({self.value})'

#     def to_python(self):
#         return str(self.value)

#     def as_sql(self):
#         self.sql = backend.quote_value(self.value)

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
