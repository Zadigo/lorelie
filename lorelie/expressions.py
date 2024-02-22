from lorelie.backends import SQL

backend = SQL()


class BaseExpression:
    def __init__(self):
        self.backend = None
        self.sql_statement = None

    def __str__(self):
        return f'<{self.__class__.__name__}: {self.sql_statement}>'

    def as_sql(self):
        pass


class When(BaseExpression):
    def __init__(self, condition, then_case, else_case=None):
        super().__init__()
        self.condition = condition
        self.then_case = then_case
        self.else_case = else_case

    def as_sql(self):
        decomposed_filter = backend.decompose_filters_from_string(
            self.condition
        )
        condition = backend.simple_join(
            backend.build_filters(decomposed_filter)
        )
        self.sql_statement = sql = backend.WHEN.format_map({
            'condition': condition,
            'value': backend.quote_value(self.then_case)
        })
        return sql


class Case(BaseExpression):
    def __init__(self, *cases):
        super().__init__()
        self.field_name = None

        for case in cases:
            if not isinstance(case, When):
                raise ValueError('Value should be an instance of When')
        self.cases = list(cases)

    def as_sql(self):
        statements_to_join = [case.as_sql() for case in self.cases]
        self.sql_statement = sql = backend.CASE.format_map({
            'field': self.field_name,
            'conditions': backend.simple_join(statements_to_join)
        })
        return sql


# condition1 = When('name__eq=Kendall', 'Kylie', else_case=None)
# case = Case(condition1)
# condition2 = When('name__eq=Kylie', 'Julie', else_case=None)
# case = Case(condition1, condition2)
# case.field_name = 'something'
# case.as_sql()
# print(case)
