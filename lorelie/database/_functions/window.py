from lorelie.functions import Functions


class Window(Functions):
    template_sql = '{function_name} over (order by {condition}) as {alias_name}'

    def __init__(self, expression=None, order_by=None):
        self.expression = expression
        self.order_by = order_by
        super().__init__(expression.field_name)

    def as_sql(self, backend):
        return self.template_sql.format(**{
            'function_name': self.expression.template_sql,
            'condition': self.expression.as_sql(backend),
            'alias_name': self.alias_field_name
        })


class WindowFunctionMixin:
    def __init__(self, *expressions):
        self.expressions = list(expressions)

        names = []
        for expression in expressions:
            if isinstance(expression, str):
                names.append(expression)

            if isinstance(expression, Functions):
                names.append(expression.field_name)

        compound_name = '_'.join(names)
        field_name = f'{self.__class__.__name__.lower()}_{compound_name}'
        super().__init__(field_name)

    def as_sql(self, backend):
        resolved_expressions = []
        for expression in self.expressions:
            if isinstance(expression, str):
                resolved_expressions.append(expression)
            elif isinstance(expression, Functions):
                result = expression.as_sql(backend)
                resolved_expressions.append(result)

        return backend.comma_join(resolved_expressions)


class Rank(WindowFunctionMixin, Functions):
    """This window function assigns a rank to each row 
    within the result set of a query. The rank of a row is 
    determined by adding one to the number of preceding 
    rows with ranks before it

    >>> db.objects.annotate(expression=Rank('age'))
    ... db.objects.annotate(expression=Rank(Length('age')))
    """

    template_sql = 'rank()'


class PercentRank(WindowFunctionMixin, Functions):
    """The PERCENT_RANK() is a window function that calculates 
    the percent rank of a given row using this formula :

    `(r - 1) / (the number of rows in the window or partition - r)`

    >>> db.objects.annotate(expression=PercentRank('age'))
    ... db.objects.annotate(expression=PercentRank(Length('age')))
    """

    template_sql = 'percent_rank()'
