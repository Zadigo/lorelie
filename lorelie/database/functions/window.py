from lorelie.database.functions.base import Functions


class Window(Functions):
    template_sql = '{function_name} {over_clause}'

    def __init__(self, function, partition_by=None, order_by=None):
        if function is None:
            raise ValueError("Function cannot be None")

        if not isinstance(function, (Rank, PercentRank, CumeDist, Lead, Lag)):
            raise ValueError("Function should be an instance of Rank")

        if order_by is None:
            order_by = function.field_name

        self.function = function
        self.partition_by = partition_by
        self.order_by = order_by
        super().__init__(function.field_name)

    def as_sql(self, backend):
        if self.partition_by:
            self.function.takes_partition = self.partition_by

        function_name = f'{self.function.template_sql}()'

        return self.template_sql.format(**{
            'function_name': function_name,
            'over_clause': self.function.as_sql(backend),
            # 'alias_name': self.alias_field_name
        })


class WindowFunctionMixin:
    over_clause = 'over ({conditions})'

    def __init__(self, *expressions):
        self.expressions = list(expressions)
        self.takes_partition = None

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

        orderby_clause = f'order by {backend.comma_join(resolved_expressions)}'
        over_clause = [orderby_clause]

        if self.takes_partition is not None:
            partition_clause = self.takes_partition
            if isinstance(self.takes_partition, Functions):
                partition_clause = self.takes_partition.as_sql(backend)
            over_clause.insert(0, f'partition by {partition_clause}')

        return self.over_clause.format(conditions=backend.simple_join(over_clause))


class Rank(WindowFunctionMixin, Functions):
    """This window function assigns a rank to each row 
    within the result set of a query. The rank of a row is 
    determined by adding one to the number of preceding 
    rows with ranks before it

    >>> db.objects.annotate(age_rank=Window(function=Rank('age')))
    ... db.objects.annotate(age_rank=Window(function=Rank(Length('age'))))
    """

    template_sql = 'rank'


class PercentRank(WindowFunctionMixin, Functions):
    """The PERCENT_RANK() is a window function that calculates 
    the percent rank of a given row using this formula :

    `(r - 1) / (the number of rows in the window or partition - r)`

    >>> db.objects.annotate(pct_rank=Window(function=PercentRank('age')))
    ... db.objects.annotate(pct_rank=Window(function=PercentRank(Length('age'))))
    """

    template_sql = 'percent_rank'


class DenseRank(WindowFunctionMixin, Functions):
    """The DENSE_RANK() is a window function that computes 
    the rank of a row in an ordered set of rows and returns 
    the rank as an integer. The ranks are consecutive integers 
    starting from 1. Rows with equal values receive the 
    same rank. And rank values are not skipped in case 
    of ties.
    """
    template_sql = 'dense_rank'


class CumeDist(WindowFunctionMixin, Functions):
    template_sql = 'cume_dist'


class FirstValue(WindowFunctionMixin, Functions):
    template_sql = 'first_value'


class LastValue(WindowFunctionMixin, Functions):
    template_sql = 'last_value'


class NthValue(WindowFunctionMixin, Functions):
    template_sql = 'nth_value'


class Lag(WindowFunctionMixin, Functions):
    template_sql = 'lag'


class Lead(WindowFunctionMixin, Functions):
    template_sql = 'lead'


class NTile(WindowFunctionMixin, Functions):
    template_sql = 'ntile'


class RowNumber(WindowFunctionMixin, Functions):
    template_sql = 'row_number'
