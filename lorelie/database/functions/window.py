from typing import ClassVar, Final, Optional
from lorelie.database.functions.base import Functions
from lorelie.lorelie_typings import TypeSQLiteBackend, TypeWindowFunction


class Window(Functions):
    """The Window class provides a way to utilize SQL window
    functions within your database operations. Window functions perform
    calculations across a set of table rows that are somehow related to
    the current row. This is similar to aggregate functions, but window
    functions do not cause rows to become grouped into a single output row.
    Instead, rows retain their separate identities

    >>> table.objects.annotate(alias_name=Window(...))

    Args:
        function (Functions): An instance of a window function class (e.g., Rank, PercentRank) to be applied.
        partition_by (str, optional): A field name to partition the data by. Defaults to None.
        order_by (str, optional): A field name to order the data by. If not provided, it defaults to the field name of the function.
    """

    template_sql: ClassVar[str] = '{function_name} {over_clause}'

    def __init__(self, function: TypeWindowFunction, partition_by: Optional[str] = None, order_by: Optional[str] = None):
        if function is None:
            raise ValueError("Function cannot be None")

        # if not isinstance(function, (Rank, PercentRank, CumeDist, Lead, Lag)):
        #     raise ValueError("Function should be an instance of Rank")

        if function.internal_type != 'window_function':
            raise ValueError("Function should be a window function instance")

        if order_by is None:
            order_by = function.field_name

        self.function = function
        self.partition_by = partition_by
        self.order_by = order_by
        super().__init__(function.field_name)

    def as_sql(self, backend: TypeSQLiteBackend) -> str:
        if self.partition_by:
            self.function.takes_partition = self.partition_by

        if self.function.requires_params:
            function_name = f'{self.function.template_sql}({self.function.expressions[0]})'
        else:
            function_name = f'{self.function.template_sql}()'

        return self.template_sql.format(**{
            'function_name': function_name,
            'over_clause': self.function.as_sql(backend)
        })


class WindowFunctionMixin:
    """
    Args:
        *expressions (str | Functions): One or more field names or function instances to be
    """

    over_clause: ClassVar[str] = 'over ({conditions})'
    requires_params: bool = False

    def __init__(self, *expressions: str):
        self.expressions = list(expressions)
        self.takes_partition = None

        names = []
        for expression in expressions:
            if isinstance(expression, (str, int)):
                names.append(expression)

            if isinstance(expression, Functions):
                names.append(expression.field_name)

        compound_name = '_'.join(names)
        field_name = f'{self.__class__.__name__.lower()}_{compound_name}'
        super().__init__(field_name)

    @property
    def internal_type(self):
        return 'window_function'

    def as_sql(self, backend: TypeSQLiteBackend):
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
            if hasattr(self.takes_partition, 'internal_type'):
                if self.takes_partition.internal_type == 'expression':
                    partition_clause = backend.comma_join(
                        self.takes_partition.as_sql(backend)
                    )
            over_clause.insert(0, f'partition by {partition_clause}')

        return self.over_clause.format(conditions=backend.simple_join(over_clause))


class Rank(WindowFunctionMixin, Functions):
    """This window function assigns a rank to each row
    within the result set of a query. The rank of a row is
    determined by adding one to the number of preceding
    rows with ranks before it

    >>> table.objects.annotate(age_rank=Window(function=Rank('age')))
    ... table.objects.annotate(age_rank=Window(function=Rank(F('age'))))

    Args:
        field_name (str): The name of the field to rank by.
    """

    template_sql: ClassVar[str] = 'rank'


class PercentRank(WindowFunctionMixin, Functions):
    """The PERCENT_RANK() is a window function that calculates
    the percent rank of a given row using this formula :

    `(r - 1) / (the number of rows in the window or partition - r)`

    >>> table.objects.annotate(pct_rank=Window(function=PercentRank('age')))
    ... table.objects.annotate(pct_rank=Window(function=PercentRank(F('age'))))

    Args:
        field_name (str): The name of the field to calculate percent rank on.
    """

    template_sql: ClassVar[str] = 'percent_rank'


class DenseRank(WindowFunctionMixin, Functions):
    """The DENSE_RANK() is a window function that computes
    the rank of a row in an ordered set of rows and returns
    the rank as an integer. The ranks are consecutive integers
    starting from 1. Rows with equal values receive the
    same rank. And rank values are not skipped in case
    of ties.

    >>> table.objects.annotate(dense_rank=Window(function=DenseRank('age')))
    ... table.objects.annotate(dense_rank=Window(function=DenseRank(F('age'))
    """
    template_sql: ClassVar[str] = 'dense_rank'


class CumeDist(WindowFunctionMixin, Functions):
    """The CUME_DIST() is a window function that calculates
    the cumulative distribution of a value in a group of values.
    It represents the relative position of a value within
    a dataset as a value between 0 and 1.

    >>> table.objects.annotate(cume_dist=Window(function=CumeDist('age')))
    ... table.objects.annotate(cume_dist=Window(function=CumeDist(F('age'))))
    """

    template_sql: ClassVar[str] = 'cume_dist'


class FirstValue(WindowFunctionMixin, Functions):
    """The FIRST_VALUE() is a window function that retrieves
    the first value in an ordered set of values.

    >>> table.objects.annotate(first_value=Window(function=FirstValue('age')))
    ... table.objects.annotate(first_value=Window(function=FirstValue(F('age'))))
    """
    template_sql: ClassVar[str] = 'first_value'
    requires_params: Final[bool] = True

    def __init__(self, expressions: str):
        super().__init__(expressions)


class LastValue(WindowFunctionMixin, Functions):
    """The LAST_VALUE() is a window function that retrieves
    the last value in an ordered set of values.

    >>> table.objects.annotate(last_value=Window(function=LastValue('age')))
    ... table.objects.annotate(last_value=Window(function=LastValue(F('age'))))
    """

    template_sql: ClassVar[str] = 'last_value'
    requires_params: Final[bool] = True

    def __init__(self, expressions: str):
        super().__init__(expressions)


class NthValue(WindowFunctionMixin, Functions):
    template_sql: ClassVar[str] = 'nth_value'


class Lag(WindowFunctionMixin, Functions):
    template_sql: ClassVar[str] = 'lag'


class Lead(WindowFunctionMixin, Functions):
    template_sql: ClassVar[str] = 'lead'


class NTile(WindowFunctionMixin, Functions):
    """The NTILE() is a window function that divides the ordered 
    result set into a specified number of approximately equal
    groups, or "tiles", and assigns a tile number to each row.

    >>> table.objects.annotate(ntile=Window(function=NTile(4)))
    ... table.objects.annotate(ntile=Window(function=NTile(F(4))))
    """

    template_sql: ClassVar[str] = 'ntile'
    requires_params: Final[bool] = True

    def __init__(self, expressions: int):
        super().__init__(expressions)


class RowNumber(WindowFunctionMixin, Functions):
    template_sql: ClassVar[str] = 'row_number'
