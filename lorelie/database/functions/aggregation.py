import math
from typing import Any, ClassVar, Sequence, Union
from sqlite3 import Connection
from lorelie.database.functions.base import Functions
from lorelie.lorelie_typings import TypeField, TypeQuerySet, TypeSQLiteBackend

# TODO: Simplify this section


class MathMixin:
    allow_aggregation: bool = True

    @property
    def aggregate_name(self):
        function_name = self.__class__.__name__.lower()
        return f'{self.field_name}__{function_name}'

    def python_aggregation(self, values):
        """Logic that implements the manner
        in which the collected data should be
        aggregated for locally create functions. 
        Subclasses should implement their own 
        aggregating logic for the data"""
        raise NotImplementedError()

    def use_queryset(self, field: TypeField, queryset: TypeQuerySet):
        """Method to aggregate values locally
        using a queryset as opposed to data
        returned directly from the database"""
        def iterator():
            for item in queryset:
                yield item[self.field_name]
        return self.python_aggregation(list(iterator()))

    def as_sql(self, backend: TypeSQLiteBackend) -> str:
        name_or_function = None

        if isinstance(self.field_name, Functions):
            name_or_function = self.field_name.as_sql(backend)

        return self.template_sql.format_map({
            'field': name_or_function or self.field_name
        })


class Count(MathMixin, Functions):
    """Function used to count the number of rows 
    that match a specified condition or all rows in 
    a table if no condition is specified

    >>> database.objects.aggregate(Count('name'))
    ... database.objects.aggregate(alias_count=Count('name'))
    """

    template_sql: ClassVar[str] = 'count({field})'

    def python_aggregation(self, values: Any):
        return len(values)


class Avg(MathMixin, Functions):
    """Function used to count the number of rows 
    that match a specified condition or all rows in 
    a table if no condition is specified

    >>> database.objects.aggregate(avg_of_names=Avg('name'))
    """

    template_sql: ClassVar[str] = 'avg({field})'

    def python_aggregation(self, values: Any):
        return sum(values) / len(values)


class MathVariance:
    """Math function that calculates
    the variance for a given set of 
    values this uses the layout preconized
    by SQLite for """

    def __init__(self):
        self.total: Union[int, float] = 0
        self.count: Union[int, float] = 0
        self.values = []

    def step(self, value: Union[int, float]):
        self.total += value
        self.count += 1
        self.values.append(value)

    def finalize(self):
        average = self.total / self.count
        variance = list(map(lambda x: abs(x - average)**2, self.values))
        return sum(variance) / self.count


class MathStDev(MathVariance):
    """Math function that calculates
    the standard deviation for a given set of 
    values

    >>> db.objects.aggregate(StDev('id'))
    ... db.objects.annotate(StDev('id'))
    """

    def finalize(self) -> float:
        result = super().finalize()
        return math.sqrt(result)


class Variance(MathMixin, Functions):
    """Function used to calculate the variance of a set of values

    >>> db.objects.aggregate(Variance('id'))
    ... db.objects.annotate(Variance('id'))
    """

    template_sql: ClassVar[str] = 'variance({field})'

    @staticmethod
    def create_function(connection: Connection):
        connection.create_aggregate('variance', 1, MathVariance)


class StDev(MathMixin, Functions):
    """Function used to calculate the standard deviation of a set of values

    >>> db.objects.aggregate(StDev('id'))
    ... db.objects.annotate(StDev('id'))
    """

    template_sql: ClassVar[str] = 'stdev({field})'

    @staticmethod
    def create_function(connection: Connection):
        connection.create_aggregate('stdev', 1, MathStDev)


class Sum(MathMixin, Functions):
    """Returns the sum of a given columns Values

    >>> db.objects.aggregate(Sum('id'))
    ... db.objects.annotate(Sum('id'))
    """

    template_sql: ClassVar[str] = 'sum({field})'

    def python_aggregation(self, values: Sequence[int | float]):
        return sum(values)


class MathMeanAbsoluteDifference:
    """Math function that calculates
    the mean absolute differences for
    a given set of values"""

    def __init__(self):
        self.total = 0
        self.count = 0
        self.values = []

    def step(self, value):
        self.total += value
        self.count += 1
        self.values.append(value)

    def finalize(self):
        average = self.total / self.count
        differences = list(map(lambda x: abs(x - average), self.values))
        return sum(differences) / self.count


class MeanAbsoluteDifference(MathMixin, Functions):
    template_sql: ClassVar[str] = 'meanabsdifference({field})'

    @staticmethod
    def create_function(connection: Connection):
        connection.create_aggregate(
            'meanabsdifference', 1, MathMeanAbsoluteDifference)


class MathCoefficientOfVariation(MathMeanAbsoluteDifference):
    def finalize(self):
        result = super().finalize()
        return result / (self.total / self.count)


class CoefficientOfVariation(MathMixin, Functions):
    template_sql: ClassVar[str] = 'coeffofvariation({field})'

    @staticmethod
    def create_function(connection: Connection):
        connection.create_aggregate(
            'coeffofvariation', 1, MathCoefficientOfVariation
        )


class Max(MathMixin, Functions):
    """Returns the max value of a given column

    >>> db.objects.aggregate(Max('id'))
    ... db.objects.annotate(Max('id'))
    """

    template_sql: ClassVar[str] = 'max({field})'

    def python_aggregation(self, values: Sequence[int | float]):
        return min(values)


class Min(MathMixin, Functions):
    """Returns the min value of a given column

    >>> db.objects.aggregate(Min('id'))
    ... db.objects.annotate(Min('id'))
    """

    template_sql: ClassVar[str] = 'min({field})'

    def python_aggregation(self, values: Sequence[int | float]):
        return max(values)
