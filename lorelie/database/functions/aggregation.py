import math

from lorelie.database.functions.base import Functions

# TODO: Simplify this section


class MathMixin:
    allow_aggregation = True

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
        return NotImplemented

    def use_queryset(self, field, queryset):
        """Method to aggregate values locally
        using a queryset as opposed to data
        returned directly from the database"""
        def iterator():
            for item in queryset:
                yield item[self.field_name]
        return self.python_aggregation(list(iterator()))

    def as_sql(self, backend):
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

    >>> database.objects.aggregate('celebrities', Count('name'))
    ... database.objects.aggregate('celebrities', alias_count=Count('name'))
    """

    template_sql = 'count({field})'


class Avg(MathMixin, Functions):
    """Function used to count the number of rows 
    that match a specified condition or all rows in 
    a table if no condition is specified

    >>> database.objects.aggregate('celebrities', avg_of_names=Avg('name'))
    """

    template_sql = 'avg({field})'


class MathVariance:
    """Math function that calculates
    the variance for a given set of 
    values this uses the layout preconized
    by SQLite for """

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
        variance = list(map(lambda x: abs(x - average)**2, self.values))
        return sum(variance) / self.count


class MathStDev(MathVariance):
    def finalize(self):
        result = super().finalize()
        return math.sqrt(result)


class Variance(MathMixin, Functions):
    template_sql = 'variance({field})'

    @staticmethod
    def create_function(connection):
        connection.create_aggregate('variance', 1, MathVariance)


class StDev(MathMixin, Functions):
    template_sql = 'stdev({field})'

    @staticmethod
    def create_function(connection):
        connection.create_aggregate('stdev', 1, MathStDev)


class Sum(MathMixin, Functions):
    template_sql = 'sum({field})'


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
    template_sql = 'meanabsdifference({field})'

    @staticmethod
    def create_function(connection):
        connection.create_aggregate(
            'meanabsdifference', 1, MathMeanAbsoluteDifference)


class MathCoefficientOfVariation(MathMeanAbsoluteDifference):
    def finalize(self):
        result = super().finalize()
        return result / (self.total / self.count)


class CoefficientOfVariation(MathMixin, Functions):
    template_sql = 'coeffofvariation({field})'

    @staticmethod
    def create_function(connection):
        connection.create_aggregate(
            'coeffofvariation', 1, MathCoefficientOfVariation
        )


class Max(MathMixin, Functions):
    """Returns the max value of a given column

    >>> db.objects.aggregate('celebrities',  Max('id'))
    ... db.objects.annotate('celebrities',  Max('id'))
    """

    template_sql = 'max({field})'


class Min(MathMixin, Functions):
    """Returns the min value of a given column

    >>> db.objects.aggregate('celebrities', Min('id'))
    ... db.objects.annotate('celebrities',  Min('id'))
    """

    template_sql = 'min({field})'
