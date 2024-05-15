import math
from lorelie.functions import Functions


class MathMixin(Functions):
    @property
    def aggregate_name(self):
        function_name = self.__class__.__name__.lower()
        return f'{self.field_name}__{function_name}'

    def python_aggregation(self, values):
        """Logic that implements that manner
        in which the collected data should be
        aggregated. Subclasses should implement
        their own aggregating logic"""
        pass

    def use_queryset(self, field, queryset):
        """Method to aggregate values locally
        using the Math aggregation functions"""
        def iterator():
            for item in queryset:
                yield item[self.field_name]
        return self.python_aggregation(list(iterator()))


class Count(MathMixin, Functions):
    """Function used to count the number of rows 
    that match a specified condition or all rows in 
    a table if no condition is specified

    >>> database.objects.annotate('celebrities', count_of_names=Count('name'))
    """

    def python_aggregation(self, values):
        return len(values)

    def as_sql(self, backend):
        return backend.COUNT.format_map({
            'field': self.field_name
        })


class Avg(MathMixin, Functions):
    """Function used to count the number of rows 
    that match a specified condition or all rows in 
    a table if no condition is specified

    >>> database.objects.annotate('celebrities', count_of_names=Count('name'))
    """

    template_sql = 'avg({field})'

    def python_aggregation(self, values):
        number_of_items = len(values)
        return sum(values) / number_of_items

    def as_sql(self, backend):
        return backend.AVERAGE.format_map({
            'field': self.field_name
        })


class Variance(MathMixin, Functions):
    template_sql = 'variance({field})'

    def python_aggregation(self, values):
        average_instance = Avg(self.field_name)
        count_instance = Count(self.field_name)

        average_total = average_instance.python_aggregation(values)
        count_total = count_instance.python_aggregation(values)

        variance = list(map(lambda x: (x - average_total)**2, values))
        return sum(variance) / count_total


class StDev(MathMixin, Functions):
    template_sql = 'stdev({field})'

    @staticmethod
    def create_function():
        return

    def python_aggregation(self, values):
        variance_instance = Variance(self.field_name)
        return math.sqrt(variance_instance.python_aggregation(values))


class Sum(MathMixin, Functions):
    template_sql = 'sum({field})'

    def python_aggregation(self, values):
        return sum(values)

    def as_sql(self, backend):
        return self.template_sql.format_map({
            'field': self.field_name
        })


class MeanAbsoluteDifference(MathMixin, Functions):
    template_sql = 'meanabsdifference({field})'

    def python_aggregation(self, values):
        average_instance = Avg(self.field_name)
        count_instance = Count(self.field_name)

        average = average_instance.python_aggregation(values)
        differences = list(map(lambda x: abs(x - average), values))
        count_total = count_instance.python_aggregation(differences)
        return count_total


class CoefficientOfVariation(MathMixin, Functions):
    template_sql = 'coeffofvariation({field})'

    def python_aggregation(self, values):
        stdev_instance = StDev(self.field_name)
        average_instance = Avg(self.field_name)
        return (
            stdev_instance.python_aggregation(values) /
            average_instance.python_aggregation(values)
        )
