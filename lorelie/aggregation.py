# "Aggregate",
#     "Avg",
#     "Count",
#     "Max",
#     "Min",
#     "StdDev",
#     "Sum",
#     "Variance"
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
        sql = backend.COUNT.format_map({
            'field': self.field_name
        })
        return sql


class Avg(MathMixin, Functions):
    """Function used to count the number of rows 
    that match a specified condition or all rows in 
    a table if no condition is specified

    >>> database.objects.annotate('celebrities', count_of_names=Count('name'))
    """

    def python_aggregation(self, values):
        number_of_items = len(values)
        return sum(values) / number_of_items

    def as_sql(self, backend):
        sql = backend.AVERAGE.format_map({
            'field': self.field_name
        })
        return sql
