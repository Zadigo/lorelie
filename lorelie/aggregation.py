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


class Count(MathMixin, Functions):
    """Function used to count the number of rows 
    that match a specified condition or all rows in 
    a table if no condition is specified

    >>> database.objects.annotate('celebrities', count_of_names=Count('name'))
    """

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

    def as_sql(self, backend):
        sql = backend.AVERAGE.format_map({
            'field': self.field_name
        })
        return sql
