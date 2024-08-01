from lorelie.database.functions.base import Functions


# ExtractIsoWeekDay,
# ExtractIsoYear,
# ExtractQuarter,
# ExtractSecond,
# ExtractWeek,
# ExtractWeekDay,
# Now,
# Trunc,
# TruncDate,
# TruncDay,
# TruncHour,
# TruncMinute,
# TruncMonth,
# TruncQuarter,
# TruncSecond,
# TruncTime,
# TruncWeek,
# TruncYear,

class ExtractDatePartsMixin(Functions):
    date_part = '%Y'

    def as_sql(self, backend):
        return backend.STRFTIME.format_map({
            'format': backend.quote_value(self.date_part),
            'value': self.field_name
        })
    

class Extract(ExtractDatePartsMixin):
    """Extracts the part of a given date

    >>> db.objects.annotate('celebrities', year=Extract('date_of_birth', 'year'))

    Or filter data based on the return value of the function

    >>> db.objects.filter('celebrities', year__gte=Extract('date_of_birth', 'year))
    """
    def __init__(self, field_name, part):
        super().__init__(field_name)

        parts = {
            'year': '%Y',
            'month': '%m',
            'date': '%d'
        }

        result = parts.get(part)
        if result is None:
            raise ValueError(f"{part} is not a valid date part")
        self.date_part = result


class ExtractYear(ExtractDatePartsMixin):
    """Extracts the year section of each
    iterated value:

    >>> db.objects.annotate('celebrities', year=ExtractYear('date_of_birth'))

    Or filter data based on the return value of the function

    >>> db.objects.filter('celebrities', year__gte=ExtractYear('date_of_birth'))
    """


class ExtractMonth(ExtractDatePartsMixin):
    """Extracts the month section of each
    iterated value:

    >>> db.objects.annotate('celebrities', month=ExtractMonth('date_of_birth'))

    Or filter data based on the return value of the function

    >>> db.objects.filter('celebrities', month__gte=ExtractMonth('date_of_birth'))
    """
    date_part = '%m'


class ExtractDay(ExtractDatePartsMixin):
    """Extracts the day section of each
    iterated value:

    >>> db.objects.annotate('celebrities', day=ExtractDay('date_of_birth'))

    Or filter data based on the return value of the function

    >>> db.objects.filter('celebrities', day__gte=ExtractDay('date_of_birth'))
    """
    date_part = '%d'


class ExtractHour(ExtractDatePartsMixin):
    """Extracts the day section of each
    iterated value:

    >>> db.objects.annotate('celebrities', day=ExtractHour('date_of_birth'))

    Or filter data based on the return value of the function

    >>> db.objects.filter('celebrities', day__gte=ExtractHour('date_of_birth'))
    """
    date_part = '%H'


class ExtractMinute(ExtractDatePartsMixin):
    """Extracts the day section of each
    iterated value:

    >>> db.objects.annotate('celebrities', day=ExtractMinute('date_of_birth'))

    Or filter data based on the return value of the function

    >>> db.objects.filter('celebrities', day__gte=ExtractMinute('date_of_birth'))
    """
    date_part = '%M'
