import hashlib


class Functions:
    custom_sql = None

    def __init__(self, field_name):
        self.field_name = field_name
        self.backend = None

    def __str__(self):
        return f'<{self.__class__.__name__}({self.field_name})>'

    @staticmethod
    def create_function():
        pass

    def as_sql(self):
        pass


class Lower(Functions):
    """Returns each values of the given
    column in lowercase

    >>> database.objects.annotate('celebrities', lowered_name=Lower('name'))
    """

    def as_sql(self, backend):
        sql = backend.LOWER.format_map({
            'field': self.field_name
        })
        return sql


class Upper(Lower):
    """Returns each values of the given
    column in uppercase

    >>> database.objects.annotate('celebrities', uppered_name=Upper('name'))
    """

    def as_sql(self, backend):
        sql = backend.UPPER.format_map({
            'field': self.field_name
        })
        return sql


class Length(Functions):
    """Function is used to return the length 
    of a string expression in the selected 
    database items

    >>> database.objects.annotate('celebrities', name_length=Length('url'))
    """

    def as_sql(self, backend):
        sql = backend.LENGTH.format_map({
            'field': self.field_name
        })
        return sql


class Max(Functions):
    """Returns the max value of a given column"""

    def as_sql(self):
        # SELECT rowid, * FROM seen_urls WHERE rowid = (SELECT max(rowid) FROM seen_urls)
        select_clause = self.backend.SELECT.format_map({
            'fields': self.backend.comma_join(['rowid', '*']),
            'table': self.backend.table.name
        })
        subquery_clause = self.backend.SELECT.format_map({
            'fields': self.backend.MAX.format_map({'field': self.field_name}),
            'table': self.backend.table.name
        })
        where_condition = self.backend.EQUALITY.format_map({
            'field': self.field_name,
            'value': self.backend.wrap_parenthentis(subquery_clause)
        })
        where_clause = self.backend.WHERE_CLAUSE.format_map({
            'params': where_condition
        })
        return self.backend.simple_join([select_clause, where_clause])


class Min(Functions):
    """Returns the min value of a given column"""

    def as_sql(self):
        select_clause = self.backend.SELECT.format_map({
            'fields': self.backend.comma_join(['rowid', '*']),
            'table': self.backend.table.name
        })
        subquery_clause = self.backend.SELECT.format_map({
            'fields': self.backend.MIN.format_map({'field': self.field_name}),
            'table': self.backend.table.name
        })
        where_condition = self.backend.EQUALITY.format_map({
            'field': self.field_name,
            'value': self.backend.wrap_parenthentis(subquery_clause)
        })
        where_clause = self.backend.WHERE_CLAUSE.format_map({
            'params': where_condition
        })
        return self.backend.simple_join([select_clause, where_clause])


class ExtractDatePartsMixin(Functions):
    date_part = '%Y'

    def as_sql(self, backend):
        sql = backend.STRFTIME.format_map({
            'format': backend.quote_value(self.date_part),
            'value': self.field_name
        })
        return sql


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

    >>> db.objects.annotate('celebrities', day=ExtractMonth('date_of_birth'))

    Or filter data based on the return value of the function

    >>> db.objects.filter('celebrities', day__gte=ExtractMonth('date_of_birth'))
    """
    date_part = '%d'


class Hash(Functions):
    custom_sql = 'Hash({field})'

    @staticmethod
    def create_function():
        def callback(text):
            text = str(text).encode('utf-8')
            return hashlib.md5(text).hexdigest()
        return callback

    def as_sql(self):
        sql = self.custom_sql.format_map({
            'field': self.field_name
        })
        return sql


# Extract,
# ExtractDay,
# ExtractHour,
# ExtractIsoWeekDay,
# ExtractIsoYear,
# ExtractMinute,
# ExtractMonth,
# ExtractQuarter,
# ExtractSecond,
# ExtractWeek,
# ExtractWeekDay,
# ExtractYear,
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

# Abs,
# ACos,
# ASin,
# ATan,
# ATan2,
# Ceil,
# Cos,
# Cot,
# Degrees,
# Exp,
# Floor,
# Ln,
# Log,
# Mod,
# Pi,
# Power,
# Radians,
# Random,
# Round,
# Sign,
# Sin,
# Sqrt,
# Tan

# MD5,
# SHA1,
# SHA224,
# SHA256,
# SHA384,
# SHA512,
# Chr,
# Concat,
# ConcatPair,
# Left,
# Length,
# Lower,
# LPad,
# LTrim,
# Ord,
# Repeat,
# Replace,
# Reverse,
# Right,
# RPad,
# RTrim,
# StrIndex,
# Substr,
# Trim,
# Upper,


# CumeDist,
# DenseRank,
# FirstValue,
# Lag,
# LastValue,
# Lead,
# NthValue,
# Ntile,
# PercentRank,
# Rank,
# RowNumber

# Cast,
# Coalesce,
# Collate,
# Greatest,
# JSONObject,
# Least,
# NullIf
