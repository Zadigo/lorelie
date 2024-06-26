import hashlib


class Functions:
    template_sql = None
    allow_aggregration = False

    def __init__(self, field_name):
        self.field_name = field_name
        self.backend = None

    def __str__(self):
        return f'<{self.__class__.__name__}({self.field_name})>'

    @staticmethod
    def create_function():
        return NotImplemented

    def as_sql(self, backend):
        return NotImplemented


class Lower(Functions):
    """Returns each values of the given
    column in lowercase

    >>> db.objects.annotate('celebrities', lowered_name=Lower('name'))
    """

    template_sql = 'lower({field})'

    def as_sql(self, backend):
        return self.template_sql.format_map({
            'field': self.field_name
        })


class Upper(Lower):
    """Returns each values of the given
    column in uppercase

    >>> db.objects.annotate('celebrities', uppered_name=Upper('name'))
    """

    template_sql = 'upper({field})'

    def as_sql(self, backend):
        return self.template_sql.format_map({
            'field': self.field_name
        })


class Length(Functions):
    """Function is used to return the length 
    of a string expression in the selected 
    database items

    >>> db.objects.annotate('celebrities', name_length=Length('url'))
    """

    template_sql = 'length({field})'

    def as_sql(self, backend):
        return self.template_sql.format_map({
            'field': self.field_name
        })


class Max(Functions):
    """Returns the max value of a given column

    >>> db.objects.annotate('celebrities',  max_id=Max('id'))
    """

    template_sql = 'max({field})'

    def as_sql(self, backend):
        # SELECT rowid, * FROM seen_urls WHERE rowid = (SELECT max(rowid) FROM seen_urls)
        select_clause = backend.SELECT.format_map({
            'fields': backend.comma_join(['rowid', '*']),
            'table': backend.table.name
        })
        subquery_clause = backend.SELECT.format_map({
            'fields': backend.MAX.format_map({'field': self.field_name}),
            'table': backend.table.name
        })
        where_condition = backend.EQUALITY.format_map({
            'field': self.field_name,
            'value': backend.wrap_parenthentis(subquery_clause)
        })
        where_clause = backend.WHERE_CLAUSE.format_map({
            'params': where_condition
        })
        return backend.simple_join([select_clause, where_clause])


class Min(Functions):
    """Returns the min value of a given column

    >>> db.objects.annotate('celebrities',  min_id=Min('id'))
    """

    template_sql = 'min({field})'

    def as_sql(self, backend):
        select_clause = backend.SELECT.format_map({
            'fields': backend.comma_join(['rowid', '*']),
            'table': backend.table.name
        })
        subquery_clause = backend.SELECT.format_map({
            'fields': backend.MIN.format_map({'field': self.field_name}),
            'table': backend.table.name
        })
        where_condition = backend.EQUALITY.format_map({
            'field': self.field_name,
            'value': backend.wrap_parenthentis(subquery_clause)
        })
        where_clause = backend.WHERE_CLAUSE.format_map({
            'params': where_condition
        })
        return backend.simple_join([select_clause, where_clause])


class ExtractDatePartsMixin(Functions):
    date_part = '%Y'

    def as_sql(self, backend):
        return backend.STRFTIME.format_map({
            'format': backend.quote_value(self.date_part),
            'value': self.field_name
        })


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


class MD5Hash(Functions):
    template_sql = 'hash({field})'

    @staticmethod
    def create_function():
        def callback(text):
            text = str(text).encode('utf-8')
            return hashlib.md5(text).hexdigest()
        return callback

    def as_sql(self, backend):
        return self.template_sql.format_map({
            'field': self.field_name
        })


class SHA256Hash(MD5Hash):
    template_sql = 'sha256({field})'

    @staticmethod
    def create_function():
        def callback(text):
            text = str(text).encode('utf-8')
            return hashlib.sha256(text).hexdigest()
        return callback


# Extract,
# ,
# ,
# ExtractIsoWeekDay,
# ExtractIsoYear,
# ,
# ,
# ExtractQuarter,
# ExtractSecond,
# ExtractWeek,
# ExtractWeekDay,
# ,
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

# ,
# SHA1,
# SHA224,
# ,
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
