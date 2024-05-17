import hashlib


class Functions:
    template_sql = None
    allow_aggregration = False

    def __init__(self, field_name):
        self.field_name = field_name
        self.backend = None

    def __str__(self):
        return f'<{self.__class__.__name__}({self.field_name})>'

    @property
    def alias_field_name(self):
        """Potential alias name that can be used
        if this function is not used via an 
        explicit alias"""
        return f'{self.__class__.__name__.lower()}_{self.field_name}'

    @staticmethod
    def create_function(connection):
        """Use this function to register a local
        none existing function in the database
        function space in other to use none
        conventional functions"""
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
    def create_function(connection):
        def callback(text):
            text = str(text).encode('utf-8')
            return hashlib.md5(text).hexdigest()
        connection.create_function('hash', 1, callback)

    def as_sql(self, backend):
        return self.template_sql.format_map({
            'field': self.field_name
        })


class SHA256Hash(MD5Hash):
    template_sql = 'sha256({field})'

    @staticmethod
    def create_function(connection):
        def callback(text):
            text = str(text).encode('utf-8')
            return hashlib.sha256(text).hexdigest()
        connection.create_function('sha256', 1, callback)


class Trim(Functions):
    template_sql = 'trim({field})'

    def as_sql(self, backend):
        return self.template_sql.format(field=self.field_name)


class LTrim(Trim):
    template_sql = 'ltrim({field})'


class RTrim(Trim):
    template_sql = 'rtrim({field})'


class SubStr(Functions):
    template_sql = 'substr({field}, {start}, {end})'

    def __init__(self, field_name, start, end):
        self.start = start
        self.end = end
        super().__init__(field_name)

    def as_sql(self, backend):
        return self.template_sql.format_map({
            'field': self.field_name,
            'start': self.start,
            'end': self.end
        })


class Concat(Functions):
    template_sql = 'concat({fields})'

    def __init__(self, *fields):
        self.fields = list(fields)
        super().__init__()

    @property
    def alias_field_name(self):
        return None

    def as_sql(self, backend):
        return backend.comma_join(self.fields)


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
# ,
# ConcatPair,
# Left,
# Length,
# ,
# LPad,
# ,
# Ord,
# Repeat,
# Replace,
# Reverse,
# Right,
# RPad,
# ,
# StrIndex,
# ,
# ,
# ,


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
