import hashlib

from lorelie.database.functions.base import Functions

# Chr,
# ConcatPair,
# Left,
# Length,
# LPad,
# Ord,
# Repeat,
# Replace,
# Reverse,
# Right,
# RPad,
# StrIndex,


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


class SHA1Hash(MD5Hash):
    template_sql = 'sha1({field})'

    def create_function(self, connection):
        def callback(text):
            text = str(text).encode('utf-8')
            return hashlib.sha1(text).hexdigest()
        connection.create_function('sha1', 1, callback)


class SHA224Hash(MD5Hash):
    template_sql = 'sha224({field})'

    def create_function(self, connection):
        def callback(text):
            text = str(text).encode('utf-8')
            return hashlib.sha224(text).hexdigest()
        connection.create_function('sha224', 1, callback)


class SHA256Hash(MD5Hash):
    template_sql = 'sha256({field})'

    @staticmethod
    def create_function(connection):
        def callback(text):
            text = str(text).encode('utf-8')
            return hashlib.sha256(text).hexdigest()
        connection.create_function('sha256', 1, callback)


class SHA384Hash(MD5Hash):
    template_sql = 'sha384({field})'

    @staticmethod
    def create_function(connection):
        def callback(text):
            text = str(text).encode('utf-8')
            return hashlib.sha384(text).hexdigest()
        connection.create_function('sha384', 1, callback)


class SHA512Hash(MD5Hash):
    template_sql = 'sha512({field})'

    @staticmethod
    def create_function(connection):
        def callback(text):
            text = str(text).encode('utf-8')
            return hashlib.sha512(text).hexdigest()
        connection.create_function('sha512', 1, callback)


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
        # TODO: Will raise an error
        super().__init__()

    @property
    def alias_field_name(self):
        return None

    def as_sql(self, backend):
        return backend.comma_join(self.fields)


# Cast,
# Coalesce,
# Collate,
# Greatest,
# JSONObject,
# Least,
# NullIf
