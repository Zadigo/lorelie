import re
import hashlib
from sqlite3 import Connection
from typing import ClassVar

from lorelie.database.functions.base import Functions
from lorelie.lorelie_typings import TypeSQLiteBackend

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

    >>> db.objects.annotate(lowered_name=Lower('name'))
    """

    template_sql: ClassVar[str] = 'lower({field})'

    def as_sql(self, backend: TypeSQLiteBackend):
        return self.template_sql.format_map({
            'field': self.field_name
        })


class Upper(Lower):
    """Returns each values of the given
    column in uppercase

    >>> db.objects.annotate(uppered_name=Upper('name'))
    """

    template_sql: ClassVar[str] = 'upper({field})'

    def as_sql(self, backend: TypeSQLiteBackend):
        return self.template_sql.format_map({
            'field': self.field_name
        })


class Length(Functions):
    """Function is used to return the length 
    of a string expression in the selected 
    database items

    >>> db.objects.annotate(name_length=Length('url'))
    """

    template_sql: ClassVar[str] = 'length({field})'

    def as_sql(self, backend: TypeSQLiteBackend):
        return self.template_sql.format_map({
            'field': self.field_name
        })


class MD5Hash(Functions):
    """Function used to generate an MD5 hash of a given string

    A MD5 hash is a one-way cryptographic function that produces
    a fixed-size 128-bit (16-byte) hash value from input data of any size

    >>> db.objects.annotate(md5_hashed=MD5Hash('name'))
    """
    template_sql: ClassVar[str] = 'hash({field})'

    @staticmethod
    def create_function(connection: Connection):
        def callback(text: str):
            text = str(text).encode('utf-8')
            return hashlib.md5(text).hexdigest()
        connection.create_function('hash', 1, callback)

    def as_sql(self, backend: TypeSQLiteBackend):
        return self.template_sql.format_map({
            'field': self.field_name
        })


class SHA1Hash(MD5Hash):
    """Function used to generate a SHA1 hash of a given string.

    A SHA1 hash is a one-way cryptographic function that produces
    a fixed-size 160-bit (20-byte) hash value from input data of any size.
    """
    template_sql: ClassVar[str] = 'sha1({field})'

    @staticmethod
    def create_function(connection: Connection):
        def callback(text):
            text = str(text).encode('utf-8')
            return hashlib.sha1(text).hexdigest()
        connection.create_function('sha1', 1, callback)


class SHA224Hash(MD5Hash):
    """Function used to generate a SHA224 hash of a given string.

    A SHA224 hash is a one-way cryptographic function that produces
    a fixed-size 224-bit (28-byte) hash value from input data of any size.

    >>> db.objects.annotate(sha224_hashed=SHA224Hash('name'))
    """

    template_sql: ClassVar[str] = 'sha224({field})'

    @staticmethod
    def create_function(connection: Connection):
        def callback(text):
            text = str(text).encode('utf-8')
            return hashlib.sha224(text).hexdigest()
        connection.create_function('sha224', 1, callback)


class SHA256Hash(MD5Hash):
    """Function used to generate a SHA256 hash of a given string.

    A SHA256 hash is a one-way cryptographic function that produces
    a fixed-size 256-bit (32-byte) hash value from input data of any size.
    """

    template_sql: ClassVar[str] = 'sha256({field})'

    @staticmethod
    def create_function(connection: Connection):
        def callback(text):
            text = str(text).encode('utf-8')
            return hashlib.sha256(text).hexdigest()
        connection.create_function('sha256', 1, callback)


class SHA384Hash(MD5Hash):
    template_sql: ClassVar[str] = 'sha384({field})'

    @staticmethod
    def create_function(connection: Connection):
        def callback(text):
            text = str(text).encode('utf-8')
            return hashlib.sha384(text).hexdigest()
        connection.create_function('sha384', 1, callback)


class SHA512Hash(MD5Hash):
    template_sql: ClassVar[str] = 'sha512({field})'

    @staticmethod
    def create_function(connection: Connection):
        def callback(text):
            text = str(text).encode('utf-8')
            return hashlib.sha512(text).hexdigest()
        connection.create_function('sha512', 1, callback)


class Trim(Functions):
    """Function used to remove leading and trailing
    spaces from a given string

    >>> db.objects.annotate(trimmed_name=Trim('name'))
    """

    template_sql: ClassVar[str] = 'trim({field})'

    def as_sql(self, backend: TypeSQLiteBackend):
        return self.template_sql.format(field=self.field_name)


class LTrim(Trim):
    """Function used to remove leading
    spaces from a given string

    >>> db.objects.annotate(ltrimmed_name=LTrim('name'))
    """

    template_sql: ClassVar[str] = 'ltrim({field})'


class RTrim(Trim):
    """Function used to remove trailing
    spaces from a given string

    >>> db.objects.annotate(rtrimmed_name=RTrim('name'))
    """

    template_sql: ClassVar[str] = 'rtrim({field})'


class SubStr(Functions):
    """Function used to return a substring
    from a given string based on the
    specified starting position and length
    >>> db.objects.annotate(substring_name=SubStr('name', 1, 3))
    """

    template_sql: ClassVar[str] = 'substr({field}, {start}, {end})'

    def __init__(self, field_name, start, end):
        self.start = start
        self.end = end
        super().__init__(field_name)

    def as_sql(self, backend: TypeSQLiteBackend):
        return self.template_sql.format_map({
            'field': self.field_name,
            'start': self.start,
            'end': self.end
        })


class Concat(Functions):
    """Function used to concatenate multiple
    string fields into a single string.
    >>> db.objects.annotate(full_name=Concat('first_name', 'last_name'))
    """

    template_sql: ClassVar[str] = 'concat({fields})'

    def __init__(self, *fields: str):
        self.fields = list(fields)
        # TODO: Will raise an error
        super().__init__()

    @property
    def alias_field_name(self):
        return None

    def as_sql(self, backend: TypeSQLiteBackend):
        return backend.comma_join(self.fields)


class RegexSearch(Functions):
    template_sql: ClassVar[str] = 'regexp({field})'

    @staticmethod
    def create_function(connection: Connection):
        def callback(pattern, text):
            return 1 if re.search(pattern, text) else 0
        connection.create_function('regexp', 2, callback)

# Cast,
# Coalesce,
# Collate,
# Greatest,
# JSONObject,
# Least,
# NullIf
