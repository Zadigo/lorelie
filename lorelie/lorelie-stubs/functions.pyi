from collections.abc import Callable
from typing import Any, Literal, override

from lorelie.backends import SQLiteBackend


class Functions:
    field_name: str = ...
    backend: SQLiteBackend = ...
    template_sql: str = ...
    allow_aggregration: bool = Literal[False]

    def __init__(self, field_name: str) -> None: ...
    def __str__(self) -> str: ...

    @property
    def alias_field_name(self) -> str: ...

    @staticmethod
    def create_function() -> Callable[[int, float, str], Any]: ...

    def as_sql(self, backend: SQLiteBackend) -> list[str]: ...


class Lower(Functions):
    ...


class Upper(Lower):
    ...


class Length(Functions):
    ...


class ExtractDatePartsMixin(Functions):
    date_part: str = ...


class ExtractYear(ExtractDatePartsMixin):
    ...


class ExtractMonth(ExtractDatePartsMixin):
    ...


class ExtractDay(ExtractDatePartsMixin):
    ...


class ExtractHour(ExtractDatePartsMixin):
    ...


class ExtractMinute(ExtractDatePartsMixin):
    ...


class MD5Hash(Functions):
    ...


class SHA256Hash(Functions):
    ...


class Trim(Functions):
    ...


class LTrim(Trim):
    ...


class RTrim(Trim):
    ...


class SubStr(Functions):
    @override
    def __init__(self, field_name: str, start: int, end: int) -> None: ...


class Concat(Functions):
    fields: list[str] = ...

    @override
    def __init__(self, *fields: list) -> None: ...
