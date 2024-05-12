from collections.abc import Callable
from typing import Any, Literal

from lorelie.backends import SQLiteBackend


class Functions:
    field_name: str = ...
    backend: SQLiteBackend = ...
    custom_sql: str = ...
    allow_aggregration: bool = Literal[False]

    def __init__(self, field_name: str) -> None: ...
    def __str__(self) -> str: ...

    @staticmethod
    def create_function() -> Callable[[int, float, str], Any]: ...

    def as_sql(self, backend: SQLiteBackend) -> list[str]: ...


class Lower(Functions):
    ...


class Upper(Lower):
    ...


class Length(Functions):
    ...


class Max(Functions):
    ...


class Min(Functions):
    ...


class ExtractDatePartsMixin(Functions):
    date_part: str = ...


class ExtractYear(ExtractDatePartsMixin):
    ...


class Hash(ExtractDatePartsMixin):
    ...


class MD5Hash(Functions):
    ...
