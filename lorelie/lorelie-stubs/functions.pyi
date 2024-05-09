from collections.abc import Callable
from lorelie.backends import SQLiteBackend


class Functions:
    field_name: str = ...
    backend: SQLiteBackend = ...
    custom_sql: str = ...

    def __init__(self, field_name: str) -> None: ...
    def __str__(self) -> str: ...

    @staticmethod
    def create_function() -> Callable: ...

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


class ExtractYear(Functions):
    ...


class Count(Functions):
    ...


class Hash(Functions):
    ...
