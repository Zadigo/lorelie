from typing import Any, override

from lorelie.backends import SQLiteBackend
from lorelie.expressions import Q

class BaseConstraint:
    template_sql: str = ...
    prefix: str = ...

    def as_sql(self, backend: SQLiteBackend) -> str: ...


class CheckConstraint(BaseConstraint):
    name: str = ...
    condition: Q = ...

    def __init__(self, name: str, condition: Q) -> None: ...
    
    @override
    def as_sql(self, backend: SQLiteBackend) -> str: ...


class UniqueConstraint(BaseConstraint):
    fields: list[str] = ...
    
    def __init__(self, name: str, *, fields: list[str] = ...) -> None: ...

    @override
    def as_sql(self, backend: SQLiteBackend) -> str: ...


class MaxLengthConstraint(CheckConstraint):
    max_length: int = ...

    def __init__(self, fields: list[str] = ...): ...
    def __call__(self, value: Any): ...

    def as_sql(self, backend: SQLiteBackend) -> str: ...
