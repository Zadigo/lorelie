from typing import Union, override

from lorelie.backends import SQLiteBackend
from lorelie.functions import Functions
from lorelie.queries import QuerySet

class MathMixin(Functions):
    @property
    def aggregate_name(self) -> str: ...

    def python_aggregation(self, values: list[int]) -> Union[int, float]: ...

    def use_queryset(
        self,
        field: str,
        queryset: QuerySet
    ) -> Union[int, float]: ...


class Count(MathMixin, Functions):
    @override
    def python_aggregation(self, values: list[int]) -> int: ...
    @override
    def as_sql(self, backend: SQLiteBackend) -> str: ...


class Avg(MathMixin, Functions):
    @override
    def python_aggregation(self, values: list[int]) -> float: ...
    @override
    def as_sql(self, backend: SQLiteBackend) -> str: ...
