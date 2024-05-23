import sqlite3
from typing import Union, override

from lorelie.database.functions.base import Functions
from lorelie.queries import QuerySet


class MathMixin:
    allow_aggregation: bool = ...

    @property
    def aggregate_name(self) -> str: ...

    def python_aggregation(
        self,
        values: list[Union[int, float]]
    ) -> Union[int, float]: ...

    def use_queryset(
        self,
        field: str,
        queryset: QuerySet
    ) -> Union[int, float]: ...

    def as_sql(self, backend) -> str: ...


class Count(MathMixin, Functions):
    ...


class Avg(MathMixin, Functions):
    ...


class MathVariance:
    total: int = 0
    count: int = 0
    values: list[Union[int, float]] = ...

    def __init__(self) -> None: ...

    def step(self, value: Union[int, float]) -> None: ...
    def finalize(self) -> Union[int, float]: ...


class MathStDev(MathVariance):
    @override
    def create_function(self, connection: sqlite3.Connection) -> None: ...


class Variance(MathMixin, Functions):
    @override
    def create_function(self, connection: sqlite3.Connection) -> None: ...


class StDev(MathMixin, Functions):
    @override
    def create_function(self, connection: sqlite3.Connection) -> None: ...


class Sum(MathMixin, Functions):
    ...


class MathMeanAbsoluteDifference:
    total: int = 0
    count: int = 0
    values: list[Union[int, float]] = ...

    def __init__(self) -> None: ...

    def step(self, value: Union[int, float]) -> None: ...
    def finalize(self) -> Union[int, float]: ...


class MeanAbsoluteDifference(MathMixin, Functions):
    @override
    def create_function(self, connection: sqlite3.Connection) -> None: ...


class MathCoefficientOfVariation(MathMeanAbsoluteDifference):
    ...


class CoefficientOfVariation(MathMixin, Functions):
    @override
    def create_function(self, connection: sqlite3.Connection) -> None: ...


class Max(MathMixin, Functions):
    ...


class Min(MathMixin, Functions):
    ...
