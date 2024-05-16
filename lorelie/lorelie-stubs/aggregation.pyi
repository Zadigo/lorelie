from typing import Union, override

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


class Avg(MathMixin, Functions):
    @override
    def python_aggregation(self, values: list[int]) -> Union[int, float]: ...


class Variance(MathMixin, Functions):
    @override
    def python_aggregation(self, values: list[int]) -> Union[int, float]: ...


class StDev(MathMixin, Functions):
    @override
    def python_aggregation(self, values: list[int]) -> Union[int, float]: ...


class Sum(MathMixin, Functions):
    @override
    def python_aggregation(self, values: list[int]) -> Union[int, float]: ...


class MeanAbsoluteDifference(MathMixin, Functions):
    @override
    def python_aggregation(self, values: list[int]) -> Union[int, float]: ...


class CoefficientOfVariation(MathMixin, Functions):
    @override
    def python_aggregation(self, values: list[int]) -> Union[int, float]: ...


class Max(Functions):
    ...


class Min(Functions):
    ...
