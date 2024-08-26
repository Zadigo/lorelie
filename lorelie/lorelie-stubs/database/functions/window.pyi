from typing import Union, override

from backends import SQLiteBackend
from expressions import F, Q

from lorelie.database.functions.base import Functions


class Window(Functions):
    function: str = ...
    order_by: str = ...

    def __init__(
        self,
        function: Union[Rank, PercentRank, CumeDist, DenseRank, LastValue, Lag, NthValue, NTile, Lead, RowNumber],
        partition_by: Union[str, F] = ...,
        order_by: Union[str, F] = ...
    ) -> None: ...

    @override
    def as_sql(self, backend: SQLiteBackend) -> str: ...


class WindowFunctionMixin:
    expressions: list[Q] = ...
    takes_partition: Union[str, F]

    def __init__(self, *expressions: Union[str, F]) -> None: ...


class Rank(WindowFunctionMixin, Functions):
    ...


class PercentRank(WindowFunctionMixin, Functions):
    ...


class DenseRank(WindowFunctionMixin, Functions):
    ...


class CumeDist(WindowFunctionMixin, Functions):
    ...


class FirstValue(WindowFunctionMixin, Functions):
    ...


class LastValue(WindowFunctionMixin, Functions):
    ...


class NthValue(WindowFunctionMixin, Functions):
    ...


class Lag(WindowFunctionMixin, Functions):
    ...


class Lead(WindowFunctionMixin, Functions):
    ...


class NTile(WindowFunctionMixin, Functions):
    ...


class RowNumber(WindowFunctionMixin, Functions):
    ...
