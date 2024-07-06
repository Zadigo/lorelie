from dataclasses import dataclass, field
from typing import Iterator, Union

from lorelie.tables import Table


@dataclass
class ExpressionMap:
    column: str = None
    columns: list = field(default_factory=list)
    operator: str = None
    value: Union[str, list, int, float, dict] = None
    tokens: list = field(default_factory=list)

    def __post_init__(self) -> None: ...

    @property
    def expands_foreign_key(self) -> bool: ...

    def __hash__(self) -> int: ...


class ExpressionFilter:
    expressions: list[str] = ...
    table: Union[str, Table] = ...
    expressions_maps: list[ExpressionMap] = ...

    def __init__(
        self,
        expression: Union[dict, str],
        table: Union[str, Table] = ...
    ) -> None: ...

    def __str__(self) -> str: ...

    def __getitem__(self, index: int) -> ExpressionMap: ...

    def __iter__(self) -> Iterator[ExpressionMap]: ...

    @staticmethod
    def check_tokens(tokens: list[str]) -> None: ...
