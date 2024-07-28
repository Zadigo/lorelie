from dataclasses import dataclass, field
from typing import Dict, Iterator, Union, List, Tuple

from lorelie.backends import SQLiteBackend
from lorelie.tables import Table
from functools import cached_property


class ExpressionFiltersMixin:
    base_filters: Dict[str, str] = ...

    @cached_property
    def list_of_operators(self) -> list[str]: ...

    def is_query_filter(self, value_or_values: Union[list, str]) -> bool: ...

    def translate_operator_from_tokens(
        self,
        tokens: List[str]
    ) -> list[str]: ...

    def translate_operators_from_tokens(
        self,
        tokens: List[List[str]]
    ) -> list[list[str]]: ...

    def decompose_filters_columns(
        self,
        value: Union[str, dict]
    ) -> list[str]: ...

    def decompose_filters_from_string(self, value: str) -> str: ...
    def decompose_filters(self, **kwargs) -> List[Tuple[str]]: ...

    def build_filters(
        self,
        items: List[Tuple[str]],
        space_characters: bool = ...
    ) -> List[str]: ...


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


class ExpressionFilter(ExpressionFiltersMixin):
    parsed_expressions: list[str] = ...
    table: Union[str, Table] = ...
    expressions_maps: list[ExpressionMap] = ...

    def __init__(
        self,
        expression: Union[dict, str, list[Union[list[str], tuple[str]]]],
        table: Union[str, Table] = ...
    ) -> None: ...

    def __str__(self) -> str: ...

    def __getitem__(self, index: int) -> ExpressionMap: ...

    def __iter__(self) -> Iterator[ExpressionMap]: ...

    @staticmethod
    def check_tokens(tokens: list[str]) -> None: ...
