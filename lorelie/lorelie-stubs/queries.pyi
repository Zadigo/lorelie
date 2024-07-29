from functools import total_ordering
from typing import Any, Generator, List, Literal, Optional, Type, Union

import pandas

from lorelie.backends import BaseRow, SQLiteBackend
from lorelie.database.nodes import BaseNode, SelectMap
from lorelie.tables import Table


class Query:
    table: Table = ...
    backend: SQLiteBackend = ...
    sql: str = ...
    result_cache: list[BaseRow] = ...
    alias_fields: list[str] = ...
    is_evaluated: bool = Literal[False]
    statements: list[Union[BaseNode, str]] = ...
    select_map: SelectMap = ...

    def __init__(
        self,
        table: Optional[Table] = ...,
        backend: Optional[SQLiteBackend] = ...
    ) -> None: ...

    def __repr__(self) -> str: ...

    # @classmethod
    # def run_multiple(
    #     cls: Type[Query],
    #     backend: SQLiteBackend,
    #     *sqls: str,
    #     **kwargs
    # ) -> Generator[Query]: ...

    @classmethod
    def create(
        cls: Type[Query],
        sql_tokens: Optional[List[str]] = ...,
        table: Optional[Table] = ...,
        backend: Optional[SQLiteBackend] = ...,
    ) -> Query: ...

    @classmethod
    def run_script(
        cls: Type[Query],
        backend: Optional[SQLiteBackend] = ...,
        table: Optional[Table] = ...,
        sql_tokens: Optional[List[str]] = ...
    ) -> Union[Query, bool]: ...

    @property
    def return_single_item(self) -> BaseRow: ...

    def add_sql_node(self, node: Union[BaseNode, str]) -> None: ...
    def add_sql_nodes(self, nodes: list[Union[BaseNode, str]]) -> None: ...

    def pre_sql_setup(self) -> None: ...
    def run(self, commit: Optional[bool] = ...) -> None: ...
    def transform_to_python(self) -> None: ...


class ValuesIterable:
    queryset: QuerySet = ...
    fields: list[str] = ...

    def __init__(
        self,
        queryset: QuerySet,
        fields: list[str] = ...
    ) -> None: ...

    def __iter__(self) -> list[dict[str, Any]]: ...


@total_ordering
class EmptyQuerySet:
    def __repr__(self) -> str: ...
    def __len__(self) -> Literal[0]: ...
    def __contains__(self) -> Literal[False]: ...
    def __iter__(self) -> list: ...
    def __eq__(self) -> Literal[False]: ...
    def __gt__(self) -> Literal[False]: ...
    def __gte__(self) -> Literal[False]: ...


class QuerySet:
    query: Query = ...
    result_cache: list[BaseRow] = ...
    skip_transform: Optional[bool] = ...
    use_commit: bool = ...
    alias_view_name: str = ...
    force_reload_cache: bool = ...

    def __init__(
        self, 
        query: Query,
        skip_transform: Optional[bool] = ...
    ) -> None: ...

    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...
    def __getitem__(self, index: int) -> BaseRow: ...
    def __iter__(self) -> Generator[BaseRow]: ...

    @property
    def sql_statement(self) -> str: ...

    def check_alias_view_name(self) -> bool: ...
    def load_cache(self) -> None: ...
    def first(self) -> BaseRow: ...
    def last(self) -> BaseRow: ...
    def all(self) -> QuerySet[BaseRow]: ...
    def filter(self, *args, **kwargs) -> QuerySet[BaseRow]: ...
    def get(self, *args, **kwargs) -> BaseRow: ...
    def annotate(self, **kwargs) -> QuerySet[BaseRow]: ...
    def values(self, *fields: str) -> ValuesIterable: ...
    def dataframe(self, *fields: str) -> pandas.DataFrame: ...
    def aggregate(self, *args, **kwargs) -> dict[str, int]: ...
    def count(self) -> int: ...
    def exclude(self, **kwargs: str) -> QuerySet[BaseRow]: ...
    def update(self, **kwargs) -> int: ...
    def order_by(self, *fields: str) -> QuerySet[BaseRow]: ...
    def exists(self) -> bool: ...
