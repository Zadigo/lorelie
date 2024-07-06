import dataclasses
from typing import Any, Callable, Dict, Literal, Tuple, Union, override

from database.base import RelationshipMap
from expressions import Q
from tables import Table

from lorelie.backends import SQLiteBackend
from lorelie.database.functions import Functions

@dataclasses.dataclass
class SelectMap:
    select: type[SelectNode] = None
    where: type[WhereNode] = None
    order_by: type[OrderByNode] = None
    limit: int = None
    groupby: list = None
    having: list = None

    def __setitem__(self, name: str,  value: Any) -> None: ...

    @property
    def should_resolve_map(self) -> bool: ...

    def resolve(self, backend) -> list[str]: ...
    def add_ordering(self, other: OrderByNode) -> None: ...


class RawSQL:
    nodes: list[str, BaseNode] = ...
    backend: SQLiteBackend = ...
    resolve_select: bool = ...
    select_map: SelectMap = ...

    def __init__(
        self, backend: SQLiteBackend,
        *
        nodes: Union[str, BaseNode]
    ) -> None: ...

    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...
    def __iter__(self) -> list[str]: ...
    def __eq__(self, value) -> bool: ...
    def as_sql(self) -> list[str]: ...


class ComplexNode:
    def __init__(self, *nodes: BaseNode) -> None: ...
    def __repr__(self) -> str: ...
    def __add__(self, node) -> ComplexNode: ...
    def __contains__(self, node) -> bool: ...

    def as_sql(self, backend) -> RawSQL: ...


class BaseNode:
    template_sql: str = ...
    table: Table = ...
    fields: list = ...

    def __init__(
        self,
        table: Table = ...,
        fields: list[str] = ...
    ) -> None: ...

    def __repr__(self) -> str: ...
    def __add__(self, node: BaseNode) -> ComplexNode: ...
    def __eq__(self, node: BaseNode) -> bool: ...
    def __contains__(self, value: BaseNode) -> bool: ...
    def __and__(self, node: BaseNode) -> BaseNode: ...

    @property
    def node_name(self) -> str: ...

    def as_sql(self, backend: SQLiteBackend) -> list[str]: ...


class SelectNode(BaseNode):
    distinct: bool = Literal[False]

    def __init__(
        self,
        table: Table,
        *fields: str,
        distinct: bool = Literal[False],
    ) -> None: ...

    @override
    def __call__(self, *fields: str) -> SelectNode: ...


class WhereNode(BaseNode):
    expressions: dict[str, Functions] = ...
    func_expressions: list[Functions] = ...

    def __init__(self, *args: Functions, **expressions) -> None: ...
    @override
    def __call__(self, *args: Functions, **expressions) -> WhereNode: ...


class OrderByNode(BaseNode):
    ascending: set = ...
    descending: set = ...
    cached_fields: list[str] = ...

    def __init__(self, table: Table, *fields: str) -> None: ...
    def __hash__(self) -> int: ...
    def __and__(self, node) -> OrderByNode: ...

    @staticmethod
    def construct_sql(
        backend: SQLiteBackend,
        field: str,
        ascending: bool = Literal[True]
    ) -> Union[str, None]: ...


class UpdateNode(BaseNode):
    args: Tuple[Union[str, Q]] = ...
    kwargs: Dict[str, Union[Q, Any]] = ...
    defaults: Dict[str, Any] = ...

    def __init__(
        self,
        table: Table,
        update_defaults: Dict[str, Any],
        *where_args: Q,
        **where_expressions: Union[Any, Q]
    ) -> None: ...


class InsertNode(BaseNode):
    template_sql: str = ...
    insert_values: dict[str, Union[int, float, Callable[..., Any]]] = ...
    batch_values: list[dict[str, Union[int, float, Callable[..., Any]]]] = ...

    def __init__(
        self,
        table: Table,
        batch_values: list[dict[str, Any]] = ...,
        insert_values: dict[str, Any] = ...,
        returning: list[str] = ...
    ) -> None: ...


class JoinNode(BaseNode):
    def __init__(
        self,
        table: str,
        relationship_map: RelationshipMap,
        join_type: str = ...
    ) -> None: ...
