import dataclasses
import pathlib
from collections.abc import Callable
from typing import (Any, Literal, Optional, OrderedDict, Protocol, Type,
                    TypeVar, Union)

from lorelie.backends import BaseRow, SQLiteBackend
from lorelie.database.manager import DatabaseManager, ForeignTablesManager
from lorelie.database.migrations import Migrations
from lorelie.queries import Query, QuerySet
from lorelie.tables import Table

TableType = TypeVar('TableType', bound=Table)


class InnerMethodProtocol(Protocol):
    def __call__(
        self,
        instance: BaseRow,
        table: Table,
        **kwargs: Any
    ) -> None: ...


@dataclasses.dataclass
class RelationshipMap:
    left_table: Table
    right_table: Table
    junction_table: Optional[Table] = None
    relationship_type: Literal['foreign'] = 'foreign'
    can_be_validated: bool = ...
    error_message: str = ...

    def __post_init__(self) -> None: ...
    def __repr__(self) -> str: ...

    @property
    def relationship_name(self) -> Union[str, None]: ...

    @property
    def forward_field_name(self) -> str: ...

    @property
    def backward_field_name(self) -> str: ...

    @property
    def foreign_backward_related_field_name(self) -> str: ...

    @property
    def foreign_forward_related_field_name(self) -> str: ...

    def get_relationship_condition(self, table: str) -> tuple[str, str]: ...
    def creates_relationship(self, table: Table) -> bool: ...


T = TypeVar('T', bound=InnerMethodProtocol)


class Database:
    migrations_class: Type[Migrations] = ...
    backend_class: Type[SQLiteBackend] = ...
    database_name: str = ...
    migrations: Migrations = ...
    query_class: Type[Query] = ...
    table_map: dict[str, Table] = ...
    table_instances: list[Table] = ...
    objects: DatabaseManager = ...
    path: pathlib.Path = ...
    relationships: OrderedDict[str, ForeignTablesManager] = ...

    def __init__(
        self,
        *tables: Table,
        name: Optional[str] = ...,
        path: Optional[Union[str, pathlib.Path]] = ...,
        log_queries: Optional[bool] = ...
    ): ...

    def __repr__(self) -> str: ...
    def __getitem__(self, table_name: str) -> Table: ...
    def __contains__(self, value: Any) -> bool: ...
    def __getattr__(self, name: Any) -> Union[DatabaseManager, Any]: ...
    def __hash__(self) -> int: ...

    @property
    def in_memory(self) -> bool: ...
    @property
    def is_ready(self) -> bool: ...
    @property
    def table_names(self) -> list[str]: ...
    @property
    def has_relationships(self) -> bool: ...

    def _add_table(self, table: Table) -> None: ...

    def _prepare_relationship_map(
        right_table: Table,
        left_table: Table
    ) -> RelationshipMap: ...

    def get_table(self, table_name: str) -> Table: ...
    def make_migrations(self) -> None: ...
    def migrate(self) -> None: ...
    def simple_load(self) -> None: ...

    # TODO: Remove this line
    def create_view(
        self,
        name: str,
        queryset: QuerySet,
        temporary: bool = ...
    ) -> None: ...

    def register_trigger(
        self,
        trigger: str,
        table: Optional[Table] = ...,
    ) -> Callable[[Callable[..., None]], T]: ...

    def foreign_key(
        self,
        name: str,
        left_table: TableType,
        right_table: TableType,
        on_delete: str = ...
    ) -> None: ...

    def many_to_many(
        self,
        left_table: Table,
        right_table: Table,
        primary_key: Optional[bool] = True,
        related_name: Optional[str] = ...
    ) -> None: ...

    def one_to_one_key(
        self,
        left_table: Table,
        right_table: Table,
        on_delete,
        related_name: Optional[str] = ...
    ) -> None: ...
