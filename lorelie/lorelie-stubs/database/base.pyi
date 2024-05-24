import dataclasses
import pathlib
from collections.abc import Callable
from dataclasses import field
from typing import (Any, List, Literal, Optional, OrderedDict, Protocol, Type,
                    TypeVar, Union)

from lorelie.backends import BaseRow, SQLiteBackend
from lorelie.database.manager import DatabaseManager
from lorelie.database.migrations import Migrations
from lorelie.fields.relationships import ForeignKeyField
from lorelie.queries import QuerySet
from lorelie.tables import Table

TableType = TypeVar('TableType', bound=Table)


class MasterRegistry:
    current_database: Database = ...
    known_tables: OrderedDict[str, Table] = ...

    def __repr__(self) -> str: ...

    def register_database(self, database: Database) -> None: ...


registry: MasterRegistry


class Databases:
    database_map: OrderedDict[str, Database] = ...

    def __init__(self) -> None: ...
    def __getitem__(self, name: str) -> Database: ...
    def __contains__(self, value: Any) -> bool: ...

    @property
    def created_databases(self) -> List[Database]: ...

    def register(self, database: Database) -> None: ...


databases: Databases


@dataclasses.dataclass
class RelationshipMap:
    left_table: Table
    right_table: Table
    junction_table: Optional[Table] = None
    relationship_type: Literal['foreign'] = 'foreign'
    field: Union[ForeignKeyField] = ...
    can_be_validated: bool = ...
    error_message: str = ...

    def __post_init__(self) -> None: ...
    def __repr__(self) -> str: ...

    @property
    def relationship_field_name(self) -> Union[str, None]: ...
    @property
    def forward_field_name(self) -> str: ...
    @property
    def backward_field_name(self) -> str: ...
    @property
    def backward_related_field(self) -> str: ...
    def creates_relationship(self, table) -> bool: ...


class InnerMethodProtocol(Protocol):
    def __call__(
        self,
        instance: BaseRow,
        table: Table,
        **kwargs: Any
    ) -> None: ...


T = TypeVar('T', bound=InnerMethodProtocol)


@dataclasses.dataclass
class TriggersMap:
    pre_save: list[InnerMethodProtocol] = field(default_factory=list)
    post_save: list[InnerMethodProtocol] = field(default_factory=list)
    pre_delete: list[InnerMethodProtocol] = field(default_factory=list)

    def list_functions(
        self,
        table: Union[str, Table],
        trigger_name: str
    ) -> list[InnerMethodProtocol]: ...


class Database:
    migrations_class: Type[Migrations] = ...
    backend_class: Type[SQLiteBackend] = ...
    database_name: str = ...
    migrations: Migrations = ...
    table_map: dict[str, Table] = ...
    table_instances: list[Table] = ...
    objects: DatabaseManager = ...
    path: pathlib.Path = ...
    relationships: RelationshipMap = ...
    triggers_map: TriggersMap = ...

    def __init__(self, *tables: Table,
                 name: Optional[str] = ..., log_queries: Optional[bool] = ...): ...

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

    def get_table(self, table_name: str) -> Table: ...
    def make_migrations(self) -> None: ...
    def migrate(self) -> None: ...

    def register_trigger(
        self,
        table: Optional[Table] = ...,
        trigger: Optional[str] = ...
    ) -> Callable[[Callable[..., None]], T]: ...

    def create_view(
        self,
        name: str,
        queryset: QuerySet,
        temporary: bool = ...
    ) -> None: ...

    def foreign_key(
        self,
        left_table: TableType,
        right_table: TableType,
        on_delete,
        related_name: str = ...
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
