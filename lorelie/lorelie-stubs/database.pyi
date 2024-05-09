from typing import Any, List, OrderedDict, Type, Union

import pandas

from lorelie.backends import BaseRow, SQLiteBackend
from lorelie.migrations import Migrations
from lorelie.queries import QuerySet
from lorelie.tables import Table


class Databases:
    database_map: OrderedDict[str, Database] = ...
    created_databases: List[Database] = ...

    def __init__(self) -> None: ...
    def __getitem__(self, name: str) -> Database: ...
    def __contains__(self, value: Any) -> bool: ...

    @property
    def in_memory(self) -> bool: ...

    def register(self, database: Database) -> None: ...


databases: Databases


class DatabaseManager:
    table_map: dict[str, Table] = ...
    database: Database = ...

    def __init__(self) -> None: ...
    def __repr__(self) -> str: ...

    def __get__(
        self,
        instance: Database,
        cls: Type[Database] = ...
    ) -> DatabaseManager: ...

    def _get_select_sql(
        self,
        selected_table: Table,
        columns: list = ...
    ) -> list[str]: ...

    def before_action(self, table_name: str) -> Table: ...
    def first(self, table: str) -> BaseRow: ...
    def last(self, table: str) -> BaseRow: ...
    def all(self, table: str) -> list[BaseRow]: ...
    def create(self, table: str, **kwargs) -> BaseRow: ...
    def filter(self, table: str, **kwargs) -> list[BaseRow]: ...
    def get(self, table: str, **kwargs) -> BaseRow: ...
    def annotate(self, table: str, **kwargs) -> list[BaseRow]: ...
    def values(self, table: str, *args: str) -> list[dict[str, Any]]: ...
    def dataframe(self, table: str, **kwarg) -> pandas.DataFrame: ...
    def bulk_create(self, table: str) -> QuerySet: ...
    def order_by(self, table: str) -> QuerySet: ...
    def count(self, table: str) -> int: ...
    def dates(self, table: str) -> int: ...
    def datetimes(self, table: str) -> int: ...
    def difference(self, table: str) -> QuerySet: ...
    def distinct(self, table: str) -> QuerySet: ...
    def earliest(self, table: str) -> BaseRow: ...
    def latest(self, table: str) -> BaseRow: ...
    def only(self, table: str) -> QuerySet: ...
    def exclude(self, table: str) -> QuerySet: ...
    def extra(self, table: str) -> QuerySet: ...
    def get_or_create(self, table: str) -> Union[BaseRow, QuerySet]: ...
    def select_for_update(self, table: str) -> QuerySet: ...
    def select_related(self, table: str) -> QuerySet: ...
    def fetch_related(self, table: str) -> QuerySet: ...
    def update(self, table: str) -> QuerySet: ...
    def update_or_create(self, table: str) -> QuerySet: ...
    def resolve_expression(self, table: str) -> Union[BaseRow, QuerySet]: ...


class Database:
    migrations_class: Type[Migrations] = ...
    backend_class: Type[SQLiteBackend] = ...
    database_name: str = ...
    migrations: Migrations = ...
    table_map: dict[str, Table] = ...
    table_instances: list[Table] = ...
    objects: DatabaseManager = ...

    def __init__(self, *tables: Table, name: str = ...): ...
    def __repr__(self) -> str: ...
    def __getitem__(self, table_name: str) -> Table: ...
    # def __getattribute__(self, name: str) -> Union[Table, Any]: ...
    def __hash__(self) -> int: ...

    @property
    def in_memory(self) -> bool: ...
    def get_table(self, table_name: str) -> Table: ...
    def make_migrations(self) -> None: ...
    def migrate(self) -> None: ...

    def foreign_key(
        self,
        left_table: Table,
        right_table: Table,
        on_delete,
        related_name: str = ...
    ) -> None: ...

    def many_to_many(
        self,
        left_table: Table,
        right_table: Table,
        primary_key: bool = True,
        related_name: str = ...
    ) -> None: ...

    def one_to_one_key(
        self,
        left_table: Table,
        right_table: Table,
        on_delete,
        related_name: str = ...
    ) -> None: ...
