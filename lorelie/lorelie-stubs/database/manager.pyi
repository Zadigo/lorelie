from typing import Any, Literal, Type, Union

import pandas

from lorelie.aggregation import Avg, Count, Sum
from lorelie.backends import BaseRow
from lorelie.database.base import Database
from lorelie.queries import QuerySet, ValuesIterable
from lorelie.tables import Table


class DatabaseManager:
    table_map: dict[str, Table] = ...
    database: Database = ...
    auto_created: bool = Literal[True]

    def __init__(self) -> None: ...
    def __repr__(self) -> str: ...

    def __get__(
        self,
        instance: Database,
        cls: Type[Database] = ...
    ) -> DatabaseManager: ...

    @classmethod
    def as_manager(
        cls,
        table_map: dict = ...,
        database: Database = ...
    ) -> DatabaseManager: ...

    def _get_select_sql(
        self,
        selected_table: Table,
        columns: list = ...
    ) -> list[str]: ...

    def _get_first_or_last_sql(
        self,
        selected_table: Table, first: bool = Literal[True]
    ) -> list[str]: ...

    def before_action(self, table_name: str) -> Table: ...
    def first(self, table: str) -> BaseRow: ...
    def last(self, table: str) -> BaseRow: ...
    def all(self, table: str) -> QuerySet[BaseRow]: ...
    def create(self, table: str, **kwargs) -> BaseRow: ...
    def filter(self, table: str, *args, **kwargs) -> QuerySet[BaseRow]: ...
    def get(self, table: str, *args, **kwargs) -> BaseRow: ...
    def annotate(self, table: str, **kwargs) -> QuerySet[BaseRow]: ...
    def values(self, table: str, *args: str) -> ValuesIterable: ...
    def dataframe(self, table: str, *args) -> pandas.DataFrame: ...
    def bulk_create(self, table: str, *objs) -> QuerySet[BaseRow]: ...
    def order_by(self, table: str, *fields: str) -> QuerySet[BaseRow]: ...
    def count(self, table: str) -> int: ...
    def dates(self, table: str) -> int: ...
    def datetimes(self, table: str) -> int: ...
    def difference(self, table: str) -> QuerySet[BaseRow]: ...
    def distinct(self, table: str, *fields: str) -> QuerySet[BaseRow]: ...
    def earliest(self, table: str) -> BaseRow: ...
    def latest(self, table: str) -> BaseRow: ...
    def only(self, table: str) -> QuerySet[BaseRow]: ...
    def exclude(self, table: str) -> QuerySet[BaseRow]: ...
    def extra(self, table: str) -> QuerySet[BaseRow]: ...
    def get_or_create(self, table: str) -> Union[BaseRow, QuerySet]: ...
    def select_for_update(self, table: str) -> QuerySet[BaseRow]: ...
    def select_related(self, table: str) -> QuerySet[BaseRow]: ...
    def fetch_related(self, table: str) -> QuerySet[BaseRow]: ...
    def update(self, table: str) -> QuerySet[BaseRow]: ...
    def update_or_create(self, table: str) -> QuerySet: ...

    def resolve_expression(
        self,
        table: str
    ) -> Union[BaseRow, QuerySet[BaseRow]]: ...

    def aggregate(
        self,
        table: str,
        *args: Union[Count, Avg, Sum],
        **kwargs
    ) -> dict[str, int]: ...

    def count(self, table: str) -> int: ...
    def foreign_table(self, relationship: str) -> ForeignTablesManager: ...


class ForeignTablesManager:
    manager: DatabaseManager = ...
    left_table: Table = ...
    right_table: Table = ...

    def __init__(
        self,
        left_table: str,
        right_table: str,
        manager: DatabaseManager
    ) -> None: ...
