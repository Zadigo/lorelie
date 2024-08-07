import datetime
from collections.abc import Awaitable
from typing import (Any, ClassVar, Dict, Literal, NamedTuple, Optional,
                    Protocol, Type, Union)

import pandas

from lorelie.backends import BaseRow
from lorelie.database.base import Database, RelationshipMap
from lorelie.database.functions.aggregation import Avg, Count, Sum
from lorelie.queries import Query, QuerySet, ValuesIterable
from lorelie.tables import Table


class DataclassProtocol(Protocol):
    __dataclass_fields__: ClassVar[Dict[str, Any]]


class DatabaseManager:
    table_map: dict[str, Table] = ...
    database: Database = ...
    auto_created: bool = Literal[True]

    def __init__(self) -> None: ...
    def __repr__(self) -> str: ...

    def __get__(
        self,
        instance: Database,
        cls: Optional[Type[Database]] = ...
    ) -> DatabaseManager: ...

    @classmethod
    def as_manager(
        cls,
        table_map: Optional[dict] = ...,
        database: Optional[Database] = ...
    ) -> DatabaseManager: ...

    def _validate_auto_fields(
        self,
        table: Table,
        params: dict[str, str],
        update_only: Optional[bool] = ...
    ) -> dict[str, str]: ...

    def _get_select_sql(
        self,
        selected_table: Table,
        columns: Optional[list] = ...
    ) -> list[str]: ...

    def _get_first_or_last_sql(
        self,
        selected_table: Table,
        first: Optional[bool] = ...
    ) -> list[str]: ...

    def pre_save(
        self,
        selected_table: Table,
        data: list[Any]
    ) -> NamedTuple: ...

    def before_action(self, table_name: str) -> Table: ...
    def first(self, table: str) -> BaseRow: ...
    def last(self, table: str) -> BaseRow: ...
    def all(self, table: str) -> QuerySet[BaseRow]: ...
    def create(self, table: str, **kwargs) -> BaseRow: ...
    def filter(self, table: str, *args, **kwargs) -> QuerySet[BaseRow]: ...
    def get(self, table: str, *args, **kwargs) -> BaseRow: ...
    def annotate(self, table: str, *args, **kwargs) -> QuerySet[BaseRow]: ...
    def values(self, table: str, *fields: str) -> ValuesIterable: ...
    def dataframe(self, table: str, *fields) -> pandas.DataFrame: ...

    def bulk_create(
        self,
        table: str,
        objs: list[DataclassProtocol]
    ) -> QuerySet[BaseRow]: ...

    def order_by(self, table: str, *fields: str) -> QuerySet[BaseRow]: ...
    def count(self, table: str) -> int: ...

    def dates(
        self,
        table: str,
        field: str,
        field_to_sort: Optional[str] = ...,
        ascending: Optional[bool] = ...
    ) -> list[datetime.date]: ...

    def datetimes(
        self,
        table: str,
        field: str,
        field_to_sort: Optional[str] = ...,
        ascending: Optional[bool] = ...
    ) -> list[datetime.datetime]: ...

    def difference(self, table: str, *qs: QuerySet) -> QuerySet[BaseRow]: ...
    def distinct(self, table: str, *fields: str) -> QuerySet[BaseRow]: ...
    def earliest(self, table: str, *fields: str) -> BaseRow: ...
    def latest(self, table: str, *fields: str) -> BaseRow: ...
    def only(self, table: str, *fields) -> QuerySet[BaseRow]: ...
    def exclude(self, table: str, *args, **kwargs) -> QuerySet[BaseRow]: ...
    def extra(self, table: str) -> QuerySet[BaseRow]: ...

    def get_or_create(
        self,
        table: str,
        create_defaults: Optional[dict[str, Any]] = ...,
        **kwargs
    ) -> BaseRow: ...

    # def select_for_update(self, table: str) -> QuerySet[BaseRow]: ...
    # def select_related(self, table: str) -> QuerySet[BaseRow]: ...
    # def fetch_related(self, table: str) -> QuerySet[BaseRow]: ...

    def update_or_create(
        self,
        table: str,
        create_defaults: Optional[dict[str, Any]] = ...,
        **kwargs
    ) -> BaseRow: ...

    def intersect(self, table: str, qs1: QuerySet, qs2: QuerySet) -> QuerySet: ...

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

    async def afirst(self, table: str) -> Awaitable[BaseRow]: ...
    async def alast(self, table: str) -> Awaitable[BaseRow]: ...
    async def aall(self, table: str) -> Awaitable[QuerySet]: ...
    async def acreate(self, table: str, **kwargs) -> Awaitable[BaseRow]: ...

    async def afilter(
        self,
        table: str,
        *args,
        **kwargs
    ) -> Awaitable[QuerySet[BaseRow]]: ...

    async def aget(
        self,
        table: str,
        *args, **kwargs
    ) -> Awaitable[BaseRow]: ...

    async def aannotate(
        self,
        table: str,
        *args, **kwargs
    ) -> Awaitable[QuerySet[BaseRow]]: ...

    async def avalues(
        self,
        table: str,
        *fields: str
    ) -> Awaitable[ValuesIterable]: ...

    async def adataframe(
        self,
        table: str,
        *fields
    ) -> Awaitable[pandas.DataFrame]: ...

    async def abulk_create(
        self,
        table: str,
        objs: list[DataclassProtocol]
    ) -> Awaitable[QuerySet[BaseRow]]: ...

    async def aorder_by(
        self,
        table: str,
        *fields: str
    ) -> Awaitable[QuerySet[BaseRow]]: ...

    async def acount(self, table: str) -> Awaitable[int]: ...

    async def adates(
        self,
        table: str,
        field: str,
        field_to_sort: Optional[str] = ...,
        ascending: Optional[bool] = ...
    ) -> Awaitable[list[datetime.date]]: ...

    async def adatetimes(
        self,
        table: str,
        field: str,
        field_to_sort: Optional[str] = ...,
        ascending: Optional[bool] = ...
    ) -> Awaitable[list[datetime.datetime]]: ...

    async def adifference(
        self,
        table: str,
        *qs: QuerySet
    ) -> Awaitable[QuerySet[BaseRow]]: ...

    async def adistinct(
        self,
        table: str,
        *fields: str
    ) -> Awaitable[QuerySet[BaseRow]]: ...

    async def aearliest(
        self,
        table: str,
        *fields: str
    ) -> Awaitable[BaseRow]: ...

    async def alatest(
        self,
        table: str,
        *fields: str
    ) -> Awaitable[BaseRow]: ...

    async def aonly(
        self,
        table: str,
        *fields
    ) -> Awaitable[QuerySet[BaseRow]]: ...

    async def aexclude(
        self,
        table: str,
        *args,
        **kwargs
    ) -> Awaitable[QuerySet[BaseRow]]: ...

    async def aextra(self, table: str) -> Awaitable[QuerySet[BaseRow]]: ...

    async def aget_or_create(
        self,
        table: str,
        create_defaults: Optional[dict[str, Any]] = ...,
        **kwargs
    ) -> Awaitable[BaseRow]: ...

    # async def aselect_for_update(self, table: str) -> Awaitable[QuerySet[BaseRow]]: ...
    # async def aselect_related(self, table: str) -> Awaitable[QuerySet[BaseRow]]: ...
    # async def afetch_related(self, table: str) -> Awaitable[QuerySet[BaseRow]]: ...

    async def aupdate_or_create(
        self,
        table: str,
        create_defaults: Optional[dict[str, Any]] = ...,
        **kwargs
    ) -> Awaitable[BaseRow]: ...

    async def aresolve_expression(
        self,
        table: str
    ) -> Awaitable[Union[BaseRow, QuerySet]]: ...

    async def aaggregate(
        self,
        table: str,
        *args: Union[Count, Avg, Sum],
        **kwargs
    ) -> Awaitable[dict[str, int]]: ...


class ForeignTablesManager:
    reverse: bool = ...
    left_table: Table = ...
    right_table: Table = ...
    # relatationship_name: str = ...
    # relationship: RelationshipMap = ...
    # database_manager: DatabaseManager = ...
    current_row: BaseRow = ...

    def __init__(
        self,
        relationship_map: RelationshipMap,
        reverse: Optional[bool] = ...
    ) -> None: ...

    def __repr__(self) -> None: ...

    def all(self) -> QuerySet: ...
    def last(self) -> BaseRow: ...
    def create(self, **kwargs) -> BaseRow: ...
