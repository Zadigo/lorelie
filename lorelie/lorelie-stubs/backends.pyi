import dataclasses
import re
import sqlite3
import pathlib
from sqlite3 import Cursor, Row
from typing import Any, Callable, DefaultDict, List, Literal, Optional, Tuple, Union

from lorelie.database.base import Database
from lorelie.database.functions.base import Functions
from lorelie.database.manager import ForeignTablesManager
from lorelie.expressions import BaseExpression, CombinedExpression
from lorelie.queries import Query, QuerySet
from lorelie.tables import Table


class Connections:
    connections_map: dict[str, SQLiteBackend] = ...
    created_connections: set[SQLiteBackend] = ...

    def __repr__(self): ...
    def __getitem__(self, name: str) -> SQLiteBackend: ...

    def get_last_connection(self) -> SQLiteBackend: ...
    def register(self, name: str, connection: SQLiteBackend) -> None: ...


connections: Connections


class BaseRow(Row):
    updated_fields: dict[str, Any] = ...
    mark_for_update: bool = ...
    cursor: Cursor = ...
    _fields: list[str] = ...
    _cached_data: dict[str, Any] = ...
    _backend: SQLiteBackend = ...
    linked_to_table: str = ...
    updated_fields: dict = ...
    pk: int = ...

    def __init__(
        self,
        fields: list[str],
        data: dict[str, Any],
        cursor: Cursor = ...,
    ) -> None: ...

    def __repr__(self) -> str: ...
    def __setitem__(self, name: str, value: Any) -> None: ...
    def __getitem__(self, name: str) -> Any: ...
    def __hash__(self) -> int: ...
    def __contains__(self, value: str) -> bool: ...
    def __eq__(self, value: str) -> bool: ...
    def __getattr__(self, key: str) -> Union[ForeignTablesManager, Any]: ...

    @property
    def pk(self) -> int: ...

    def save(self) -> None: ...
    def delete(self) -> BaseRow: ...
    def refresh_from_database(self) -> BaseRow: ...


def row_factory(
    backend: SQLiteBackend
) -> Callable[[Cursor, Row], BaseRow]: ...


@dataclasses.dataclass
class AnnotationMap:
    sql_statements_dict: dict = ...
    alias_fields: list = ...
    field_names: list = ...
    annotation_type_map: dict = ...

    @property
    def joined_final_sql_fields(self) -> list[str]: ...
    @property
    def requires_grouping(self) -> bool: ...


class SQL:
    ALTER_TABLE: str = ...
    CREATE_TABLE: str = ...
    CREATE_INDEX: str = ...
    DROP_TABLE: str = ...
    DROP_INDEX: str = ...
    DELETE: str = ...
    INSERT: str = ...
    SELECT: str = ...
    UPDATE: str = ...

    AND: str = ...
    OR: str = ...

    CONDITION: str = ...
    EQUALITY: str = ...
    LIKE: str = ...
    BETWEEN: str = ...
    IN: str = ...
    NOT_LIKE: str = ...
    WHERE_CLAUSE: str = ...
    WHERE_NOT: str = ...

    WILDCARD_MULTIPLE: str = ...
    WILDCARD_SINGLE: str = ...

    ASCENDING: str = ...
    DESCENDING: str = ...

    ORDER_BY: str = ...
    GROUP_BY: str = ...

    # LOWER: str = ...
    # UPPER: str = ...
    # LENGTH: str = ...
    # MAX: str = ...
    # MIN: str = ...
    AVERAGE: str = ...
    COUNT: str = ...
    STRFTIME: str = ...

    CHECK_CONSTRAINT: str = ...

    CASE: str = ...
    WHEN: str = ...

    LIMIT: str = ...

    SQL_REGEXES: list[re.Pattern] = ...

    base_filters: dict[str, str] = ...

    @staticmethod
    def quote_value(value: Any) -> str: ...
    @staticmethod
    def comma_join(values: List[str]) -> str: ...

    @staticmethod
    def operator_join(
        values: list[str],
        operator: str = Literal['and']
    ) -> str: ...

    @staticmethod
    def simple_join(
        values: List[str],
        space_characters: bool = ...
    ) -> str: ...

    @staticmethod
    def finalize_sql(sql: str) -> str: ...

    @staticmethod
    def de_sqlize_statement(sql: str) -> str: ...

    @staticmethod
    def wrap_parenthentis(value: Any) -> str: ...

    @staticmethod
    def build_alias(condition, alias) -> str: ...

    def build_dot_notation(self, values: list[tuple[str]]) -> list[str]: ...

    # def is_query_filter(self, value_or_values: Union[list, str]) -> bool: ...
    def parameter_join(self, data: dict[str, Any]) -> str: ...
    def quote_values(self, values) -> list[str]: ...
    def quote_startswith(self, value: str) -> str: ...
    def quote_endswith(self, value: str) -> str: ...
    def quote_like(self, value: str) -> str: ...

    def dict_to_sql(
        self,
        data: dict[str, int, float],
        quote_values: bool = ...
    ) -> Tuple[list[str], list[str]]: ...

    def build_script(self, *sqls: str) -> str: ...
    # def decompose_filters_columns(
    #     self, value: Union[str, dict]) -> list[str]: ...

    # def decompose_filters_from_string(self, value: str) -> str: ...
    # def decompose_filters(self, **kwargs) -> List[Tuple[str]]: ...

    # def build_filters(
    #     self,
    #     items: List[Tuple[str]],
    #     space_characters: bool = ...
    # ) -> List[str]: ...

    def build_annotation(
        self,
        conditions: dict[
            str,
            Union[Functions, BaseExpression, CombinedExpression]
        ]
    ) -> AnnotationMap: ...

    def decompose_sql_statement(
        self,
        sql: str
    ) -> DefaultDict[str, list[tuple[str, str]]]: ...


class SQLiteBackend(SQL):
    database_name: str = ...
    database_path: pathlib.Path = ...
    database_instance: Database = ...
    connection: sqlite3.Connection = ...
    current_table: Table = ...
    log_queries: bool = ...
    connection_timestamp: float = ...
    in_memory_connection: bool = ...

    def __init__(
        self,
        database_or_name: Optional[Union[Database, str]] = ...,
        log_queries: Optional[bool] = ...,
        path: Optional[pathlib.Path] = ...
    ) -> None: ...

    def set_current_table(self, table: Table) -> None: ...
    def set_current_table_from_row(self, row: BaseRow) -> None: ...
    def list_table_columns(self, table: Table) -> QuerySet[BaseRow]: ...

    def create_table_fields(
        self,
        table: Table,
        columns_to_create: list[str]
    ) -> None: ...

    def list_all_tables(self) -> list[BaseRow]: ...
    def list_database_indexes(self) -> QuerySet[BaseRow]: ...
    def list_table_indexes(self) -> List[BaseRow]: ...

    def save_row_object(self, row: BaseRow,) -> Query: ...
    def delete_row_object(self, row: BaseRow) -> Query: ...
