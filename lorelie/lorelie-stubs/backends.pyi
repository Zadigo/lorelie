import sqlite3
from sqlite3 import Cursor, Row
from typing import Any, Callable, List, Literal, Tuple

from lorelie.functions import Functions
from lorelie.tables import Table


class BaseRow(Row):
    updated_fields: dict[str, Any] = ...
    _mark_for_update: bool = ...
    _cursor: Cursor = ...
    _fields: list[str] = ...
    _cached_data: tuple[str] = ...
    _backend: SQLiteBackend = ...
    _table: Table = ...

    def __init__(
        self, cursor: Cursor,
        fields: list[str], 
        data: tuple[str]
    ): ...

    def __repr__(self) -> str: ...
    def __setitem__(self, name: str, value: Any) -> None: ...
    def __getitem__(self, name: str) -> Any: ...
    def __hash__(self) -> int: ...
    def __contains__(self, value: str) -> bool: ...
    def __eq__(self, value: str) -> bool: ...

    def save(self) -> BaseRow: ...
    def delete(self) -> BaseRow: ...


def row_factory(
    backend: SQLiteBackend
) -> Callable[[Cursor, Row], BaseRow]: ...


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
    DESCENDNIG: str = ...

    ORDER_BY: str = ...
    GROUP_BY: str = ...

    LOWER: str = ...
    UPPER: str = ...
    LENGTH: str = ...
    MAX: str = ...
    MIN: str = ...
    COUNT: str = ...

    STRFTIME: str = ...

    CHECK_CONSTRAINT: str = ...

    CASE: str = ...
    WHEN: str = ...

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

    def quote_startswith(self, value: str) -> str: ...
    def quote_endswith(self, value: str) -> str: ...
    def quote_like(self, value: str) -> str: ...

    def dict_to_sql(
        self,
        data: dict[str, int, float],
        quote_values: bool = ...
    ) -> Tuple[list[str], list[str]]: ...

    def build_script(self, *sqls) -> str: ...
    def decompose_filters_from_string(self, value: str) -> str: ...
    def decompose_filters(self, **kwargs) -> List[Tuple[str]]: ...
    def build_filters(self, items: List[Tuple[str]]) -> List[str]: ...

    def build_annotation(
        self,
        **conditions: Functions
    ) -> List[str]: ...


class SQLiteBackend(SQL):
    database_name: str = ...
    connection: sqlite3.Connection = ...
    # table: Table = ...

    def __init__(
        self,
        database_name: str = ...,
        table: Table = ...
    ) -> None: ...

    def list_table_columns_sql(self, table: Table) -> list[BaseRow]: ...
    def drop_indexes_sql(self, row: BaseRow) -> str: ...

    def create_table_fields(
        self,
        table: Table,
        columns_to_create: list[str]
    ) -> None: ...

    def list_tables_sql(self) -> list[BaseRow]: ...
    def list_database_indexes(self) -> List[BaseRow]: ...
    def list_table_indexes(self) -> List[BaseRow]: ...

    def save_row_object(
        self,
        row: BaseRow,
        sql_tokens: list[str]
    ) -> BaseRow: ...
