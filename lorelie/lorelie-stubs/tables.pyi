from ast import List
from typing import Any, Literal, OrderedDict, Type, Union, NamedTuple

from lorelie.backends import BaseRow, SQLiteBackend
from lorelie.constraints import CheckConstraint
from lorelie.fields import Field
from lorelie.indexes import Index
from lorelie.migrations import Migrations
from lorelie.queries import Query, QuerySet
from lorelie.functions import Functions


class BaseTable(type):
    def __new__(cls, name: str, bases: tuple, attrs: dict) -> type: ...
    @classmethod
    def prepare(cls, table: type) -> None: ...


class AbstractTable(metaclass=BaseTable):
    query_class: Type[Query] = ...
    backend_class: Type[SQLiteBackend] = ...
    backend: SQLiteBackend = ...
    is_prepared: bool = Literal[False]

    def __init__(
        self,
        database_name: str = ...,
        inline_build: bool = ...
    ) -> None: ...

    def __hash__(self) -> int: ...
    def __eq__(self) -> bool: ...

    def validate_values(self, fields, values) -> Any: ...
    def all(self) -> list[BaseRow]: ...
    def filter(self, **kwargs) -> list[BaseRow]: ...
    def first(self) -> BaseRow: ...
    def last(self) -> BaseRow: ...
    def create(self, **kwargs) -> BaseRow: ...

    def bulk_create(
        self,
        objs: List[Union[dict, NamedTuple]]
    ) -> List[BaseRow]: ...
    def get(self, **kwargs) -> Union[BaseRow, None]: ...
    def annotate(self, **kwargs: Functions) -> QuerySet: ...
    def order_by(self, *fields: str) -> list[BaseRow]: ...


class Table(AbstractTable):
    fields_map: OrderedDict[str, Field] = ...
    name: str = ...
    query: Query = ...

    def __init__(
        self,
        name: str,
        *,
        database_name: str = ...,
        inline_build: bool = ...,
        fields: list[Field] = ...,
        index: list[Index],
        constraints: list[CheckConstraint]
    ) -> None: ...

    def __repr__(self) -> str: ...

    def has_field(self, name: str) -> bool: ...
    def create_table_sql(self, fields: list[str]) -> list[str]: ...
    def drop_table_sql(self, name: str) -> list[str]: ...
    def build_field_parameters(self) -> list[str]: ...
    def prepare(self) -> None: ...


class Database:
    migrations: Migrations = ...
    migrations_class: Type[Migrations] = ...
    table_map: dict[str, Table] = {}
    database_name: str = ...
    table_instances: list[Table] = ...

    def __init__(self, name: str, *tables: Table): ...
    def __repr__(self) -> str: ...
    def __getitem__(self, table_name: str) -> Table: ...

    def get_table(self, table_name: str) -> Table: ...
    def make_migrations(self) -> None: ...
    def migrate(self) -> None: ...
