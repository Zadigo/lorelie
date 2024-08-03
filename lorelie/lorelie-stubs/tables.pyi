import dataclasses
from typing import (Any, List, Literal, Optional, OrderedDict, Tuple, Type, Union,
                    override)

from lorelie.backends import SQLiteBackend
from lorelie.constraints import CheckConstraint
from lorelie.database.base import Database
from lorelie.database.indexes import Index
from lorelie.database.manager import DatabaseManager
from lorelie.fields.base import Field
from lorelie.queries import Query

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


@dataclasses.dataclass
class Column:
    field: Field
    index: int = 1
    name: str = None
    relationship_map: RelationshipMap = None

    def __post_init__(self) -> None: ...
    def __eq__(self, item: Column) -> bool: ...
    def __hash__(self) -> int: ...

    @property
    def is_foreign_column(self) -> bool: ...
    
    def copy(self) -> Column: ...

class BaseTable(type):
    def __new__(
        cls, name: str,
        bases: tuple,
        attrs: dict
    ) -> type: ...

    @classmethod
    def prepare(cls, table: type[Table]) -> None: ...


class AbstractTable(metaclass=BaseTable):
    query_class: Type[Query] = ...
    backend_class: Type[SQLiteBackend] = ...
    backend: SQLiteBackend = ...
    is_prepared: bool = Literal[False]
    field_types: OrderedDict[str, str] = ...
    database: Database = ...
    objects: DatabaseManager = ...

    def __init__(self) -> None: ...
    def __hash__(self) -> int: ...
    def __eq__(self, value: Any) -> bool: ...
    def __bool__(self) -> bool: ...

    @staticmethod
    def validate_table_name(name: str) -> str: ...

    def validate_values_from_list(
        self,
        fields: List[str],
        values: List[Any]
    ) -> List[Tuple[list[str], dict[str, Any]]]: ...

    def validate_values_from_dict(
        self,
        fields: List[str],
        values: List[Any]
    ) -> Tuple[list[str], dict[str, Any]]: ...

    def validate_values(
        self,
        fields: List[str],
        values: List[Any]
    ) -> Tuple[list[str], dict[str, Any]]: ...

    def load_current_connection(self) -> None: ...


class Table(AbstractTable):
    name: str = ...
    verbose_name: str = ...
    indexes: list[str] = ...
    table_constraints: list[CheckConstraint] = ...
    field_constraints: dict = ...
    str_field: str = ...
    ordering: set[str] = ...
    fields_map: OrderedDict[str, Field] = ...
    auto_add_fields: set = ...
    auto_update_fields: set = ...
    field_names: list[str] = ...
    is_foreign_key_table: bool = Literal[False]
    # TODO: Remove Query on the class
    query: type[Query] = ...
    backend_class = type[SQLiteBackend] = ...
    objects: DatabaseManager = ...
    columns: set[Column] = ...

    def __init__(
        self,
        name: str,
        *,
        fields: Optional[list[Field]] = ...,
        indexes: Optional[list[Index]] = ...,
        constraints: Optional[list[CheckConstraint]] = ...,
        ordering: Optional[list[str]] = ...,
        str_field: Optional[str] = ...
    ) -> None: ...

    @override
    def __hash__(self) -> int: ...

    def __repr__(self) -> str: ...

    @override
    def __eq__(self, table: Table) -> bool: ...

    def __contains__(self, value: Any) -> bool: ...
    def __setattr__(self, name: str, value: Any) -> Any: ...
    def __getattribute__(self, name) -> Any: ...
    def __repr__(self) -> str: ...

    @staticmethod
    def compare_field_types(*fields: Field) -> bool: ...

    @property
    def has_relationships(self) -> bool: ...

    def _add_field(self, field_name: str, field: Field) -> list[str]: ...

    def has_field(
        self,
        name: str,
        raise_exception: bool = Literal[False]
    ) -> bool: ...

    def get_field(self, name: str) -> Field: ...
    def create_table_sql(self, fields: list[str]) -> list[str]: ...
    def drop_table_sql(self) -> list[str]: ...
    def build_all_field_parameters(self) -> list[str]: ...
    def prepare(self, database: Database) -> None: ...
