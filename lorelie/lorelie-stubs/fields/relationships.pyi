from typing import Any, Callable, List, Literal, Optional, TypedDict, Unpack, override

from lorelie.database.base import Database, RelationshipMap
from lorelie.fields.base import Field
from lorelie.database.tables.base import Table

class FieldOptions(TypedDict):
    null: bool
    primary_key: bool
    default: Any
    unique: bool
    validators: list[Callable[[str], None]]


class ForeignKeyAction:
    choice: str = ...

    def __repr__(self) -> str: ...

    @classmethod
    def as_sql(
        cls,
        choice: str,
        on_delete: bool = ...,
        **kwargs
    ) -> None: ...


class ForeignKeyActions:
    CASCADE: ForeignKeyAction = ...
    SET_DEFAULT: ForeignKeyAction = ...
    SET_NULL: ForeignKeyAction = ...
    DO_NOTHING: ForeignKeyAction = ...


class BaseRelationshipField(Field):
    table: Table = ...
    database: Database = ...
    related_name: str = ...
    relationship_map: RelationshipMap = ...
    is_relationship_field: bool = ...
    relationship_field_params: List[str] = ...
    reverse: bool = ...

    def __init__(
        self,
        related_name: Optional[str] = ...,
        reverse: Optional[bool] = ...,
        **kwargs: Unpack[FieldOptions]
    ) -> None: ...

    @override
    def prepare(self, database: Database) -> None: ...


class ForeignKeyField(BaseRelationshipField):
    on_delete: ForeignKeyAction = ...

    def __init__(
        self,
        table: Table,
        name: str,
        on_delete: Optional[ForeignKeyAction] = ...,
        **kwargs
    ) -> None: ...

    @override
    def field_parameters(self) -> List[str]: ...
