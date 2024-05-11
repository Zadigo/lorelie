from typing import Any, Callable, TypedDict, Unpack, override

from lorelie.backends import SQLiteBackend
from lorelie.database import Database, RelationshipMap
from lorelie.fields.base import Field


class FieldOptions(TypedDict):
    null: bool
    primary_key: bool
    default: Any
    unique: bool
    validators: list[Callable[[str], None]]


class BaseRelationshipField(Field):
    template: str = ...
    name: str = ...
    relationship_map: str = ...

    def __init__(
        self,
        relationship_map: RelationshipMap = ...,
        related_name: str = ...,
        **kwargs: Unpack[FieldOptions]
    ) -> None: ...

    def prepare(self, database: Database) -> None: ...
    def as_sql(self, backend: SQLiteBackend) -> list: ...


class ForeignKeyField(BaseRelationshipField):
    @override
    def as_sql(self, backend: SQLiteBackend) -> list: ...
