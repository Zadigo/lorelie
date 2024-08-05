from dataclasses import dataclass

from lorelie.database.tables.base import RelationshipMap, Table
from lorelie.fields.base import Field

@dataclass
class Column:
    field: Field
    index: int = 1
    name: str = None
    relationship_map: RelationshipMap = None
    reverse_relation: bool = False
    double_relation: bool = False

    def __post_init__(self) -> None: ...
    def __str__(self) -> str: ...
    def __eq__(self, item: Column) -> bool: ...
    def __hash__(self) -> int: ...

    @property
    def is_foreign_column(self) -> bool: ...

    @property
    def table(self) -> Table: ...

    def copy(self) -> Column: ...
    def prepare(self) -> None: ...
