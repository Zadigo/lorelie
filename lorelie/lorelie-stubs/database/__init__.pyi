from typing import (OrderedDict, TypeVar)

from lorelie.database.base import Database
from lorelie.tables import Table

TableType = TypeVar('TableType', bound=Table)


class MasterRegistry:
    current_database: Database = ...
    known_tables: OrderedDict[str, Table] = ...

    def __repr__(self) -> str: ...

    def register_database(self, database: Database) -> None: ...


registry: MasterRegistry
