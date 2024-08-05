import dataclasses
import functools
from dataclasses import field
from typing import Any, OrderedDict, Protocol, TypeVar, Union

from lorelie.backends import BaseRow
from lorelie.database.base import Database
from lorelie.database.tables.base import Table

TableType = TypeVar('TableType', bound=Table)


class InnerMethodProtocol(Protocol):
    def __call__(
        self,
        instance: BaseRow,
        table: Table,
        **kwargs: Any
    ) -> None: ...


@dataclasses.dataclass
class TriggersMap:
    container: list[InnerMethodProtocol] = field(default_factory=list)
    # unlinked: list[InnerMethodProtocol] = field(default_factory=list)
    # pre_init: list[InnerMethodProtocol] = field(default_factory=list)
    # post_init: list[InnerMethodProtocol] = field(default_factory=list)
    # pre_save: list[InnerMethodProtocol] = field(default_factory=list)
    # post_save: list[InnerMethodProtocol] = field(default_factory=list)
    # pre_delete: list[InnerMethodProtocol] = field(default_factory=list)
    # post_delete: list[InnerMethodProtocol] = field(default_factory=list)

    @functools.lru_cache(maxsize=100)
    def list_functions(
        self,
        table: Union[str, Table],
        trigger_name: str
    ) -> list[InnerMethodProtocol]: ...

    def run_named_triggers(
        self,
        name: str = ...,
        table: Table = ...
    ) -> None: ...


class MasterRegistry:
    current_database: Database = ...
    known_tables: OrderedDict[str, Table] = ...
    registered_triggers: TriggersMap = ...

    def __repr__(self) -> str: ...

    def register_database(self, database: Database) -> None: ...


registry: MasterRegistry
