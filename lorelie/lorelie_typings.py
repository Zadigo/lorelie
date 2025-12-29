import pathlib
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from lorelie.database.base import SQLiteBackend
    from lorelie.database.tables.base import Table

TypeSQLiteBackend = TypeVar('TypeSQLiteBackend', bound='SQLiteBackend')

TypeTable = TypeVar('TypeTable', bound='Table')

TypeTableMap = TypeVar('TypeTableMap', bound='dict[str, Table]')

TypePathlibPath = TypeVar('TypePathlibPath', bound='pathlib.Path')

TypeStrOrPathLibPath = TypeVar(
    'TypeStrOrPathLibPath', bound='str | pathlib.Path')

TypeAny = TypeVar('TypeAny')
