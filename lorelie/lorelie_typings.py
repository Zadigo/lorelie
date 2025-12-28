from typing import TypeVar
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lorelie.database.base import SQLiteBackend

TypeSQLiteBackend = TypeVar('TypeSQLiteBackend', bound='SQLiteBackend')
