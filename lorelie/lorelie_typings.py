import pathlib
from typing import TYPE_CHECKING, TypeVar
from enum import Enum
if TYPE_CHECKING:
    from lorelie.database.base import SQLiteBackend
    from lorelie.database.tables.base import Table
    from lorelie.fields import Field
    from lorelie.database.base import Database

TypeSQLiteBackend = TypeVar('TypeSQLiteBackend', bound='SQLiteBackend')

TypeTable = TypeVar('TypeTable', bound='Table')

TypeTableMap = TypeVar('TypeTableMap', bound='dict[str, Table]')

TypePathlibPath = TypeVar('TypePathlibPath', bound='pathlib.Path')

TypeStrOrPathLibPath = TypeVar(
    'TypeStrOrPathLibPath', bound='str | pathlib.Path')

TypeAny = TypeVar('TypeAny')

TypeField = TypeVar('TypeField', bound='Field')

TypeDatabase = TypeVar('TypeDatabase', bound='Database')


class FieldTypeEnum(Enum):
    TEXT = 'text'
    INTEGER = 'integer'
    REAL = 'real'
    BLOB = 'blob'
    NULL = 'null'

class ConstraintTypeEnum(Enum):
    PRIMARY_KEY = 'primary_key'
    UNIQUE = 'unique'
    NOT_NULL = 'not_null'
    DEFAULT = 'default'
    CHECK = 'check'
    FOREIGN_KEY = 'foreign_key'
