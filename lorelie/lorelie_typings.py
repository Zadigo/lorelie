import pathlib
from typing import TYPE_CHECKING, Any, Literal, TypeVar
from enum import Enum
if TYPE_CHECKING:
    from lorelie.database.base import SQLiteBackend
    from lorelie.database.tables.base import Table
    from lorelie.fields import Field
    from lorelie.database.base import Database
    from lorelie.database.nodes import BaseNode
    from lorelie.queries import QuerySet

TypeSQLiteBackend = TypeVar('TypeSQLiteBackend', bound='SQLiteBackend')

TypeTable = TypeVar('TypeTable', bound='Table')

TypeTableMap = TypeVar('TypeTableMap', bound='dict[str, Table]')

TypePathlibPath = TypeVar('TypePathlibPath', bound='pathlib.Path')

TypeStrOrPathLibPath = TypeVar(
    'TypeStrOrPathLibPath', bound='str | pathlib.Path')

TypeAny = TypeVar('TypeAny')

TypeField = TypeVar('TypeField', bound='Field')

TypeDatabase = TypeVar('TypeDatabase', bound='Database')

TypeNode = TypeVar('TypeNode', bound='BaseNode')

TypeQuerySet = TypeVar('TypeQuerySet', bound='QuerySet')

TypeDecomposedFilterTuple = tuple[str, str, Any]


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


class NodeEnums(Enum):
    """The different types of nodes available."""
    SELECT = 'select'
    # INSERT = 'insert'
    UPDATE = 'update'
    DELETE = 'delete'
    CREATE = 'create'
    # WHERE = 'where'
    # JOIN = 'join'
    # ORDER_BY = 'order_by'
    # GROUP_BY = 'group_by'
    # HAVING = 'having'
    # LIMIT = 'limit'
    # OFFSET = 'offset'
    # RAW_SQL = 'raw_sql'
    # FIELD = 'field'
    # TABLE = 'table'
    # VALUE = 'value'
    # CONDITION = 'condition'
    INTERSECT = 'intersect'
    VIEW = 'view'


class JoinTypeEnum(Enum):
    INNER = 'inner'
    LEFT = 'left'
    RIGHT = 'right'
    CROSS = 'cross'


TypeJoinTypes = Literal[
    'inner',
    'left',
    'right',
    'cross'
]
