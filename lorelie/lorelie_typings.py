import pathlib
from typing import TYPE_CHECKING, Any, Literal, Protocol, Sequence, Tuple, TypeVar
from enum import Enum

if TYPE_CHECKING:
    from lorelie.database.base import SQLiteBackend
    from lorelie.database.tables.base import Table
    from lorelie.fields import Field
    from lorelie.database.base import Database
    from lorelie.database.nodes import BaseNode
    from lorelie.backends import BaseRow
    from lorelie.queries import QuerySet, Query
    from lorelie.expressions import Q, CombinedExpression
    from lorelie.constraints import BaseConstraint
    from lorelie.database.indexes import Index
    from lorelie.database.manager import DatabaseManager
    from lorelie.database.functions.base import Functions
    from lorelie.database.functions.window import Rank, PercentRank, CumeDist, Lead, Lag, DenseRank, LastValue, FirstValue, NthValue, NTile, RowNumber


type TypeRow = 'BaseRow'

type TypeRowList = list[TypeRow]

type TypeDatabaseManager = 'DatabaseManager'

type TypeTable = 'Table'

type TypeSQLiteBackend = 'SQLiteBackend'

type NullableType[T] = T | None

type TypeFunction = 'Functions'

type TypeDatabase = 'Database'

type TypeField = 'Field'

type TypeListStr = list[str]

type TypeNode = 'BaseNode'

type TypeTableMap = dict[str, TypeTable]

type TypeQuerySet = 'QuerySet'

type TypeQ = 'Q'

type TypeOrCombinedExpression[T] = 'T | CombinedExpression'

type TypeDecomposedFilterTuple = tuple[str, TypeOperatorType, Any]

type TypeListAny[T: str | int | float | bool | dict | list | None] = Sequence[T]


TranslatedOperatorType = Literal[
    '=', '<', '>', '<=', '>=', '<>', 'like', 'startwith',
    'endwith', 'between', '!=', 'in', 'isnull', 'regexp'
]

TypeOperatorType = Literal[
    'eq', 'lt', 'gt', 'lte', 'gte', 'contains',
    'startswith', 'endswith', 'range', 'ne', 'in', 'isnull',
    'regex', 'day', 'month', 'iso_year', 'year',
    'minute', 'second', 'hour', 'time', '<>'
]

type TypeListTranslatedOperatorType = list[TranslatedOperatorType | str]


TypeAny = TypeVar('TypeAny')

# TypeSQLiteBackend = TypeVar('TypeSQLiteBackend', bound='SQLiteBackend')

# TypeTable = TypeVar('TypeTable', bound='Table')

# TypeTableMap = TypeVar('TypeTableMap', bound='dict[str, Table]')

TypePathlibPath = TypeVar('TypePathlibPath', bound='pathlib.Path')

TypeStrOrPathLibPath = TypeVar(
    'TypeStrOrPathLibPath', bound='str | pathlib.Path')


TypeAnyNormalTypes = TypeVar(
    'TypeAnyNormalTypes', bound='str | int | float | bool | dict | list | None')

# TypeField = TypeVar('TypeField', bound='Field')

# TypeDatabase = TypeVar('TypeDatabase', bound='Database')

TypeIndex = TypeVar('TypeIndex', bound='Index')

# TypeNode = TypeVar('TypeNode', bound='BaseNode')

# TypeQuerySet = TypeVar('TypeQuerySet', bound='QuerySet')

TypeQuery = TypeVar('TypeQuery', bound='Query')

# TypeQ = TypeVar('TypeQ', bound='Q')

TypeExpression = TypeVar('TypeExpression', 'Q', 'CombinedExpression')

TypeConstraint = TypeVar('TypeConstraint', bound='BaseConstraint')

# TypeDatabaseManager = TypeVar('TypeDatabaseManager', bound='DatabaseManager')


class TypeNewValue(Protocol):
    __dataclass_fields__: dict


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
    INSERT = 'insert'
    ORDER_BY = 'order_by'
    WHERE = 'where'
    # JOIN = 'join'
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



# class ExpressionFiltersDict(TypedDict, total=False):
#     eq: str
#     lt: str
#     gt: str
#     lte: str
#     gte: str
#     contains: str
#     startswith: str
#     endswith: str
#     range: str
#     ne: str
#     # in: str
#     isnull: str
#     regex: str
#     day: str
#     month: str
#     iso_year: str
#     year: str
#     minute: str
#     second: str
#     hour: str
#     time: str


# a = ExpressionFiltersDict({'eq': '=', 'lt': '<', 'gt': '>', 'lte': '<=', 'gte': '>=',
#                        'contains': 'LIKE', 'startswith': 'LIKE', 'endswith': 'LIKE',
#                        'range': 'BETWEEN', 'ne': '!=', 'isnull': 'IS',
#                        'regex': 'REGEXP', 'day': 'DAY', 'month': 'MONTH',
#                        'iso_year': 'ISO_YEAR', 'year': 'YEAR', 'minute': 'MINUTE',
#                        'second': 'SECOND', 'hour': 'HOUR', 'time': 'TIME'})


# TypeListExpression = list[str, OperatorType, Any]

# TypeListOperatorType = list[str, TranslatedOperatorType, Any]


# TypeRow = TypeVar('TypeRow', bound='BaseRow')

TypeLogicalOperators = Literal['and', 'or']


class OnDeleteEnum(Enum):
    CASCADE = 'cascade'
    SET_NULL = 'set_null'
    RESTRICT = 'restrict'
    NO_ACTION = 'no_action'
    SET_DEFAULT = 'set_default'


TypeOnDeleteTypes = Literal[OnDeleteEnum.CASCADE,
                            OnDeleteEnum.SET_NULL,
                            OnDeleteEnum.RESTRICT,
                            OnDeleteEnum.NO_ACTION,
                            OnDeleteEnum.SET_DEFAULT]


class TriggerEnum(Enum):
    BEFORE_INSERT = 'before_insert'
    AFTER_INSERT = 'after_insert'
    BEFORE_UPDATE = 'before_update'
    AFTER_UPDATE = 'after_update'
    BEFORE_DELETE = 'before_delete'
    AFTER_DELETE = 'after_delete'


TypeDeconstructedIndex = Tuple[str, list[str], dict[str, Any]]

TypeDeconstructedField = Tuple[str, str, dict[str, bool]]


# TypeFunction = TypeVar('TypeFunction', bound='Functions')

TypeWindowFunction = TypeVar(
    'TypeWindowFunction', 'Rank', 'PercentRank', 'CumeDist', 'Lead', 'Lag')
