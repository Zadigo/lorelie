import enum


class DatabaseEvent(enum.Enum):
    """Database trigger events which are created
    directly on the tables."""

    BEFORE_INSERT = 'before_insert'
    AFTER_INSERT = 'after_insert'
    BEFORE_UPDATE = 'before_update'
    AFTER_UPDATE = 'after_update'
    BEFORE_DELETE = 'before_delete'
    AFTER_DELETE = 'after_delete'
    INSTEAD_OF_INSERT = 'instead_of_insert'
    INSTEAD_OF_DELETE = 'instead_of_delete'
    INSTEAD_OF_UPDATE = 'instead_of_update'


class PythonEvent(enum.Enum):
    """Python trigger events which are managed
    by the Lorelie ORM."""

    PRE_INIT = 'pre_init'
    POST_INIT = 'post_init'
    BEFORE_CREATE = 'before_create'
    AFTER_CREATE = 'after_create'


class DataTypes(enum.Enum):
    TEXT = 'text'
    INTEGER = 'integer'
    REAL = 'real'
    BLOB = 'blob'
    NULL = 'null'
    BOOLEAN = 'boolean'
    DATE = 'date'
    DATETIME = 'datetime'
    # TIME = 'time'
    TIMESTAMP = 'timestamp'
