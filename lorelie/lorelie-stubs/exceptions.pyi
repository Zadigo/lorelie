from lorelie.database.tables.base import Table


class ImproperlyConfiguredError(Exception):
    def __init__(self, table: Table, message: str) -> None: ...


class TableExistsError(Exception):
    message: str = ...

    def __init__(self, name: str) -> None: ...


class NoDatabaseError(Exception):
    message: str = ...

    def __init__(self, table: Table) -> None: ...


class FieldExistsError(Exception):
    def __init__(self, name: str, table: Table) -> None: ...


class ValidationError(Exception):
    def __init__(self, message: str, **kwargs) -> None: ...


class MigrationsExistsError(Exception):
    def __init__(self) -> None: ...


class ConnectionExistsError(Exception):
    message: str = ...

    def __init__(self) -> None: ...
