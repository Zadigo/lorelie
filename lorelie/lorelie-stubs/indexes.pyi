from lorelie.backends import SQLiteBackend

class Index:
    prefix: str = ...
    index_name: str =  ...
    _fields: list[str] = ...
    _backend: SQLiteBackend = ... 

    def __init__(self, name: str, *fields: str) -> None: ...

    def __repr__(self) -> str: ...
    def function_sql(self) -> str: ...
