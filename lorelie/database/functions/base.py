from typing import ClassVar, Optional
from lorelie.lorelie_typings import TypeSQLiteBackend
from abc import ABC, abstractmethod


class Functions(ABC):
    template_sql: ClassVar[Optional[str]] = None
    allow_aggregation: ClassVar[bool] = False

    def __init__(self, field_name: str):
        self.field_name = field_name

    def __repr__(self):
        return f'{self.__class__.__name__}({self.field_name})'

    @property
    def alias_field_name(self):
        """Potential alias name that can be used
        if this function is no name as provided via an 
        explicit alias"""
        return f'{self.__class__.__name__.lower()}_{self.field_name}'

    @staticmethod
    def create_function(connection):
        """Use this function to register a local
        none existing function in the database
        function space in order to use none
        conventional functions"""
        return NotImplemented

    @property
    def internal_type(self):
        return 'function'

    @abstractmethod
    def as_sql(self, backend: TypeSQLiteBackend):
        return NotImplemented
