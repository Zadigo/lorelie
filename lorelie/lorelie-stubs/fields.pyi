from typing import (Any, Callable, Tuple, Type, TypedDict, Union, Unpack,
                    override)

from lorelie.constraints import MaxLengthConstraint
from lorelie.tables import Table

class FieldOptions(TypedDict):
    null: bool
    primary_key: bool
    default: Any
    unique: bool
    validators: list[Callable[[str], None]]


class Field:
    python_type: Type[Union[str, bool, list, dict]] = ...
    base_validators: list[Callable[[Union[str, int]], None]]
    base_constraints: list[MaxLengthConstraint] = ...
    name: str = ...
    null: bool = ...
    primary_key: bool = ...
    default: Any = ...
    unique: bool = ...
    table: Table = ...
    max_length: int = ...,
    base_field_parameters: dict[str, bool] = ...

    def __init__(
        self,
        name: str,
        *,
        max_length: int = ...,
        null: bool = ...,
        primary_key: bool = ...,
        default: Any = ...,
        unique: bool = ...,
        validators: list[Callable[[str], None]] = ...
    ) -> None: ...

    def __repr__(self) -> str: ...
    def __hash__(self) -> int: ...
    def __eq__(self, value: str) -> bool: ...

    @property
    def field_type(self) -> str: ...

    @classmethod
    def create(
        cls,
        name: str,
        params: list[str]
    ) -> Field: ...

    def run_validators(self, value: Any) -> None: ...
    def to_python(self, data: Any) -> Any: ...
    def to_database(self, data: Any) -> str: ...
    def field_parameters(self) -> list[str]: ...
    def prepare(self, table: Table) -> None: ...
    def deconstruct(self) -> Tuple[str, list[str]]: ...


class CharField(Field):
    ...


class IntegerField(Field):
    python_type: Type[int] = ...
    min_value: int = ...
    max_value: int = ...

    def __init__(
        self,
        name: str,
        *,
        min_value: int = ...,
        max_value: int = ...,
        **kwargs: Unpack[FieldOptions]
    ) -> None: ...


class FloatField(Field):
    pass


class JSONField(Field):
    python_type: Type[dict] = ...

    @override
    def to_python(self, data: str) -> dict: ...

    @override
    def to_database(self, data: dict) -> str: ...


class BooleanField(Field):
    truth_types: list[Union[str, int]] = ...
    false_types: list[Union[str, int]] = ...

    @override
    def to_python(self, data: Union[str, bool]) -> bool: ...

    @override
    def to_database(self, data: Union[str, bool]) -> int: ...


class AutoField(Field):
    python_type: Type[int] = ...


class DateField(Field):
    pass


class DateTimeField(Field):
    pass


class TimeField(Field):
    pass


class EmailField(CharField):
    pass


class FilePathField(CharField):
    pass


class SlugField(CharField):
    pass


class UUIDField(Field):
    pass
