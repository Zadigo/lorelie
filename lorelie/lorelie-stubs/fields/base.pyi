from typing import (Any, Callable, Literal, Tuple, Type, TypedDict, Union, Unpack,
                    override, TypeVar)

from lorelie.backends import SQLiteBackend
from lorelie.constraints import MaxLengthConstraint
from lorelie.tables import Table
import datetime

OutputFieldOptions = TypeVar(
    'OutputFieldOptions',
    CharField,
    DateTimeField,
    DateField,
    EmailField,
    FilePathField,
    FloatField,
    IntegerField,
    JSONField,
    SlugField,
    UUIDField,
    URLField
)


class FieldOptions(TypedDict):
    null: bool
    primary_key: bool
    default: Any
    unique: bool
    validators: list[Callable[[str], None]]


class Field:
    python_type: Type[Union[str, bool, list, dict]] = ...
    base_validators: list[Callable[[Union[str, int]], None]]
    standard_field_types: list[str] = ...
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

    @property
    def is_standard_field_type(self) -> bool: ...

    @staticmethod
    def validate_field_name(name: str) -> str: ...

    @classmethod
    def create(
        cls,
        name: str,
        params: list[str]
    ) -> Field: ...

    def run_validators(self, value: Any) -> None: ...
    def to_python(self, data: Any) -> Any: ...
    def to_database(self, data: Any) -> Any: ...
    def field_parameters(self) -> list[str]: ...
    def prepare(self, table: Table) -> None: ...
    def deconstruct(self) -> Tuple[str, list[str]]: ...


class CharField(Field):
    @override
    @property
    def field_type(self) -> Literal['text']: ...


class IntegerField(Field):
    python_type: Type[int] = ...
    min_value: int = ...
    max_value: int = ...

    @override
    def __init__(
        self,
        name: str,
        *,
        min_value: int = ...,
        max_value: int = ...,
        **kwargs: Unpack[FieldOptions]
    ) -> None: ...

    @override
    @property
    def field_type(self) -> Literal['integer']: ...


class FloatField(Field):
    @override
    @property
    def field_type(self) -> Literal['float']: ...


class JSONField(Field):
    python_type: Type[dict] = ...

    @override
    @property
    def field_type(self) -> Literal['dict']: ...

    @override
    def to_python(self, data: str) -> dict: ...

    @override
    def to_database(self, data: dict) -> str: ...


class BooleanField(Field):
    truth_types: list[Union[str, int]] = ...
    false_types: list[Union[str, int]] = ...

    @override
    @property
    def field_type(self) -> Literal['bool']: ...

    @override
    def to_python(self, data: Union[str, bool]) -> bool: ...

    @override
    def to_database(self, data: Union[str, bool]) -> int: ...


class AutoField(IntegerField):
    python_type: Type[int] = ...


class DateFieldMixin:
    date_format: str = ...
    python_type: Type[str] = ...
    auto_update: bool = Literal[False]
    auto_add: bool = Literal[False]

    @override
    def __init__(
        self,
        name: str,
        *,
        auto_update: bool = Literal[False],
        auto_add: bool = Literal[False],
        **kwargs
    ) -> None: ...

    def parse_date(self, d: str) -> datetime.datetime: ...


class DateField(DateFieldMixin, Field):
    @override
    @property
    def field_type(self) -> Literal['datetime.date']: ...

    @override
    def to_python(self, data: str) -> datetime.date: ...

    @override
    def to_database(self, data: str) -> datetime.date: ...


class DateTimeField(DateFieldMixin, Field):
    @override
    @property
    def field_type(self) -> Literal['datetime.datetime']: ...

    @override
    def to_python(self, data: str) -> datetime.date: ...

    @override
    def to_database(self, data: str) -> datetime.date: ...


class TimeField(DateTimeField):
    @override
    @property
    def field_type(self) -> Literal['datetime.time']: ...


class EmailField(CharField):
    ...


class FilePathField(CharField):
    ...


class SlugField(CharField):
    ...


class UUIDField(Field):
    ...


class URLField(CharField):
    ...


class Value:
    output_field: Union[
        CharField,
        DateTimeField,
        DateField,
        EmailField,
        FilePathField,
        FloatField,
        IntegerField,
        JSONField,
        SlugField,
        UUIDField,
        URLField
    ]
    value: Any = ...

    def __init__(
        self,
        value: Any,
        output_field: OutputFieldOptions = ...
    ) -> None: ...

    def __repr__(self) -> str: ...

    def to_database(self) -> Union[str, list, dict, int, float]: ...
    def as_sql(self, backend: SQLiteBackend) -> list[str]: ...


class AliasField(Field):
    name: str = ...

    def __init__(self, name: str) -> None: ...

    def get_data_field(
        self,
        data: Any
    ) -> Union[CharField, IntegerField, DateTimeField, DateField, JSONField]: ...
