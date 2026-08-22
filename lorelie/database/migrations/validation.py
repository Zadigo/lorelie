import datetime
from typing import Annotated, Any

import pydantic
from pydantic import BeforeValidator, model_validator

from lorelie.lorelie_typings import (
    NullableType,
    TypeDeconstructedField,
    TypeDeconstructedIndex,
)


def validate_id(value: Any):
    if value is None:
        return None

    if not isinstance(value, str):
        raise TypeError('ID should be a string')

    return value


def validate_date(value: Any):
    if value is None:
        return None

    str_times = [
        '%Y-%m-%dT%H:%M:%S.%f%z',
        '%Y-%m-%d %H:%M:%S.%f%z',
        '%Y-%m-%d %H:%M:%S.%f',
    ]

    for str_time in str_times:
        try:
            d = datetime.datetime.strptime(value, str_time)
            break
        except ValueError:
            continue
    else:
        raise ValueError(f"Time data '{value}' does not match any of the expected formats")

    return str(d)


class SchemaFieldParams(pydantic.BaseModel):
    null: bool
    primary_key: bool
    default: Any | None = None
    unique: bool
    editable: bool
    max_length: int | None = None


class SchemaTable(pydantic.BaseModel):
    name: str
    fields: list
    indexes: list = pydantic.Field(..., default_factory=list)
    constraints: list = pydantic.Field(..., default_factory=list)
    ordering: list = pydantic.Field(..., default_factory=list)
    str_field: str

    @model_validator(mode='before')
    @classmethod
    def validate_fields(cls, data: Any):
        fields: TypeDeconstructedField = data.get('fields', [])
        for field_type, name, params in fields:
            SchemaFieldParams(**params)
        return data


class JsonSchema(pydantic.BaseModel):
    name: str
    tables: list[SchemaTable]


class JsonMigrationSchema(pydantic.BaseModel):
    """Represents the structure of the JSON migration file which is used to
    track the different states of the database and its tables across different
    migration runs. It is used as a reference for the Migrations class to
    determine the necessary operations to perform on the database in order to
    update it to the latest state as defined by the user in their codebase
    and the existing migration file (if any)

    .. code-block:: json
        {"id": None, "date": None", "number": 0, "migrated": False, "schema": {}}
    
    Attributes:
        id (Optional[str]): A unique identifier for the migration, generated as a random hexadecimal string. It is used to track different migration runs and can be useful for debugging and reference purposes.
        date (Optional[str]): The date and time when the migration was created or last updated, stored as a string. It is used to track the timeline of migrations and can be useful for debugging and reference purposes.
        number (Optional[int]): A sequential number representing the migration version. It is incremented with each migration run and is used to track the progression of migrations over time.
        migrated (bool): A boolean flag indicating whether the migration has been applied to the database. It is used to determine whether the database is up to date with the latest migration schema and can be useful for conditional logic in the migration process.
        in_memory (bool): A boolean flag indicating whether the database is an in-memory database. It is used to determine the migration strategy, as in-memory databases need to be recreated with each migration run, while physical databases can be altered in place.
        schema (dict): A dictionary representing the current state of the database schema, including the tables, fields, indexes, and other relevant metadata.
    """
     
    id: Annotated[str, BeforeValidator(validate_id)]
    date: Annotated[str, BeforeValidator(validate_date)]
    number: int
    migrated: bool = False
    in_memory: bool = False
    database_schema: JsonSchema | None = pydantic.Field(default=None)

    @model_validator(mode='before')
    @classmethod
    def validate_schema(cls, data: Any):
        print(data)
        return data

    @property
    def _table_names(self) -> set[str]:
        if self.database_schema is None:
            return set()
        return {item.name for item in self.database_schema.tables}

    def get_table_indexes(self, table_name: str) -> list[TypeDeconstructedIndex]:
        table = self.get_table(table_name)
        return table.indexes if table is not None else []

    def get_table(self, table_name: str) -> SchemaTable | None:
        """Returns the table schema for a given table
        in the current migration schema"""
        for item in self.database_schema.tables:
            if item.name == table_name:
                return SchemaTable(**item)
        return None

    def get_table_fields(self, table_name: str) -> NullableType[list[TypeDeconstructedField]]:
        """Returns the fields map for a given table
        in the current migration schema"""
        json_table = self.get_table(table_name)
        return json_table.fields if json_table is not None else None

    def get_table_field(self, table_name: str, field_name: str) -> NullableType[TypeDeconstructedField]:
        """Returns the field parameters for a given field
        in a given table from the current migration schema"""
        json_fields = self.get_table_fields(table_name)
        if json_fields:
            for field_type, name, params in json_fields:
                if name == field_name:
                    return (field_type, name, params)
        return None

    def table_has_field(self, table_name: str, field_name: str) -> bool:
        """Checks whether a given table has a field in the
        current migration schema"""
        return self.get_table_field(table_name, field_name) is not None
