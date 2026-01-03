from typing import Any, Callable, Optional
import uuid
import datetime
import decimal
import json
import re
from decimal import Decimal
from functools import cached_property
from urllib.parse import unquote

from lorelie.constraints import (MaxLengthConstraint, MaxValueConstraint,
                                 MinValueConstraint)
from lorelie.exceptions import ValidationError
from lorelie.lorelie_typings import TypeAny, TypeAnyNormalTypes, TypeConstraint, TypeDeconstructedField, TypeTable
from lorelie.validators import url_validator


class Field:
    python_type = str
    base_validators: list[Callable[[Any], None]] = []
    default_field_errors: dict[str, str] = {}

    def __init__(self, name: str, *, max_length: int = None, null: bool = False, primary_key: bool = False, default=None, unique: bool = False, validators: list[Callable[[Any], None]] = [], verbose_name: str = None, editable: bool = False):
        self.constraints: list[TypeConstraint] = []
        self.name: str = self.validate_field_name(name)
        self.verbose_name: str = verbose_name
        self.editable: bool = editable
        self.null: bool = null
        self.primary_key: bool = primary_key
        self.default = default
        self.unique: bool = unique
        self.table: TypeTable = None
        self.max_length: int = max_length
        self.base_validators = self.base_validators + validators
        self.standard_field_types = ['text', 'integer', 'blob', 'real', 'null']
        self.is_relationship_field = False
        self.index = 0
        self.base_field_parameters = {
            'primary key': False,
            'not null': True,
            'unique': False
        }

        if max_length is not None:
            instance = MaxLengthConstraint(self.max_length, self)
            self.constraints.append(instance)

    def __repr__(self):
        return f'<{self.__class__.__name__}[{self.name}]>'

    def __hash__(self):
        return hash((self.name, self.field_type))

    def __eq__(self, value):
        if not isinstance(value, Field):
            return NotImplemented

        return any([
            self.name == value.name,
            self.field_type == value.field_type
        ])

    @property
    def field_type(self):
        """The field type is the type for
        the field that will be registered
        in the database. SQLite has converters
        that allows us then convert the data
        back to its Python representation"""
        return 'text'

    @property
    def field_python_name(self):
        return self.__class__.__name__

    @property
    def is_standard_field_type(self):
        return self.field_type in self.standard_field_types

    @staticmethod
    def validate_field_name(name: str):
        result = re.match(r'^(\w+\_?)+$', name)
        if not result:
            raise ValueError(
                "Field name is not a valid name and contains "
                f"invalid spaces or caracters: {name}"
            )

        result = re.search(r'\s+', name)
        if result:
            raise ValueError(
                f'Field name "{name}" contains invalid spaces'
            )
        return name.lower()

    @classmethod
    def create(cls, name: str, params: dict[str, bool]):
        instance = cls(name)
        instance.base_field_parameters = params
        if 'null' in params:
            instance.null = True

        if 'primary key' in params:
            instance.primary_key = True
        instance.field_parameters()
        return instance

    def run_validators(self, value: Any):
        for validator in self.base_validators:
            if not callable(validator):
                raise ValueError('Validator should be a callable')
            validator(value)

    def to_python(self, data: Any) -> TypeAny:
        return data

    def to_database(self, data: TypeAnyNormalTypes) -> TypeAnyNormalTypes:
        """The `to_database` adapts certain types of values
        e.g. dict to the sqlite database. Each subclass should
        define their specific logic for normalizing the data
        for the database after calling this method"""
        if callable(data):
            data = data()

        if data is None or data == '':
            return ''

        # 1. Check that the data matches the python
        # type of the field we are trying to set
        if not isinstance(data, self.python_type):
            raise TypeError(
                f"Value '{data}' for column '{self.name}' "
                f"should be an instance of {self.python_type}"
            )

        self.run_validators(data)
        return data

    def field_parameters(self):
        """Adapts the python function parameters passed within
        the fields to usable SQL text statements:

        >>> field = BooleanField('visited', default=False)
        ... field.field_parameters()
        ... ['visited', 'integer', 'not null', 'default', 0]
        """
        field_type: str = None
        if self.max_length is not None:
            # varchar does not raise constraint. It only
            # says that the field should be of x length
            field_type = f'varchar({self.max_length})'

        initial_parameters = [self.name, field_type or self.field_type]

        if not self.null:
            # Columns are nullable by default so only
            # indicate not null if the field is not nullable
            self.base_field_parameters['not null'] = True

        self.base_field_parameters['primary key'] = self.primary_key
        if self.primary_key:
            # Primary keys are not null by default so there's not
            # need to indicate both
            self.base_field_parameters['not null'] = False

        self.base_field_parameters['unique'] = self.unique

        if self.default is not None:
            database_value = self.to_database(self.default)

            try:
                value = self.table.backend.quote_value(database_value)
            except:
                raise AttributeError(
                    "Field does not seem to be associated to a table "
                    "and therefore cannot its default value"
                )
            else:
                initial_parameters.extend(['default', value])

        _additional_parameters: list[tuple[str, bool]] = list(
            filter(
                lambda x: x[1] is True,
                self.base_field_parameters.items()
            )
        )
        additional_parameters = list(
            map(
                lambda x: x[0],
                _additional_parameters
            )
        )

        base_field_parameters = initial_parameters + additional_parameters

        for constraint in self.constraints:
            # FIXME: AutoField needs to be setup with
            # AutoField.table.backend which is None otherwise
            # it raises a NoneType error in this section
            try:
                constraint_sql = constraint.as_sql(self.table.backend)
            except:
                raise ExceptionGroup(
                    "An exception occured while trying to build "
                    f"the field parameters for {self}",
                    [
                        AttributeError(
                            "Field does not seem to be associated to a table "
                            "and therefore cannot build its constraints"
                        )
                    ]
                )
            else:
                base_field_parameters.append(constraint_sql)

        return base_field_parameters

    def prepare(self, table: TypeTable):
        from lorelie.database.tables.base import Table
        if not isinstance(table, Table):
            raise ValueError(
                f"{table} should be an "
                "instance of Table"
            )

        # Register the constraints present
        # on the field at the table level
        # TODO: Don't think this is necessary anymore
        for instance in self.constraints:
            table.field_constraints[self.name] = instance

        self.table = table

    def deconstruct(self) -> TypeDeconstructedField:
        """Deconstruct the field into its
        components in order to recreate it
        later on"""
        params = {
            'null': self.null,
            'primary_key': self.primary_key,
            'default': self.default,
            'unique': self.unique,
            'editable': self.editable
        }

        if self.max_length is not None:
            params['max_length'] = self.max_length

        return (self.__class__.__name__, self.name, params)


class CharField(Field):
    def to_database(self, data: Any) -> TypeAny:
        if callable(data):
            data = data()
        return super().to_database(str(data))


class NumericFieldMixin:
    def __init__(self, name: str, *, min_value: Optional[int] = None, max_value: Optional[int] = None, **kwargs: str):
        self.min_value = min_value
        self.max_value = max_value

        super().__init__(name, **kwargs)

        if min_value is not None:
            instance = MinValueConstraint(min_value, self)
            self.constraints.append(instance)

        if max_value is not None:
            instance = MaxValueConstraint(max_value, self)
            self.constraints.append(instance)


class IntegerField(NumericFieldMixin, Field):
    python_type = int

    @property
    def field_type(self):
        return 'integer'

    def to_database(self, data: Any) -> TypeAny:
        if isinstance(data, str):
            if data.isnumeric() or data.isdigit():
                return super().to_database(self.python_type(data))
        elif isinstance(data, float):
            return super().to_database(self.python_type(data))
        return super().to_database(data)


class FloatField(NumericFieldMixin, Field):
    python_type = float

    def to_python(self, data: Any) -> TypeAny:
        if data is None or data == '':
            return data

        if isinstance(data, int):
            return self.python_type(data)

        if isinstance(data, float):
            return data

        try:
            return self.python_type(data)
        except (TypeError, ValueError):
            raise ValidationError(
                "The value for {name} is not valid",
                name=self.name
            )


class DecimalField(NumericFieldMixin, Field):
    def __init__(self, name: str, digits: Optional[int] = None, **kwargs: str):
        super().__init__(name, **kwargs)
        if digits is None:
            raise ValueError(f'{digits} should be an integer')
        self.digits = digits

    @cached_property
    def build_context(self):
        return decimal.Context(prec=self.digits)

    def to_python(self, data):
        try:
            if isinstance(data, float):
                return self.build_context.create_decimal_from_float(data)
            return Decimal(data)
        except Exception as e:
            raise ValidationError(e.args)

    def to_database(self, data):
        decimal_value = self.build_context.create_decimal(data)
        return str(decimal_value)


class JSONField(Field):
    python_type = (dict, list)

    # @property
    # def field_type(self):
    #     return 'dict'

    def to_python(self, data: str) -> TypeAny:
        if data is None or data == '':
            return data

        try:
            return json.loads(data)
        except json.JSONDecodeError:
            raise ValidationError(
                "The value for {name} is not valid",
                name=self.name
            )

    def to_database(self, data: Any) -> TypeAny:
        clean_data = super().to_database(data)
        return json.dumps(clean_data, ensure_ascii=False, sort_keys=True)


class BooleanField(Field):
    python_type = (bool, int)
    truth_types = ['true', 't', 1, '1']
    false_types = ['false', 'f', 0, '0']

    @property
    def field_type(self):
        return 'boolean'

    def to_database(self, data: Any) -> TypeAny:
        if data in self.truth_types:
            return super().to_database(1)

        if data in self.false_types:
            return super().to_database(0)
        return super().to_database(data)


class AutoField(IntegerField):
    """Represents an alias to the `rowid` field
    in the database"""

    def __init__(self):
        super().__init__('id', primary_key=True)
        self.base_field_parameters.pop('not null')
        self.base_field_parameters['autoincrement'] = True


class DateFieldMixin:
    python_type = str
    date_format = '%Y-%m-%d'

    def __init__(self, name: str, *, auto_update: bool = False, auto_add: bool = False, **kwargs):
        self.auto_update = auto_update
        self.auto_add = auto_add

        if self.auto_update or self.auto_add:
            kwargs['null'] = True

        super().__init__(name, **kwargs)

    def parse_from_format(self, data: Any, formats: tuple[str, ...]) -> datetime.datetime:
        d = None
        for f in formats:
            try:
                d = datetime.datetime.strptime(data, f)
            except:
                continue

        if d is None:
            raise ValueError("Date format could not be identified")
        return d


class DateField(DateFieldMixin, Field):
    """
    * `auto_add` will update the field with the
      current date every time a value is created

    * `auto_update` will update the field with the
      current date every time a value is updated
    """

    @property
    def field_type(self):
        return 'date'

    # def field_parameters(self):
    #     self.base_field_parameters.update(current_date=self.auto_add)
    #     return super().field_parameters()

    def to_database(self, data: Any) -> TypeAny:
        if data == '' or data is None:
            return data

        clean_data = ''

        if isinstance(data, str):
            formats = (
                '%Y-%m-%d',
                '%Y-%m-%d %H:%M:%S.%f',
                '%Y-%m-%d %H:%M:%S.%f%z'
            )

            d = self.parse_from_format(data, formats)
            d = d.date()
            self.run_validators(d)
            clean_data = self.python_type(d)

        if hasattr(data, 'date'):
            data = getattr(data, 'date')
            d = data()
            self.run_validators(d)
            clean_data = self.python_type(d)

        # TODO: Auto update the times at the field level
        # if self.auto_add or self.auto_update:
        #     clean_data = str(datetime.datetime.now())
        return clean_data


class DateTimeField(DateFieldMixin, Field):
    """The `DateTimeField` class is a specialized field for 
    handling date and time data in your database models. It extends 
    `DateFieldMixin` and Field, providing additional functionality 
    specifically for date and time fields.

    * `auto_add` will update the field with the
      current date every time a value is created

    * `auto_update` will update the field with the
      current date every time a value is updated
    """

    date_format = '%Y-%m-%d %H:%M:%S.%f%z'

    @property
    def field_type(self):
        return 'datetime'

    # def field_parameters(self):
    #     self.base_field_parameters.update(current_timestamp=self.auto_add)
    #     return super().field_parameters()

    def to_database(self, data: Any) -> TypeAny:
        if data == '' or data is None:
            return data

        clean_data = ''

        if isinstance(data, str):
            formats = (
                '%Y-%m-%d %H:%M:%S.%f',
                '%Y-%m-%d %H:%M:%S.%f%z'
            )

            clean_data = self.parse_from_format(data, formats)
            self.run_validators(clean_data)
            clean_data = str(clean_data)

        if hasattr(data, 'date'):
            self.run_validators(data)
            clean_data = str(data)

        # TODO: Auto update the times at the field level
        # if self.auto_add or self.auto_update:
        #     clean_data = str(datetime.datetime.now())

        return clean_data


class TimeField(DateTimeField):
    date_format = '%H:%M:%S'

    # def field_parameters(self):
    #     self.base_field_parameters.update(current_time=self.auto_add)
    #     return super().field_parameters()

    def to_database(self, data: Any) -> TypeAny:
        clean_data = super().to_database(data)
        return datetime.datetime.fromisoformat(clean_data)


class EmailField(CharField):
    base_validators = []


class FilePathField(CharField):
    base_validators = []


class SlugField(CharField):
    pass


class UUIDField(Field):
    def __init__(self, name: str, **kwargs: Any):
        super().__init__(name, **kwargs)
        self.python_type = (str, uuid.UUID)

    def to_database(self, data):
        if data is None:
            return None

        if isinstance(data, uuid.UUID):
            return str(data)

        if callable(data):
            return str(data())

        return data.hex

    def to_python(self, data: Any) -> TypeAny:
        if not isinstance(data, uuid.UUID):
            try:
                param = 'hex' if isinstance(data, int) else 'hex'
                return uuid.UUID(**{param: data})
            except:
                raise ValidationError('uuid is not valid')
        return data


class URLField(CharField):
    base_validators = [url_validator]

    def to_database(self, data: Any) -> TypeAny:
        if data is None or data == '':
            return data
        return super().to_database(unquote(data))


class BinaryField(Field):
    pass


class CommaSeparatedField(CharField):
    base_validators = []

    def to_python(self, data: Any) -> list[str]:
        if data is None or data == '':
            return []
        
        return data.split(',')

    def to_database(self, data: Any) -> TypeAny:
        if isinstance(data, (list, set, tuple)):
            data = ','.join(data)
        return super().to_database(data)


class AliasField(Field):
    """A special field that guesses the type
    of the data and returns the correct database
    field. This class is determined for fields in
    the queryset that uses an alias"""

    def __init__(self, name: str):
        super().__init__(name)

    def to_database(self, data):
        return None

    def get_data_field(self, data: TypeAny) -> Field:
        # Infer the data type and return
        # the correct database field
        if isinstance(data, str):
            if data.isdigit() or data.isnumeric():
                return IntegerField(self.name)
            return CharField(self.name)
        elif isinstance(data, int):
            return IntegerField(self.name)
        elif isinstance(data, datetime.date):
            return DateField(self.name)
        elif isinstance(data, datetime.datetime):
            return DateTimeField(self.name)
        elif isinstance(data, (list, dict)):
            return JSONField(self.name)
        else:
            return CharField(self.name)
