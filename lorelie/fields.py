import json

from lorelie.constraints import MaxLengthConstraint


class Field:
    python_type = str
    base_validators = []

    def __init__(self, name, *, max_length=None, null=False, primary_key=False, default=None, unique=False, validators=[]):
        self.constraints = []
        self.name = name
        self.null = null
        self.primary_key = primary_key
        self.default = default
        self.unique = unique
        self.table = None
        self.max_length = max_length
        self.base_validators = self.base_validators + validators
        self.base_field_parameters = {
            'primary key': False,
            'null': False,
            'not null': True,
            'unique': False
        }

        if max_length is not None:
            instance = MaxLengthConstraint(self.max_length, self)
            self.constraints.append(instance)

    def __repr__(self):
        return f'<{self.__class__.__name__}[{self.name}]>'

    def __hash__(self):
        return hash((self.name))

    def __eq__(self, value):
        if isinstance(value, Field):
            return value.name == self.name
        return self.name == value

    @property
    def field_type(self):
        return 'text'

    @classmethod
    def create(cls, name, params):
        instance = cls(name)
        instance.base_field_parameters = params
        if 'null' in params:
            instance.null = True

        if 'primary key' in params:
            instance.primary_key = True
        instance.field_parameters()
        return instance

    def run_validators(self, value):
        for validator in self.base_validators:
            if not callable(validator):
                raise ValueError('Validator should be a callable')
            validator(value)

    def to_python(self, data):
        return data

    def to_database(self, data):
        if callable(data):
            return self.to_python(str(data()))

        if data is None:
            return ''

        if not isinstance(data, self.python_type):
            raise ValueError(
                f"{type(data)} should be an instance "
                f"of {self.python_type}"
            )
        self.run_validators(data)
        return self.to_python(data)

    def field_parameters(self):
        """Adapt the python function parameters to the
        database field creation ones. For example: 

        >>> field = CharField('visited', default=False)
        ... field.field_parameters()
        ... ['visited', 'text', 'not null', 'default', 0]
        """
        field_type = None
        if self.max_length is not None:
            # varchar does not raise constraint. It only
            # says that the field should be of x length
            field_type = f'varchar({self.max_length})'

        initial_parameters = [self.name, field_type or self.field_type]

        if self.null:
            self.base_field_parameters['null'] = True
            self.base_field_parameters['not null'] = False
        else:
            self.base_field_parameters['null'] = False
            self.base_field_parameters['not null'] = True

        self.base_field_parameters['primary key'] = self.primary_key
        self.base_field_parameters['unique'] = self.unique

        if self.default is not None:
            database_value = self.to_database(self.default)
            value = self.table.backend.quote_value(database_value)
            initial_parameters.extend(['default', value])

        true_parameters = list(filter(
            lambda x: x[1] is True,
            self.base_field_parameters.items()
        ))
        additional_parameters = list(map(lambda x: x[0], true_parameters))
        base_field_parameters = initial_parameters + additional_parameters

        for constraint in self.constraints:
            # FIXME: AutoField needs to be setup with
            # AutoField.table.backend which is None otherwise
            # it raises a NoneType error in this section
            constraint_sql = constraint.as_sql(self.table.backend)
            base_field_parameters.append(constraint_sql)

        return base_field_parameters

    def prepare(self, table):
        from lorelie.tables import Table
        if not isinstance(table, Table):
            raise ValueError(f"{table} should be an instance of Table")

        for instance in self.constraints:
            table.field_constraints[self.name] = instance

        self.table = table

    def deconstruct(self):
        return (self.name, self.field_parameters())


class CharField(Field):
    def to_python(self, data):
        return self.python_type(data)


class IntegerField(Field):
    python_type = int

    def __init__(self, name, *, min_value=None, max_value=None, **kwargs):
        self.min_value = min_value
        self.max_value = max_value
        super().__init__(name, **kwargs)

        if min_value is not None:
            instance = MinValueValidator(min_value)
            self.base_validators.append(instance)

        if max_value is not None:
            instance = MaxValueValidator(max_value)
            self.base_validators.append(instance)

    @property
    def field_type(self):
        return 'integer'


class FloatField(Field):
    pass


class JSONField(Field):
    python_type = dict

    def to_python(self, data):
        return json.loads(data)

    def to_database(self, data):
        return json.dumps(data, ensure_ascii=False, sort_keys=True)


class BooleanField(Field):
    truth_types = ['true', 't', 1, '1']
    false_types = ['false', 'f', 0, '0']

    def to_python(self, data):
        if data in self.truth_types:
            return True

        if data in self.false_types:
            return False

    def to_database(self, data):
        if isinstance(data, bool):
            if data == True:
                return 1
            return 0

        if isinstance(data, str):
            if data in self.truth_types:
                return 1

            if data in self.false_types:
                return 0
        return data


class AutoField(IntegerField):
    """Represents an alias to the `rowid` field
    in the database"""

    def __init__(self):
        super().__init__('id', primary_key=True)
        self.base_field_parameters.pop('null')
        self.base_field_parameters.pop('not null')
        self.base_field_parameters['autoincrement'] = True


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


class Value:
    def __init__(self, value, output_field=None):
        self.value = value
        
        if output_field is None:
            output_field = CharField('value_field')
        self.output_field = output_field

    def __repr__(self):
        return f'{self.__class__.__name__}({self.to_database()})'

    def to_database(self):
        return self.output_field.to_database(self.value)

    def as_sql(self, backend):
        return [backend.quote_value(self.to_database())]


class AliasField(Field):
    """A special field that guesses the type
    of the data and returns the correct database
    field. This class is determined for fields in
    the queryset that uses an alias"""

    def __init__(self, name):
        self.name = name
        super().__init__(name)

    def get_data_field(self, data):
        # Infer the data type and return
        # the correct database field
        if isinstance(data, str):
            return CharField(self.name)
        elif isinstance(data, int):
            return IntegerField(self.name)
        return CharField(self.name)
