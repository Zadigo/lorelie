import re
from collections import OrderedDict

from lorelie.backends import SQLiteBackend
from lorelie.exceptions import FieldExistsError, ImproperlyConfiguredError
from lorelie.fields.base import (AutoField, DateField, DateTimeField, Field,
                                 IntegerField)
from lorelie.queries import Query


class BaseTable(type):
    def __new__(cls, name, bases, attrs):
        super_new = super().__new__

        if 'prepare' in attrs:
            new_class = super_new(cls, name, bases, attrs)
            cls.prepare(new_class)
            return new_class
        
        return super_new(cls, name, bases, attrs)

    @classmethod
    def prepare(cls, table):
        pass


class AbstractTable(metaclass=BaseTable):
    # TODO: Remove
    query_class = Query
    backend_class = SQLiteBackend

    def __init__(self):
        self.backend = None
        self.is_prepared = False
        self.field_types = OrderedDict()

    def __hash__(self):
        return hash((self.name, self.verbose_name, *self.field_names))

    def __eq__(self, value):
        return self.name == value

    def __bool__(self):
        return self.is_prepared

    @staticmethod
    def validate_table_name(name):
        result = re.search(r'^(\w+\_?)+$', name)
        if not result:
            raise ValueError(
                "Table name is not a valid name and contains "
                f"invalid carachters: {name}"
            )

        result = re.search(r'\s?', name)
        if result:
            raise ValueError(
                "Table name contains invalid spaces"
            )
        return name.lower()

    # TODO: Rename this to validate_new_values
    def validate_values(self, fields, values):
        """Validate an incoming value in regards
        to the related field the user is trying
        to set on the column. The returned values
        are quoted by default"""
        validated_values = []
        for i, field in enumerate(fields):
            # TODO: Allow creation with id field
            if field == 'rowid' or field == 'id':
                continue

            try:
                field = self.fields_map[field]
            except KeyError:
                raise FieldExistsError(field, self)

            value = list(values)[i]
            validated_value = self.backend.quote_value(
                field.to_database(value)
            )
            validated_values.append(validated_value)
        return validated_values

    def load_current_connection(self):
        from lorelie.backends import connections
        self.backend = connections.get_last_connection()


class Table(AbstractTable):
    """Represents a table in the database. This class
    can be used independently but would require creating
    and managing table creation

    To create a table without using `Database`:

    >>> table = Table('my_table', 'my_database', fields=[Field('url')])
    ... table.prepare()
    ... table.create(url='http://example.come')

    However, if you wish to manage a migration file and other table related
    tasks, wrapping tables in `Database` is the best option:

    >>> table = Table('my_table', 'my_database', fields=[Field('url')])
    ... database = Database('my_database', table)
    ... database.make_migrations()
    ... database.migrate()
    ... database.objects.create('url', url='http://example.com')
    ... database.objects.all('url')
    """

    def __init__(self, name, *, fields=[], index=[], constraints=[], ordering=[], str_field='id'):
        self.name = self.validate_table_name(name)
        self.verbose_name = name.lower().title()
        self.indexes = index
        self.table_constraints = constraints
        self.field_constraints = {}
        # The str_field is the name of the
        # field to be used for representing
        # the column in the BaseRow
        self.str_field = str_field

        self.ordering = set(ordering)

        super().__init__()
        self.fields_map = OrderedDict()
        self.auto_add_fields = set()
        self.auto_update_fields = set()

        non_authorized_names = ['rowid', 'id']
        for field in fields:
            # TODO: This does not work
            # if not issubclass(field.__class__, Field):
            #     raise ValueError(f'{field} should be an instance of Field')

            if field.name in non_authorized_names:
                raise ValueError(
                    f'Invalid name "{field.name}" '
                    f'for field: {field}'
                )

            # Identify the date fields that require either
            # an auto_update or auto_add. Which means that
            # we will need to implement the current date/time
            # when creating or updating said fields
            if isinstance(field, (DateField, DateTimeField)):
                if field.auto_add:
                    self.auto_add_fields.add(field.name)

                if field.auto_update:
                    self.auto_update_fields.add(field.name)

            self.field_types[field.name] = field.field_type

            field.prepare(self)
            # If the user uses the same keys multiple
            # times, leave the error for him to resolve
            # since this will just override the first
            # apparition of the duplicate field multiple
            # times in the map
            self.fields_map[field.name] = field

        # Automatically create an ID field and set
        # it up with the table and backend
        id_field = AutoField()
        # TODO: Call load_current_connection
        id_field.prepare(self)
        self.fields_map['id'] = id_field

        field_names = list(self.fields_map.keys())
        field_names.append('rowid')
        self.field_names = field_names

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.name}>'

    def __eq__(self, table):
        if not isinstance(table, Table):
            return NotImplemented

        return all([
            self.name == table.name,
            self.field_names == table.field_names
        ])

    def __setattr__(self, name, value):
        if name == 'name':
            if re.search(r'\W', value):
                raise ValueError(
                    "The table name should not contain carachters "
                    "such as _, -, @ or %"
                )
        return super().__setattr__(name, value)

    def __contains__(self, value):
        return value in self.field_names

    def __getattribute__(self, name):
        if name == 'backend':
            backend = self.__dict__['backend']
            if backend is None:
                raise ImproperlyConfiguredError(
                    self,
                    "You are trying to use a table outside of a Database "
                    "and therefore calling it without a backend being set"
                )
        return super().__getattribute__(name)

    @staticmethod
    def compare_field_types(*fields):
        """Compare the different field types
        and check if we are dealing with
        mixed types"""
        unique_types = set()
        odd_types = set()

        for field in fields:
            unique_types.add(field.field_type)

            if field.field_type in unique_types:
                odd_types.add(field.field_type)

        return len(odd_types) > 1

    def _add_field(self, field_name, field):
        """Internala function to add a field on the
        database. Returns the newly constructued
        field paramters"""
        if not isinstance(field, Field):
            raise ValueError(f"{field} should be be an instance of Field")

        if field_name != field.name:
            raise ValueError('Name does not match the internal field name')

        if field_name in self.fields_map:
            raise ValueError("Field is already present on the database")

        self.fields_map[field_name] = field

        field_params = self.build_field_parameters()
        field_params = [
            self.backend.simple_join(params)
            for params in field_params
        ]
        return field_params

    # TODO: Rename to check_field
    def has_field(self, name, raise_exception=False):
        result = name in self.fields_map
        if not result and raise_exception:
            raise FieldExistsError(name, self)
        return result

    def get_field(self, name):
        return self.fields_map[name]

    def create_table_sql(self, fields):
        sql = self.backend.CREATE_TABLE.format_map({
            'table': self.name,
            'fields': fields
        })
        return [sql]

    def drop_table_sql(self):
        sql = self.backend.DROP_TABLE.format_map({
            'table': self.name
        })
        return [sql]

    def build_field_parameters(self):
        """Returns the paramaters for all
        the fields present on the current
        table. The parameters are the SQL
        parameters e.g. null, autoincrement
        used to define the field in 
        the database"""
        return [
            field.field_parameters()
            for field in self.fields_map.values()
        ]

    def prepare(self, database):
        """Prepares the table with other parameters, 
        creates the create SQL and then creates the
        different tables in the database using the 
        parameters of the different fields"""
        field_params = self.build_field_parameters()
        field_params = [
            self.backend.simple_join(params)
            for params in field_params
        ]

        if database.has_relationships:
            for _, relationship_map in database.relationships.items():
                if not relationship_map.can_be_validated:
                    raise ValueError(relationship_map.error_message)

                if relationship_map.creates_relationship(self):
                    # We have to create the field automatically
                    # in the fields map of the table
                    field_name = relationship_map.backward_related_field
                    field_params = self._add_field(
                        field_name,
                        IntegerField(field_name, null=False)
                    )

                    relationship_sql = relationship_map.field.as_sql(
                        self.backend
                    )
                    field_params.extend([
                        self.backend.simple_join(relationship_sql)
                    ])

        create_sql = self.create_table_sql(
            self.backend.comma_join(field_params)
        )
        query = self.query_class(table=self)
        query.add_sql_nodes(create_sql)
        query.run(commit=True)
        self.is_prepared = True
