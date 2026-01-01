import re
from collections import OrderedDict
from typing import Any, ClassVar, Generic, Optional, Type

from lorelie.backends import SQLiteBackend
from lorelie.constraints import CheckConstraint, UniqueConstraint
from lorelie.database.indexes import Index
from lorelie.database.manager import DatabaseManager
from lorelie.database.tables.columns import Column
from lorelie.exceptions import FieldExistsError, ImproperlyConfiguredError
from lorelie.fields.base import AutoField, DateField, DateTimeField, Field
from lorelie.lorelie_typings import TypeConstraint, TypeDatabase, TypeField, TypeIndex, TypeSQLiteBackend
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
    query_class: ClassVar[Type[Query]] = Query

    backend_class: ClassVar[Type[SQLiteBackend]] = SQLiteBackend
    objects: ClassVar[DatabaseManager] = DatabaseManager()

    def __init__(self):
        self.backend: Optional[TypeSQLiteBackend] = None
        self.is_prepared: bool = False
        self.field_types = OrderedDict()
        self.database: Optional[TypeDatabase] = None

    def __hash__(self):
        return hash((self.name, self.verbose_name, *self.field_names))

    def __eq__(self, value):
        return self.name == value

    def __bool__(self):
        return self.is_prepared

    @staticmethod
    def validate_table_name(name: str) -> str:
        if name == 'objects':
            raise ValueError(
                "Table name uses a reserved "
                "keyword: objects"
            )

        result = re.search(r'^(\w+\_?)+$', name)
        if not result:
            raise ValueError(
                "Table name is not a valid name and contains "
                f"invalid carachters: {name}"
            )

        result = re.search(r'\s+', name)
        if result:
            raise ValueError("Table name contains invalid spaces")

        return name.lower()

    def validate_values_from_list(self, values):
        for value in values:
            yield self.validate_values_from_dict(value)

    def validate_values_from_dict(self, values: dict[str, Any]):
        fields, values = self.backend.dict_to_sql(values, quote_values=False)
        return self.validate_values(fields, values)

    def validate_values(self, fields: list[str], values: list[Any]):
        """Validate a set of values that the user is 
        trying to insert or update in the database

        >>> validate_values(['name'], ['Kendall'])
        ... (["'Kendall'"], {'name': "'Kendall'"})
        """
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
            clean_value = field.to_database(value)
            validated_value = self.backend.quote_value(clean_value)
            validated_values.append(validated_value)

        validated_dict_values = dict(zip(fields, validated_values))
        return validated_values, validated_dict_values

    def load_current_connection(self):
        """Loads the current SQLite connection and attaches
        it to the table instance for further usage"""
        from lorelie.backends import connections
        self.backend = connections.get_last_connection()


class Table(Generic[TypeField], AbstractTable):
    """To create a table in the SQLite database, you first need to 
    create an instance of the Table class and then use it with the 
    Database class, which represents the actual database. The make_migrations 
    function can be called to generate a JSON file that contains all 
    the historical changes made to the database.

    Represents a table in your database which is managed within a Database class. 
    This setup allows you to handle migration files and perform various 
    table-related tasks:

    >>> table = Table('my_table', 'my_database', fields=[Field('url')])
    ... database = Database('my_database', table)
    ... database.make_migrations()
    ... database.migrate()
    ... database.objects.create('url', url='http://example.com')
    ... database.objects.all('url')
    """

    def __init__(self, name: str, *, fields: list[TypeField] = [], indexes: list[TypeIndex] = [], constraints: list[TypeConstraint] = [], ordering: list[str] = [], str_field='id'):
        self.name = self.validate_table_name(name)
        self.verbose_name = name.lower().title()
        self.indexes = indexes
        self.table_constraints = constraints
        self.field_constraints = {}
        self.is_foreign_key_table = False
        self.attached_to_database: Optional[TypeDatabase] = None
        self.columns_map: dict[str, Column] = {}

        # The str_field is the name of the
        # field to be used for representing
        # the column in the BaseRow
        self.str_field = str_field

        self.field_counter: int = 0
        self.ordering = set(ordering)

        super().__init__()
        self.fields_map: OrderedDict[str, TypeField] = OrderedDict()
        self.auto_add_fields: set[str] = set()
        self.auto_update_fields: set[str] = set()

        non_authorized_names = ['rowid', 'id']
        for i, field in enumerate(fields):
            if not hasattr(field, 'prepare'):
                raise ValueError(f"{field} should be an instance of Field")

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

            field.index = self.field_counter
            field.prepare(self)
            # If the user uses the same keys multiple
            # times, leave the error for him to resolve
            # since this will just override the first
            # apparition of the duplicate field multiple
            # times in the map
            # TODO: Delegate this section to the prepare
            # method of the field directly
            self.fields_map[field.name] = field
            self.field_counter += 1

        # Automatically create an ID field and set
        # it up with the table and backend
        id_field = AutoField()
        id_field.prepare(self)
        self.field_types['id'] = id_field.field_type
        self.fields_map['id'] = id_field
        self.field_counter += 1
        id_field.index = self.field_counter

        field_names = list(self.fields_map.keys())
        field_names.append('rowid')
        self.field_names = field_names

        for index in indexes:
            if not isinstance(index, Index):
                raise ValueError(f'{index} should be an instance of Index')
            index.prepare(self)

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.name}>'

    def __eq__(self, value: Any):
        if not isinstance(value, Table):
            return any([
                value == self.name,
                value in self.field_names
            ])

        return all([
            self.name == value.name,
            self.field_names == value.field_names
        ])

    def __contains__(self, value: Any):
        return value in self.field_names

    def __setattr__(self, name, value):
        if name == 'name':
            pass
        return super().__setattr__(name, value)

    # REMOVE:
    def __getattribute__(self, name: str) -> Any:
        if name == 'backend':
            backend = self.__dict__['backend']
            if backend is None:
                raise ImproperlyConfiguredError(
                    self,
                    "You are trying to use a table outside of a Database "
                    "and therefore calling it without the backend being set "
                    "on the table instance"
                )
        return super().__getattribute__(name)

    @staticmethod
    def is_mixed_type_fields(*fields: TypeField) -> bool:
        """Compare the different field types and check if we 
        are dealing with mixed types

        Args:
            fields (TypeField): The fields to compare
        """
        seen_types = []

        for field in fields:
            seen_types.append(field.field_type)

        unique_types = set(seen_types)
        return len(unique_types) > 1

    def _add_field(self, field_name: str, field: TypeField):
        """Internala function to add a field on the
        database. Returns the newly constructued
        field parameters"""
        if not isinstance(field, Field):
            raise ValueError(f"{field} should be be an instance of Field")

        if field_name != field.name:
            raise ValueError(
                f"Field name '{field_name}' does not match the "
                f"field's internal name '{field.name}'. You are trying "
                "to add a field on the table where the names do not match"
            )

        if field_name in self.fields_map:
            raise ValueError("Field is already present on the database")

        self.fields_map[field_name] = field
        self.field_names = list(self.fields_map.keys())

        field_params = self.build_all_field_parameters()
        field_params = [
            self.backend.simple_join(params)
            for params in field_params
        ]

        sorted_columns = sorted(
            self.columns_map.values(),
            key=lambda x: x.index
        )

        if self.field_names and not sorted_columns:
            raise ValueError(
                "Cannot add field to table without columns_map "
                "being populated. Please make sure the table is prepared"
            )

        self.field_counter += 1
        field.index = self.field_counter
        return field_params

    # TODO: Rename to check_field
    def has_field(self, name: str, raise_exception: bool = False) -> bool:
        result = name in self.fields_map
        if not result and raise_exception:
            raise FieldExistsError(name, self)
        return result

    def get_field(self, name: str):
        return self.fields_map[name]

    def create_table_sql(self, joined_fields_str: str):
        """Generates the SQL statement required to create
        the current table in the database"""
        unique_constraints = []
        check_constraints = []

        for constraint in self.table_constraints:
            constraint_sql = constraint.as_sql(self.backend)

            if isinstance(constraint, UniqueConstraint):
                unique_constraints.append(constraint_sql)
            elif isinstance(constraint, CheckConstraint):
                check_constraints.append(constraint_sql)

        # Unique constraints are comma separated
        # just like normal fields while check_constraints
        # are just joined by normal space
        joined_unique = self.backend.comma_join(
            [
                joined_fields_str,
                *unique_constraints
            ]
        )

        joined_all = self.backend.simple_join(
            [joined_unique, *check_constraints]
        )

        sql = self.backend.CREATE_TABLE.format_map({
            'table': self.name,
            'fields': joined_all
        })
        return [sql]

    def drop_table_sql(self):
        sql = self.backend.DROP_TABLE.format_map({'table': self.name})
        return [sql]

    def create_index_sql(self, index: TypeIndex):
        sql = index.as_sql(self.backend)
        return [sql]

    def drop_index_sql(self, index: TypeIndex):
        sql = self.backend.DROP_INDEX.format_map({'value': index.name})
        return [sql]

    # def create_field_sql(self, field: TypeField):
    #     field_params = field.field_parameters()
    #     joined_params = self.backend.simple_join(field_params)

    #     sql = self.backend.ADD_COLUMN.format_map({
    #         'table': self.name,
    #         'field_definition': joined_params
    #     })
    #     return [sql]

    # def alter_field_sql(self, field: TypeField):
    #     field_params = field.field_parameters()
    #     joined_params = self.backend.simple_join(field_params)

    #     sql = self.backend.ALTER_COLUMN.format_map({
    #         'table': self.name,
    #         'field_definition': joined_params
    #     })
    #     return [sql]

    def build_all_field_parameters(self):
        """Returns the paramaters for all
        the fields present on the current
        table. The parameters are the SQL
        parameters e.g. null, autoincrement
        used to define/create the field in 
        the database on creation or update"""
        for field in self.fields_map.values():
            yield field.field_parameters()

            if field.is_relationship_field:
                yield field.relationship_field_params

    def prepare(self, database: TypeDatabase, skip_creation: bool = False):
        """Prepares the table with additional parameters by 
        getting all the necessary field parameters to be used in 
        order to create the current table. Runs the created SQL
        statement in an existing sqlite connection

        This function is called by the Migrations class principally
        when running the migration process to the database

        Args:
            database (TypeDatabase): The database instance to which the table is being prepared and attached
            skip_creation (bool): Can be used to prevent the creationg process for tables that were created outside of this prepare function 
        """
        field_params = self.build_all_field_parameters()
        field_params = [
            self.backend.simple_join(params)
            for params in field_params
        ]

        # TODO: Relationships validation
        # if database.has_relationships:
        #     for _, manager in database.relationships.items():
        #         if not manager.relationship_map.can_be_validated:
        #             raise ValueError(manager.relationship_map.error_message)

        #         if manager.relationship_map.creates_relationship(self):
        #             # TODO: Gather all the fields and append the sql
        #             # used for creating the relationship on table
        #             continue

        joined_fields = self.backend.comma_join(field_params)
        create_sql = self.create_table_sql(joined_fields)

        # Once the table is created and everything
        # is setup correctly, we create an abstract
        # database column to interface the column locally
        for name, field in self.fields_map.items():
            column = Column(field)
            column.prepare()
            self.columns_map.setdefault(name, column)

        if not skip_creation:
            query = database.query_class(backend=self.backend)
            query.add_sql_nodes(create_sql)
            query.run(commit=True)
            database.migrations.write_to_sql_file(create_sql)

        self.is_prepared = True
        return create_sql

    def deconstruct(self):
        """Decomposes the current table into its
        basic serializable components for migration
        purposes"""
        data = {
            'name': self.name,
            'fields': [],
            'indexes': [],
            'constraints': [],
            'ordering': list(self.ordering),
            'str_field': self.str_field
        }

        for field in self.fields_map.values():
            data['fields'].append(field.deconstruct())

        for index in self.indexes:
            data['indexes'].append(index.deconstruct())

        for constraint in self.table_constraints:
            data['constraints'].append(constraint.deconstruct())

        return data
