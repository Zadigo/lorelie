import dataclasses
import re
from collections import OrderedDict
from dataclasses import dataclass
from functools import cached_property, lru_cache

from lorelie.backends import SQLiteBackend
from lorelie.constraints import CheckConstraint, UniqueConstraint
from lorelie.database.indexes import Index
from lorelie.database.manager import (BackwardForeignTableManager,
                                      DatabaseManager,
                                      ForwardForeignTableManager)
from lorelie.database.tables.columns import Column
from lorelie.exceptions import FieldExistsError, ImproperlyConfiguredError
from lorelie.fields.base import AutoField, DateField, DateTimeField, Field
from lorelie.queries import Query


@dataclass
class ValidatedData:
    values: list
    data: dict= dataclasses.field(default_factory=dict)
    relationship_fields: dict = dataclasses.field(default_factory=dict)

    @cached_property
    def columns(self):
        return list(self.data.keys())

    @property
    def get_data_to_save(self):
        """Swaps the name of the field provided
        by the user with the true foreign fields
        present on the database e.g.
        `celebrity_follower -> celebrity_id`"""
        data = {}
        for key, value in self.data.items():
            if key in self.relationship_fields:
                data[self.relationship_fields[key]] = value
                continue
            data[key] = value
        return data


@dataclass
class RelationshipMap:
    """This map reunites all the components
    that inform on the different elements that
    create a foreign relationship in the database"""

    left_table: type
    right_table: type
    junction_table: type = None
    relationship_field: Field = None
    relationship_type: str = dataclasses.field(default='foreign')
    can_be_validated: bool = False
    error_message: str = None

    def __post_init__(self):
        accepted_types = ['foreign', 'one', 'many']
        if self.relationship_type not in accepted_types:
            self.error_message = (
                f"The relationship type is "
                "not valid: {self.relationship_type}"
            )

        # If both tables are exactly the same
        # in name and fields, this cannot be
        # a valid relationship
        if self.left_table == self.right_table:
            self.error_message = (
                "Cannot create a relationship between "
                f"two same tables: {self.left_table}, {self.right_table}"
            )

        if self.error_message is None:
            self.can_be_validated = True

    def __repr__(self):
        relationship = '/'
        template = '<RelationshipMap[{value}]>'
        if self.relationship_type == 'foreign':
            relationship = f"{self.left_table.name} -> {self.right_table.name}"
        return template.format_map({'value': relationship})

    @property
    def relationship_name(self):
        """Creates a default relationship name by using
        the respective name of each table"""
        if self.left_table is not None and self.right_table is not None:
            left_table_name = getattr(self.left_table, 'name')
            right_table_name = getattr(self.right_table, 'name')
            return f'{left_table_name}_{right_table_name}'
        return None

    @property
    def forward_field_name(self):
        # db.objects.first().followers.all()
        return self.relationship_field.name

    @property
    def backward_field_name(self):
        # db.objects.first().names_set.all()
        return f'{self.relationship_field.name}_set'

    @property
    def foreign_backward_related_field_name(self):
        """Returns the database field that will
        relate the right table to the ID field
        of the left table so if we have tables
        A (fields name) and B (fields age), then
        the age_id which is the backward related
        field name will be the name of the field
        created in A: `age_id <- id`"""
        name = getattr(self.right_table, 'name')
        return f'{name}_id'

    @property
    def foreign_forward_related_field_name(self):
        """Returns the database field that will
        relate the right table to the ID field
        of the left table so if we have tables
        A (fields name) and B (fields age), then
        the age_id which is the backward related
        field name will be the name of the field
        created in A: `id -> age_id`"""
        name = getattr(self.left_table, 'name')
        return f'{name}_id'

    def get_relationship_condition(self, table):
        tables = (self.right_table, self.left_table)
        if table not in tables:
            raise ValueError(
                "Cannot create conditions for none "
                "existing tables"
            )

        selected_table = list(filter(lambda x: table == x, tables))
        other_table = list(filter(lambda x: table != x, tables))

        lhv = f"{selected_table[-1].name}.id"
        rhv = f"{
            other_table[-1].name}.{self.foreign_forward_related_field_name}"

        return lhv, rhv

    def creates_relationship(self, table):
        """The relationship is created from left
        to right. This function allows us to determine
        if a table can create a relationship if it matches
        the left table registed in this map"""
        return table == self.left_table


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

    @staticmethod
    def validate_table_name(name):
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

    def prepare_field_names(self, field_names):
        for field_name in field_names:
            yield self.get_column(field_name)

    def pre_save_setup_from_list(self, values):
        for value in values:
            yield self.pre_save_setup_from_dict(value)

    def pre_save_setup_from_dict(self, values):
        fields, values = self.backend.dict_to_sql(values, quote_values=False)
        return self.pre_save_setup(fields, values)

    def pre_save_setup(self, fields, values):
        """Validates a set of values that the user is 
        trying to insert or update in the database by
        doing the following things:
        
            * Calls `Field.to_database` in order to transform the
              data to a valid database value

            * Returns a `ValidatedData` object with the list of valid
              values that can be saved into the database

        >>> validate_values(['name'], ['Kendall'])
        ... (["'Kendall'"], {'name': "'Kendall'"})
        """
        from lorelie.backends import BaseRow

        validated_values = []
        # TODO: We have to be able to create data
        # using the relationship field from another
        # table
        for i, field in enumerate(fields):
            # TODO: Allow creation with id field
            if field == 'rowid' or field == 'id':
                continue

            try:
                field = self.fields_map[field]
            except KeyError:
                raise FieldExistsError(field, self)

            value = list(values)[i]

            if field.is_relationship_field:
                if not isinstance(value, BaseRow):
                    raise ValueError(
                        f"{value} is expected to be an instance "
                        f"of BaseRow but got {type(value)}"
                    )
                value = str(value.pk)

            clean_value = field.to_database(value)
            validated_value = self.backend.quote_value(clean_value)
            validated_values.append(validated_value)

        validated_dict_values = dict(zip(fields, validated_values))
        validated_data = ValidatedData(validated_values, data=validated_dict_values)

        if self.relationship_maps:
            for key, value in validated_data.data.items():
                if key in self.relationship_maps:
                    relationship_map = self.relationship_maps[key]
                    true_field_name = relationship_map.foreign_forward_related_field_name
                    validated_data.relationship_fields[key] = true_field_name

        return validated_data

    def load_current_connection(self):
        from lorelie.backends import connections
        try:
            # Safeguard for if the user is trying to call this
            # function outside of a database class. Technically
            # tables should be instantiated in Database wrapper
            database_name = self.attached_to_database.database_name
        except AttributeError:
            raise NoDatabaseError(self)
        else:
            self.backend = connections.get_connection(database_name)


class Table(AbstractTable):
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
    ... table.objects.create('url', url='http://example.com')
    ... table.objects.all('url')
    """
    objects = DatabaseManager()

    def __init__(self, name, *, fields=[], indexes=[], constraints=[], ordering=[], str_field='id'):
        self.name = self.validate_table_name(name)
        self.verbose_name = name.lower().title()
        self.indexes = indexes
        self.table_constraints = []

        seen_constraints = []
        for constraint in constraints:
            if constraint in self.table_constraints:
                seen_constraints.append(constraint)
            self.table_constraints.append(constraint)

        if seen_constraints:
            raise ValueError(
                "Found existing constraint names in your "
                f"table: {', '.join(str(c) for c in seen_constraints)}."
                "Constraints names should be unique."
            )

        self.field_constraints = {}
        self.is_foreign_key_table = False
        self.relationship_maps = {}
        self.foreign_managers = {}
        self.attached_to_database = None
        # The str_field is the name of the
        # field to be used for representing
        # the column in the BaseRow
        self.str_field = str_field

        self.ordering = set(ordering)
        self.fields_map = OrderedDict()
        self.auto_add_fields = set()
        self.auto_update_fields = set()

        super().__init__()

        # Maps the local field name to its
        # true name on the foreign table
        self.foreign_fields_map = {}

        none_relationship_fields = []
        relationship_fields = []

        for field in fields:
            if field.is_relationship_field:
                relationship_fields.append(field)
                continue
            none_relationship_fields.append(field)

        non_authorized_names = ['rowid', 'id', 'table']
        for i, field in enumerate(none_relationship_fields):
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

            # We set the relationship fields
            # differently from the classic fields
            if field.is_relationship_field:
                continue

            field.prepare(self)
            field.index = i + 1
            # If the user uses the same keys multiple
            # times, leave the error for him to resolve
            # since this will just override the first
            # apparition of the duplicate field multiple
            # times in the map
            # TODO: Delegate this section to the prepare
            # method of the field directly
            self.fields_map[field.name] = field

        # Automatically create an ID field and set
        # it up with the table and backend
        id_field = AutoField()
        id_field.prepare(self)
        self.fields_map['id'] = id_field

        field_names = list(self.fields_map.keys())
        field_names.append('rowid')
        self.field_names = field_names

        for index in indexes:
            if not isinstance(index, Index):
                raise ValueError(f'{index} should be an instance of Index')
            index.prepare(self)

        # Once all the basic fields were prepared,
        # prepare the relationship fields. The reason
        # we do it last is because some items require
        # other parts of the table to be prepared
        # before continuing
        for i, field in enumerate(relationship_fields):
            params = {
                'left_table': field.foreign_table,
                'right_table': self,
                'relationship_field': field
            }

            if field.field_python_name == 'ManyToManyField':
                continue

            # TODO: Simplify this whole section
            relationship_map = RelationshipMap(**params)
            self.relationship_maps[field.name] = relationship_map

            field.prepare(self)

            foward_manager = ForwardForeignTableManager.new(
                self,
                relationship_map
            )
            backward_manager = BackwardForeignTableManager.new(
                field.foreign_table,
                relationship_map
            )

            # left_table.field_set.manager <-> right_table.field.manager
            self.foreign_managers[relationship_map.forward_field_name] = foward_manager
            field.foreign_table.foreign_managers[relationship_map.backward_field_name] = backward_manager
            self.fields_map[field.name] = field
            # Map the actual foreign table column name e.g. column_id
            # here so that we can get it when needed later on for example
            # when we start running filters in the manager
            self.foreign_fields_map[field.name] = relationship_map.foreign_forward_related_field_name

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.name}>'

    def __hash__(self):
        return hash((self.name, self.verbose_name, *self.field_names))

    def __bool__(self):
        return self.is_prepared

    def __eq__(self, value):
        if not isinstance(value, Table):
            return any([
                value == self.name,
                value in self.field_names
            ])

        return all([
            self.name == value.name,
            self.field_names == value.field_names
        ])

    def __contains__(self, value):
        return value in self.field_names

    def __setattr__(self, name, value):
        if name == 'name':
            pass
        return super().__setattr__(name, value)

    def __getattribute__(self, name):
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
    def compare_field_types(*fields):
        """Compare the different field types
        and check if we are dealing with
        mixed types"""
        seen_types = []

        for field in fields:
            seen_types.append(field.field_type)

        unique_types = set(seen_types)

        return len(unique_types) > 1

    @property
    def has_relationships(self):
        return len(self.relationship_maps.keys()) > 0

    @lru_cache(maxsize=10)
    def get_column(self, name):
        column = Column(self.get_field(name), self)
        column.prepare()
        return column

    @lru_cache(maxsize=10)
    def get_field(self, name):
        try:
            return self.fields_map[name]
        except:
            # The name of the field could be the actual
            # name of the column on the table so we should
            # be able to re-map them in order to get the
            # local field
            if self.foreign_fields_map:
                resolved_name = None
                for key, value in self.foreign_fields_map.items():
                    if name == value:
                        resolved_name = key
                        break

                try:
                    return self.fields_map[resolved_name]
                except:
                    raise FieldExistsError(name, self)
            raise FieldExistsError(name, self)

    def _add_field(self, field_name, field):
        """Internala function to add a field on the
        database. Returns the newly constructued
        field paramters"""
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
        return field_params

    # TODO: Rename to check_field
    def has_field(self, name, raise_exception=False):
        result = name in self.fields_map
        if not result and raise_exception:
            raise FieldExistsError(name, self)
        return result

    def create_table_sql(self, fields):
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
        joined_unique = self.backend.comma_join([fields, *unique_constraints])
        joined_checks = self.backend.simple_join(
            [joined_unique, *check_constraints]
        )

        sql = self.backend.CREATE_TABLE.format_map({
            'table': self.name,
            'fields': joined_checks
        })
        return [sql]

    def drop_table_sql(self):
        sql = self.backend.DROP_TABLE.format_map({
            'table': self.name
        })
        return [sql]

    def build_all_field_parameters(self):
        """Returns the paramaters for all
        the fields present on the current
        table. The parameters are the SQL
        parameters e.g. null, autoincrement
        used to define/create the field in 
        the database on creation or update"""
        for field in self.fields_map.values():
            yield field.field_parameters()

            if getattr(field, 'is_relationship_field', False):
                yield field.relationship_field_params

    def prepare(self, database, skip_creation=False):
        """Prepares the table with additional parameters, 
        gets all the field parameters to be used in order to
        create the current table and then creates the create SQL
        statement that will then be used to creates the
        different tables in the database using the database"""
        if skip_creation:
            self.attached_to_database = database
            return True

        field_params = self.build_all_field_parameters()
        field_params = [
            self.backend.simple_join(params)
            for params in field_params
        ]

        if self.has_relationships:
            for _, manager in self.foreign_managers.items():
                if not manager.relationship_map.can_be_validated:
                    raise ValueError(manager.relationship_map.error_message)

                if manager.relationship_map.creates_relationship(self):
                    # TODO: Gather all the fields and append the sql
                    # used for creating the relationship on table
                    continue

        self.attached_to_database = database
        self.is_prepared = True

        joined_fields = self.backend.comma_join(field_params)
        create_sql = self.create_table_sql(joined_fields)

        query = self.query_class(table=self)
        query.add_sql_nodes(create_sql)
        query.run(commit=True)
