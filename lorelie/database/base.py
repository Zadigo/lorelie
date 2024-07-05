import dataclasses
import pathlib
from collections import OrderedDict
from functools import wraps
from typing import Union

from lorelie.backends import SQLiteBackend
from lorelie.database import registry
from lorelie.database.manager import DatabaseManager
from lorelie.database.migrations import Migrations
from lorelie.exceptions import TableExistsError
from lorelie.fields.relationships import ForeignKeyField
from lorelie.queries import Query
from lorelie.tables import Table


@dataclasses.dataclass
class RelationshipMap:
    left_table: Table
    right_table: Table
    junction_table: Table = None
    relationship_type: str = dataclasses.field(default='foreign')
    field: Union[ForeignKeyField] = None
    can_be_validated: bool = False
    error_message: str = None

    def __post_init__(self):
        accepted_types = ['foreign', 'one', 'many']
        if self.relationship_type not in accepted_types:
            self.can_be_validated = False
            self.error_message = (
                f"The relationship type is not valid: {self.relationship_type}"
            )

        # If both tables are exactly the same
        # in name and fields, this cannot be
        # a valid relationship
        if self.left_table == self.right_table:
            self.can_be_validated = False
            self.error_message = (
                "Cannot create a relationship between "
                f"two similar tables: {self.left_table}, {self.right_table}"
            )
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
            left_table_name = getattr(self.left_table, 'name', None)
            right_table_name = getattr(self.right_table, 'name', None)
            return f'{left_table_name}_{right_table_name}'
        return None

    @property
    def forward_field_name(self):
        return getattr(self.left_table, 'name', None)

    @property
    def backward_field_name(self):
        name = getattr(self.right_table, 'name', None)
        return f'{name}_set'

    @property
    def backward_related_field(self):
        """Returns the database field that will
        relate the right table to the ID field
        of the left table e.g. table_id -> id"""
        name = getattr(self.left_table, 'name', None)
        return f'{name}_id'

    def creates_relationship(self, table):
        """The relationship is created from right
        to left. This function allows us to determine
        if a table can create a relationship with
        another one"""
        return table == self.right_table


class Database:
    """This class links and unifies independent
    tables together for a unique database and allows 
    its management via a migration file.

    Creating a new database can be done by doing the following steps:

    >>> table = Table('my_table', fields=[CharField('firstname')])
    ... db = Database(table, name='my_database')

    Providing a name is optional. If present, an sqlite dabase is created
    in the local project's path otherwise it is created in memory and
    therefore deleted once the project is teared down.

    Migrating the database is an optional step that helps tracking
    the tables and in the fields that were created in a `migrations.json`
    local file:

    >>> db.make_migrations()
    ... db.migrate()

    `make_migrations` writes the physical changes to the
    local tables into the `migrations.json` file

    `migrate` implements the changes from the migration
    file into the SQLite database. It syncs the changes from
    the file into the database such as deleting or updating
    existing tables

    Once the database is created, we can then run various operations
    on the tables within it:

    >>> db.objects.create('my_table', url='http://example.com')
    ... db.objects.all()
    """

    migrations = None
    query_class = Query
    migrations_class = Migrations
    backend_class = SQLiteBackend
    objects = DatabaseManager()

    def __init__(self, *tables, name=None, path=None, log_queries=False):
        self.database_name = name
        # Use the immediate parent path if not
        # path is provided by the user
        self.path = pathlib.Path(__name__).parent.absolute()

        if path is not None:
            self.path = path

        # Create a connection to populate the
        # connection pool for the rest of the
        # operations
        self.backend_class(database_name=name)

        self.table_map = {}
        for table in tables:
            if not isinstance(table, Table):
                raise ValueError('Value should be an instance of Table')

            table.load_current_connection()
            setattr(table, 'database', self)
            self.table_map[table.name] = table

        self.table_instances = list(tables)
        self.relationships = OrderedDict()
        self.log_queries = log_queries

        # databases.register(self)
        # FIXME: Seems like if this class is not called
        # after all the elements have been set, this
        # raises an error. Maybe create a special prepare
        # function to setup the different elements of the
        # class later on
        self.migrations = self.migrations_class(self)
        registry.register_database(self)

    def __repr__(self):
        return f'<{self.__class__.__name__} [tables: {len(self.table_names)}]>'

    def __getitem__(self, table_name):
        return self.table_map[table_name]

    def __contains__(self, value):
        return value in self.table_names

    # def __getattr__(self, name):
    #     # TODO: Continue to improve this
    #     # section so that we can call the
    #     # tables directly from the database
    #     table_names = getattr(self.__dict__['database'], 'table_names')
    #     if name in table_names:
    #         try:
    #             current_table = self.table_map[name]
    #         except:
    #             raise TableExistsError(name)
    #         else:
    #             manager = self.objects
    #             setattr(manager, '_test_current_table_on_manager', current_table)
    #             return self.objects
    #     return name

    def __hash__(self):
        return hash((self.database_name, *self.table_names))

    @property
    def in_memory(self):
        """If the database does not have a
        concrete name then it is `memory`"""
        return self.database_name is None

    @property
    def is_ready(self):
        return self.migrations.migrated

    @property
    def table_names(self):
        return list(self.table_map.keys())

    @property
    def has_relationships(self):
        return len(self.relationships) > 0

    def _add_table(self, table):
        table.load_current_connection()
        self.table_map[table.name] = table

    def get_table(self, table_name):
        try:
            return self.table_map[table_name]
        except KeyError:
            raise TableExistsError(table_name)

    def make_migrations(self):
        """The function `make_migrations` serves as a pivotal step 
        in the database schema evolution process. It collects the 
        various elements from the fields, tables, constraints, 
        and indexes defined by the user, capturing these changes in a 
        structured manner. These collected elements are then organized and stored in 
        a migration JSON file. Additionally, the function inserts this collected data into 
        a designated table named lorelie_migrations within the database."""
        self.migrations.has_migrations = True
        self.migrations.make_migrations(self.table_instances)

    def migrate(self):
        """This function executes the modifications outlined in the 
        migration file onto the database. It achieves this by orchestrating 
        various actions such as creating tables, implementing constraints, 
        and applying other specified parameters to the tables."""
        self.migrations.migrate(self.table_map)

    def simple_load(self):
        """Loads an existing sqlite and checks that the
        columns of the existing tables match those of the
        local table instances. Does not create or attempt
        to modify the elements in sqlite database contrarily
        to migrate. Use this method when you simply want to use
        an existing database without modifiying the existing
        data sqlite
        """
        return NotImplemented

    def create_view(self, name, queryset, temporary=True):
        return NotImplemented

    def register_trigger(self, trigger, table=None):
        """Registers a trigger function onto the database
        and that will get called at a specific stage of
        when the database runs a specific type of operation

        >>> db = Database()
        ... @db.register_trigger(table=None, trigger=pre_save)
        ... def my_trigger(database, table, **kwargs):
        ...     pass
        """
        def wrapper(func):
            if table is not None:
                values = [table.name, trigger, func]
            else:
                values = [None, trigger, func]
            registry.registered_triggers.container.append(values)

            @wraps(func)
            def inner(**kwargs):
                func(database=self, table=table, **kwargs)
            return inner
        return wrapper

    def foreign_key(self, left_table, right_table, on_delete=None, related_name=None):
        """Adds a foreign key between two databases by using the
        default primary ID field. The orientation for the foreign
        key goes from `left_table.id` to `right_table.id`
        >>> table1 = Table('celebrities', fields=[CharField('firstname', max_length=200)])
        ... table2 = Table('social_media', fields=[CharField('name', max_length=200)])

        >>> db = Database(table1, table2)
        ... db.foreign_key(table1, table2, on_delete='cascade', related_name='f_my_table')
        ... db.migrate()
        ... db.social_media_tbl.all()
        ... db.celebrity_tbl_set.all()
        ... db.objects.foreign_key('social_media').all()
        ... db.objects.foreign_key('social_media', reverse=True).all()
        """
        # if (not isinstance(left_table, Table) and
        #         not isinstance(right_table, Table)):
        #     raise ValueError(
        #         "Both tables should be an instance of "
        #         f"Table: {left_table}, {right_table}"
        #     )

        # if (left_table not in self.table_instances and
        #         right_table not in self.table_instances):
        #     raise ValueError(
        #         "Both tables need to be registered in the database "
        #         "namespace in order to create a relationship"
        #     )

        # right_table.is_foreign_key_table = True
        # relationship_map = RelationshipMap(left_table, right_table)

        # field = ForeignKeyField(relationship_map=relationship_map)
        # field.prepare(self)

        # relationship_map.field = field
        # self.relationships[relationship_map.relationship_field_name] = relationship_map
        return NotImplemented

    def many_to_many(self, left_table, right_table, primary_key=True, related_name=None):
        # Create an intermediate junction table that
        # will serve to query many to many fields
        # junction_name = f'{left_table.name}_{right_table.name}'
        # table = Table(junction_name, fields=[
        #     IntegerField(f'{left_table.name}_id'),
        #     IntegerField(f'{right_table.name}_id'),
        # ])
        # self._add_table(table)

        # relationship_map = RelationshipMap(left_table, right_table)
        # relationship_map.junction_table = table
        # self.relationships[relationship_map.relationship_field_name] = relationship_map
        return NotImplemented

    def one_to_one_key(self, left_table, right_table, on_delete=None, related_name=None):
        return NotImplemented
