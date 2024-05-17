import dataclasses
import pathlib
from typing import OrderedDict, Union

from asgiref.sync import sync_to_async

from lorelie.backends import SQLiteBackend
from lorelie.database.manager import DatabaseManager
from lorelie.fields.relationships import ForeignKeyField
from lorelie.database.migrations import Migrations
from lorelie.queries import Query
from lorelie.tables import Table


class Databases:
    """A class that remembers the databases
    that were created and allows their retrieval
    if needed from other sections of the code"""

    def __init__(self):
        self.database_map = {}

    def __getitem__(self, name):
        return self.database_map[name]

    def __contains__(self, value):
        return value in self.created_databases

    @property
    def created_databases(self):
        return list(self.database_map.values())

    def register(self, database):
        from lorelie.database.base import Database
        if not isinstance(database, Database):
            raise ValueError('Value should be an instance of Database')
        name = 'default' if database.database_name is None else database.database_name
        self.database_map[name] = database


databases = Databases()


@dataclasses.dataclass
class RelationshipMap:
    left_table: Table
    right_table: Table
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
        return f'<RelationshipMap[{self.right_table.name} -> {self.left_table.name}]>'

    @property
    def relationship_field_name(self):
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

    def __init__(self, *tables, name=None, path=None):
        self.database_name = name
        self.migrations = self.migrations_class(self)

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

        if path is None:
            # Use the immediate parent path if not
            # path is provided by the user
            self.path = pathlib.Path(__file__).parent.absolute()
        databases.register(self)

    def __repr__(self):
        tables = list(self.table_map.values())
        return f'<{self.__class__.__name__} {tables}>'

    def __getitem__(self, table_name):
        return self.table_map[table_name]

    def __contains__(self, value):
        return value in self.table_names

    # TODO: Implement this functionnality
    # def __getattribute__(self, name):
    #     if name.endswith('_tbl'):
    #         # When the user calls db.celebrities_tbl
    #         # for example, we have to reimplement the objects
    #         # manager on the table so that we can get
    #         # db.celebrities_tbl.objects which returns
    #         # DatabaseManager
    #         lhv, _ = name.split('_tbl')

    #         try:
    #             table = self.table_map[lhv]
    #         except KeyError as e:
    #             raise ExceptionGroup(
    #                 "Missing table",
    #                 [
    #                     KeyError(e.args),
    #                     TableExistsError(lhv)
    #                 ]
    #             )

    #         manager = DatabaseManager.as_manager()
    #         manager.database = self
    #         manager.table_map = self.table_map

    #         setattr(table, 'objects', manager)

    #         # The tricky part is on all the manager
    #         # database functions e.g. all, filter
    #         # we have to normally pass the table's
    #         # name. However, since we already know
    #         # the table that is being called, we
    #         # to alias these functions aka
    #         # all() -> all('celebrities') which
    #         # gives db.celebrities_tbl.objects.all()
    #         # instead of
    #         # db.celebrities_tbl.objects.all('celebrities')
    #         return table
    #     return super().__getattribute__(name)

    def __hash__(self):
        return hash((self.database_name, *self.table_names))

    @property
    def in_memory(self):
        """If the database does not have a
        concrete name then it is `memory`"""
        return self.database_name is None

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
        return self.table_map[table_name]

    def make_migrations(self):
        """Updates the migration file with the
        local changes to the tables. Make migrations
        should generally be called before running `migrate`
        """
        self.migrations.has_migrations = True
        self.migrations.make_migrations(self.table_instances)

    def migrate(self):
        """Implements the changes from the migration
        file to the database for example by creating the
        tables, implementing the constraints and all other
        table parameters specified by on the table"""
        self.migrations.check(self.table_map)

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
        if (not isinstance(left_table, Table) and
                not isinstance(right_table, Table)):
            raise ValueError(
                "Both tables should be an instance of "
                f"Table: {left_table}, {right_table}"
            )
        
        if (left_table not in self.table_instances and
                right_table not in self.table_instances):
            raise ValueError(
                "Both tables need to be registered in the database "
                "namespace in order to create a relationship"
            )

        relationship_map = RelationshipMap(left_table, right_table)

        field = ForeignKeyField(relationship_map=relationship_map)
        field.prepare(self)

        relationship_map.field = field
        self.relationships[relationship_map.relationship_field_name] = relationship_map

    def many_to_many(self, left_table, right_table, primary_key=True, related_name=None):
        pass

    def one_to_one_key(self, left_table, right_table, on_delete=None, related_name=None):
        pass
