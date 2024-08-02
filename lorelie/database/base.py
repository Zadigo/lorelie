import pathlib
from collections import OrderedDict
from functools import wraps

from lorelie.backends import SQLiteBackend
from lorelie.database import registry
from lorelie.database.migrations import Migrations
from lorelie.exceptions import TableExistsError
from lorelie.queries import Query
from lorelie.tables import Table


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

    def __init__(self, *tables, name=None, path=None, log_queries=False):
        self.database_name = name
        # Use the immediate parent path if not
        # path is provided by the user
        self.path = pathlib.Path(__name__).parent.absolute()

        if path is not None:
            if isinstance(path, str):
                path = pathlib.Path(path)

            self.path = path

        # Create a connection to populate the
        # connection pool for the rest of the
        # operations
        self.backend_class(
            database_or_name=self,
            path=self.path,
            log_queries=log_queries
        )

        self.table_map = {}
        for table in tables:
            if not isinstance(table, Table):
                raise ValueError('Value should be an instance of Table')

            table.load_current_connection()
            # setattr(table, 'database', self)
            self.table_map[table.name] = table
            setattr(self, table.name, table)

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

    @property
    def verbose_name(self):
        if self.database_name is not None:
            return self.database_name.title()
        return 'MEMORY'

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

    # TODO: Remove this line
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
