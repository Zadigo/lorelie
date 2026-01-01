import dataclasses
import pathlib
from collections import OrderedDict
from functools import wraps
from typing import Callable, Optional
from warnings import deprecated, warn

from asgiref.sync import sync_to_async

from lorelie.backends import SQLiteBackend
from lorelie.database import registry
from lorelie.database.manager import ForeignTablesManager
from lorelie.database.migrations import Migrations
from lorelie.database.tables.base import Table
from lorelie.exceptions import TableExistsError
from lorelie.fields import IntegerField
from lorelie.fields.relationships import ForeignKeyField
from lorelie.queries import Query
from lorelie.lorelie_typings import TypeDatabase, TypeOnDeleteTypes, TypeStrOrPathLibPath, TypeTable


@dataclasses.dataclass
class RelationshipMap:
    left_table: Table
    right_table: Table
    junction_table: Table = None
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
        return getattr(self.left_table, 'name')

    @property
    def backward_field_name(self):
        # db.objects.first().names_set.all()
        name = getattr(self.right_table, 'name')
        return f'{name}_set'

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

    Args:
        *tables (Table): A variable number of Table instances to register into the database
        name (Optional[str], optional): The name of the database. If None, an in-memory database is created. Defaults to None.
        path (Optional[TypeStrOrPathLibPath], optional): The path where the database file will be stored. Defaults to None.
        log_queries (bool, optional): Whether to log SQL queries executed on the database. Defaults to False.

    Returns:
        Database: An instance of the Database class
    """

    migrations = None
    query_class = Query
    migrations_class = Migrations
    backend_class = SQLiteBackend

    def __init__(self, *tables: Table, name: Optional[str] = None, path: Optional[TypeStrOrPathLibPath] = None, log_queries: bool = False):
        self.database_name: str = name
        # Use the immediate parent path if not
        # path is provided by the user
        self.path: pathlib.Path = pathlib.Path(__name__).parent.absolute()

        if path is not None:
            if isinstance(path, str):
                path = pathlib.Path(path)

            if not path.is_dir():
                raise ValueError(
                    "When providing a path, it should be "
                    "a directory where the database file "
                    "will be stored"
                )

            self.path = path

        # Create a connection to populate the
        # connection pool for the rest of the
        # operations. THe user can create either
        # an in-memory database or a physical
        # database by providing a name
        self.backend_class(
            database_or_name=self,
            log_queries=log_queries
        )

        self.table_map: dict[str, Table] = {}
        for table in tables:
            if not isinstance(table, Table):
                raise ValueError('Value should be an instance of Table')

            self.table_map[table.name] = table
            setattr(self, table.name, table)
            setattr(table, 'attached_to_database', self)
            table.load_current_connection()

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

    @property
    def verbose_name(self):
        if self.database_name is not None:
            return self.database_name.title()
        return 'MEMORY'

    @classmethod
    def async_database(cls, *tables: Table, name: Optional[str] = None, path: Optional[TypeStrOrPathLibPath] = None, log_queries: bool = False):
        return sync_to_async(cls)(*tables, name=name, path=path, log_queries=log_queries)

    def _add_table(self, table: TypeTable):
        # DELETE: Remove this
        table.load_current_connection()
        self.table_map[table.name] = table
        self.table_instances.append(table)

    @deprecated("Relationships are not yet fully supported")
    def _prepare_relationship_map(self, right_table, left_table):
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
                "namespace in order to create a relationship between them"
            )

        right_table.is_foreign_key_table = True
        return RelationshipMap(left_table, right_table)

    def get_table(self, table_name: str):
        try:
            return self.table_map[table_name]
        except KeyError:
            raise TableExistsError(table_name)

    # def make_migrations(self):
    #     """The function `make_migrations` serves as a pivotal step
    #     in the database schema evolution process. It collects the
    #     various elements from the fields, tables, constraints,
    #     and indexes defined by the user, capturing these changes in a
    #     structured manner. These collected elements are then organized and stored in
    #     a migration JSON file. Additionally, the function inserts this collected data into
    #     a designated table named lorelie_migrations within the database."""
    #     self.migrations.has_migrations = True
    #     self.migrations.make_migrations(self.table_instances)

    def migrate(self, dry_run: bool = False):
        """This function executes the modifications outlined in the 
        migration file onto the database. It achieves this by orchestrating 
        various actions such as creating tables, implementing constraints, 
        and applying other specified parameters to the tables.

        Args:
            dry_run (bool, optional): If set to True, the migration process will simulate the 
                                      changes without actually applying them to the database. Defaults to False.
        """
        self.migrations.migrate(self.table_map, dry_run=dry_run)

    @deprecated("Use migrate instead")
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
    @deprecated("Views are created with the View class now")
    def create_view(self, name, queryset, temporary=True):
        return NotImplemented

    @deprecated("Triggers are not yet supported")
    def register_trigger(self, trigger, table: Optional[TypeTable] = None):
        """Registers a trigger function onto the database
        and that will get called at a specific stage of
        when the database runs a specific type of operation

        >>> db = Database()
        ... @db.register_trigger(table=None, trigger=pre_save)
        ... def my_trigger(database, table, **kwargs):
        ...     pass
        """
        def wrapper(func: Callable[[TypeDatabase, TypeTable], None]):
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

    def foreign_key(self, name: str, left_table: TypeTable, right_table: TypeTable, on_delete: Optional[TypeOnDeleteTypes] = None, related_name: Optional[str] = None):
        """Adds a foreign key between two tables by using the
        default primary ID field. The orientation for the foreign
        key goes from `left_table.id` to `right_table.field_id`

        >>> table1 = Table('celebrities', fields=[CharField('firstname', max_length=200)])
        ... table2 = Table('social_media', fields=[CharField('name', max_length=200)])

        >>> db = Database(table1, table2)
        ... db.foreign_key('followers', table1, table2, on_delete=OnDeleteEnum.CASCADE, related_name='f_my_table')
        ... db.migrate()
        ... db.social_media_tbl.all()
        ... db.celebrity_tbl_set.all()
        ... db.objects.foreign_key('social_media').all()
        ... db.objects.foreign_key('social_media', reverse=True).all()
        """
        relationship_map = self._prepare_relationship_map(
            right_table,
            left_table
        )
        self.relationships[name] = ForeignTablesManager(relationship_map)

        # Create the default field that will be used to access the
        # the right table: db.objects.first().field_set.all() and
        # then implemented on the left table. The left table will
        # contain the ID relatonship keys used to associate the
        # two tables
        field = ForeignKeyField(
            relationship_map=relationship_map,
            related_name=related_name
        )
        field.prepare(self)

        relationship_map.left_table.is_foreign_key_table = True
        relationship_map.right_table._add_field(
            relationship_map.foreign_forward_related_field_name,
            field
        )

    def many_to_many(self, name: str, left_table: TypeTable, right_table: TypeTable):
        # TODO: Create an intermediate junction table that
        # will serve to query many to many fields
        # junction_name = f'{left_table.name}_{right_table.name}'
        relationship_map = self._prepare_relationship_map(
            right_table, left_table)
        relationship_map.relationship_type = 'many'

        junction_table = Table(relationship_map.relationship_name, fields=[
            IntegerField(f'{left_table.name}_id'),
            IntegerField(f'{right_table.name}_id'),
        ])
        junction_table.prepare(self)
        self._add_table(junction_table)

        self.foreign_key(
            name, relationship_map.foreign_forward_related_field_name, left_table, junction_table)
        self.foreign_key(
            name, relationship_map.foreign_forward_related_field_name, right_table, junction_table)

    def one_to_one_key(self, name: str, left_table: TypeTable, right_table: TypeTable, on_delete: Optional[TypeOnDeleteTypes] = None):
        relationship_map = self._prepare_relationship_map(
            right_table,
            left_table
        )
        relationship_map.relationship_type = 'one'
        self.relationships[name] = ForeignTablesManager(relationship_map)
        relationship_map.left_table.is_foreign_key_table = True

    def deconstruct(self):
        return {
            'name': self.database_name,
            'tables': [table.deconstruct() for table in self.table_instances]
        }
