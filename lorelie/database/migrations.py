import dataclasses
import datetime
import json
import secrets
import tomllib
from collections import defaultdict
from dataclasses import dataclass, field
from functools import cached_property
from typing import TYPE_CHECKING, Generic, Optional, TypeVar

from lorelie.backends import SQLiteBackend, connections
from lorelie.database.nodes import InsertNode
from lorelie.database.tables.base import Table
from lorelie.fields.base import CharField, DateTimeField, Field, JSONField
from lorelie.queries import Query
from lorelie.utils.json_encoders import DefaultJSonEncoder

if TYPE_CHECKING:
    from lorelie.database.base import Database

D = TypeVar('D', bound='Database')


@dataclass
class Schema:
    table: Table = None
    database: Optional['Database'] = None
    fields: list = field(default_factory=list)
    field_params: list = field(default_factory=list)
    indexes: dict = field(default_factory=dict)
    constraints: dict = field(default_factory=dict)

    def __hash__(self):
        return hash((self.table.name, self.database.database_name))

    def prepare(self):
        self.fields = self.table.field_names
        self.field_params = list(self.table.build_all_field_parameters())

    def to_dict(self):
        for field in dataclasses.fields(self):
            pass


def migration_validator(value):
    pass


class SchemaStructure(Generic[D]):
    """This class manages the schema structure
    of a given database. It references existing tables,
    dropped tables and their fields and runs the different methods
    required to create or delete them eventually. The SQL operations
    are stored in a single SQL file and the schema structure
    is stored in a JSON file."""

    basename = 'schema.json'

    def __init__(self, database: D):
        self.database = database
        self.json_file = self.database.path / self.basename
        self.CACHE = self.read_content

        try:
            self.tables: list[str] = self.CACHE['tables']
        except KeyError:
            raise KeyError('Migration file is not valid')

        self.migration_table_map = [
            table['name'] for table in self.tables
            if table is not None
        ]
        self.fields_map: defaultdict[str, list[str]] = defaultdict(list)

        self.migrated = False
        self.has_migrations = False

        self.schemas: dict[str, Schema] = defaultdict(Schema)
        self.pending_migration = {}

        self.file_id = self.CACHE['id']
    
    def __repr__(self):
        return f'<{self.__class__.__name__} {self.file_id}>'

    @cached_property
    def read_content(self):
        try:
            with open(self.json_file, mode='r') as f:
                return json.load(f)
        except FileNotFoundError:
            # Create a blank migration file
            return self.blank_migration()

    def blank_migration(self):
        """Creates a blank initial migration file"""
        migration_content = {}

        file_path = self.database.path / self.basename
        if not file_path.exists():
            file_path.touch()

        with open(file_path, mode='w') as f:
            migration_content['id'] = secrets.token_hex(5)
            migration_content['date'] = str(datetime.datetime.now())
            migration_content['number'] = 1

            migration_content['tables'] = []
            json.dump(migration_content, f, indent=4, ensure_ascii=False)
            return migration_content

    def create_schemas(self, table_instances: dict[str, Table]):
        from lorelie.database.tables.base import Table

        # Safeguard that avoids calling
        # this function in a loop over and
        # over which can reduce performance
        if self.migrated:
            return True

        errors = []
        for name, table_instance in table_instances.items():
            if not isinstance(table_instance, Table):
                errors.append(
                    f"Value should be instance "
                    f"of Table. Got: {table_instance}"
                )
            schema = self.schemas[name]
            schema.table = table_instance
            schema.database = self.database

        if errors:
            raise ValueError(*errors)

        if not table_instances:
            return
        
        # There is a case where makemigrations() is not
        # called which infers that there is no migration
        # file. However, that does not mean that the tables
        # cannot or should not be created. In that case,
        # use what we know which is the table_instances
        # passed to this function containing both the table
        # name and the Table instance
        if not self.migration_table_map:
            self.migration_table_map = list(table_instances.keys())


    def build_json_schema(self, tables: list[Table]):
        backend = connections.get_last_connection()
        migration = {
            'id': secrets.token_hex(5),
            'date': str(datetime.datetime.now()),
            'number': 1,
            'tables': []
        }

        for table in tables:
            if not isinstance(table, Table):
                raise ValueError(f'{table} is not an instance of Table')

            schema = self.schemas[table.name]
            schema.table = table
            schema.database = self.database
            migration['tables'].append(schema)

            for constraint in table.table_constraints:
                schema.constraints[constraint.name] = [
                    constraint.name,
                    constraint.as_sql(backend)
                ]

            for index in table.indexes:
                schema.indexes[index.index_name] = [
                    index.fields
                ]

            schema.prepare()
        self.pending_migration = migration

        if self.has_migrations:
            cache_copy = self.CACHE.copy()
            with open(self.database.path.joinpath(self.basename), mode='w+') as f:
                cache_copy['id'] = secrets.token_hex(5)
                cache_copy['date'] = str(datetime.datetime.now())
                cache_copy['number'] = self.CACHE['number'] + 1
                cache_copy['tables'] = migration['tables']
                json.dump(cache_copy, f, indent=4, ensure_ascii=False)


class BaseMigrations(Generic[D]):
    operations = []
    backend_class = SQLiteBackend

    def __init__(self, database: D):
        self.database = database
        self.database_name = database.database_name or 'memory'
        self.sql_file = database.path / 'migrations.sql'
        self.schema_structure = database.path / 'schema.json'

        self.tables_for_creation: set[str] = set()
        self.tables_for_deletion: set[str] = set()
        self.existing_tables: set[str] = set()

        # Indicates that check function was
        # called at least once and that the
        # the underlying database can be
        # fully functional
        self.migrated: bool = False

    def add_table_to_create(self, table_name: str):
        # When the table is in the migration file
        # and not in the database tables that we
        # listed above, it needs to be created
        if not table_name in self.existing_tables:
            self.tables_for_creation.add(table_name)

    def add_table_to_delete(self, database_row, migration_table_map):
        if database_row['name'] not in migration_table_map:
            self.tables_for_deletion.add(database_row['name'])


class Migrations(BaseMigrations):
    """This class manages the different
    states of a given database. It references
    existing tables, dropped tables and their
    fields and runs the different methods required
    to create or delete them eventually. The SQL operations
    are stored in a single SQL file and the schema structure
    is stored in a JSON file.

    The schema structure contains
    the different tables, their fields, indexes and constraints
    as well as their parameters"""

    schema_structure_class = SchemaStructure

    def __init__(self, database: 'Database'):
        super().__init__(database)

        self.schema_structure = self.schema_structure_class(database)
        self.operations: list[str] = []

    @property
    def in_memory(self):
        return self.database_name is None

    def write_operations(self):
        if not self.operations:
            return

        with open(self.sql_file, mode='a+') as f:
            for operation in self.operations:
                f.write(f'{operation};\n')

    def migrate(self, table_instances: dict[str, Table]):
        self.schema_structure.create_schemas(table_instances)

        backend = connections.get_last_connection()
        backend.linked_to_table = 'sqlite'
        self.existing_tables = backend.list_all_tables()

        for table_name in self.migration_table_map:
            self.add_table_to_create(table_name)

        for database_row in self.existing_tables:
            self.add_table_to_delete(database_row, self.schema_structure.migration_table_map)

        if self.tables_for_creation:
            for table_name in self.tables_for_creation:
                table = table_instances.get(table_name, None)
                if table is None:
                    continue

                # This is the specific section
                # that actually creates the table
                # in the database>
                table.prepare(self.database)
            self.has_migrations = True
            
            query = Query()
            query.add_sql_nodes(self.operations)

    def make_migrations(self, tables):
        self.schema_structure.build_json_schema(tables)

    def create_blank_migration(self):
        if not self.file.exists():
            with open(self.file, mode='w+') as f:
                f.write('-- Migration SQL script\n')
