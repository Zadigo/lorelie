import dataclasses
import datetime
import json
import secrets
from collections import defaultdict
from dataclasses import dataclass, field
from functools import cached_property
from typing import TYPE_CHECKING, DefaultDict, Final, Optional, Type

from lorelie.backends import SQLiteBackend, connections
from lorelie.database.tables.base import Table
from lorelie.fields.base import CharField, DateTimeField, Field, JSONField
from lorelie.lorelie_typings import TypeSQLiteBackend, TypeTable, TypeTableMap
from lorelie.queries import Query
from lorelie.utils.json_encoders import DefaultJSonEncoder

if TYPE_CHECKING:
    from lorelie.database.base import Database


@dataclass
class Schema:
    table: Optional[Table] = None
    database: Optional['Database'] = None
    fields: list = field(default_factory=list)
    field_params: list = field(default_factory=list)
    indexes: dict = field(default_factory=dict)
    constraints: dict = field(default_factory=dict)

    def __hash__(self):
        return hash((self.table.name, self.database.database_name))

    def prepare(self):
        """Transform the instances into serializable
        data that can be written to a migration file"""
        fields = self.table.field_names
        field_params = list(self.table.build_all_field_parameters())
        return {
            'name': self.table.name,
            'fields': fields,
            'field_params': field_params,
            'indexes': self.indexes,
            'constraints': self.constraints
        }


@dataclass
class JsonMigrationsSchema:
    id: Optional[str] = None
    date: Optional[str] = None
    number: Optional[int] = None
    tables: list[Schema] = field(default_factory=list)

    def __post_init__(self):
        self.id = self.id or secrets.token_hex(5)
        self.date = self.date or str(datetime.datetime.now())
        self.number = self.number or 1


def migration_validator(value):
    pass


class Migrations:
    """This class manages the different
    states of a given database. It references
    existing tables, dropped tables and their
    fields and runs the different methods required
    to create or delete them eventually"""

    JSON_MIGRATIONS_SCHEMA: Optional[JsonMigrationsSchema] = None
    backend_class: Final[Type[SQLiteBackend]] = SQLiteBackend

    def __init__(self, database: 'Database'):
        self.file = database.path / 'migrations.json'
        self.database = database
        self.database_name = database.database_name or 'memory'
        self.JSON_MIGRATIONS_SCHEMA = self.read_content
        self.file_id = self.JSON_MIGRATIONS_SCHEMA.id

        try:
            self.tables: list[str] = self.JSON_MIGRATIONS_SCHEMA.tables
        except KeyError:
            raise KeyError('Migration file is not valid')

        self.migration_table_map = [
            table['name']
            for table in self.tables if table is not None
        ]
        self.fields_map = defaultdict(list)

        self.tables_for_creation: set[str] = set()
        self.tables_for_deletion: set[str] = set()
        self.existing_tables: set[str] = set()
        self.has_migrations = False
        # Indicates that check function was
        # called at least once and that the
        # the underlying database can be
        # fully functionnal
        self.migrated = False
        self.schemas: DefaultDict[str, Schema] = defaultdict(Schema)

    def __repr__(self):
        return f'<{self.__class__.__name__} {self.file_id}>'

    @property
    def in_memory(self):
        return self.database_name is None

    @cached_property
    def read_content(self):
        try:
            with open(self.file, mode='r') as f:
                return JsonMigrationsSchema(**json.load(f))
        except FileNotFoundError:
            # Create a blank migration file
            instance = JsonMigrationsSchema()
            return self.blank_migration(instance)

    # def _write_fields(self, table):
    #     """Parses the different fields from
    #     a given table for a migration file"""
    #     fields_map = []
    #     for name, field in table.fields_map.items():
    #         field_name, params = list(field.deconstruct())
    #         fields_map.append({
    #             'name': field_name,
    #             'params': params
    #         })
    #     self.fields_map[table.name] = fields_map

    # def _write_indexes(self, table, backend=None):
    #     """Parses the different indexes from
    #     a given table for a migration file"""
    #     indexes = {}
    #     for index in table.indexes:
    #         indexes[index.index_name] = [
    #             index.fields,
    #             index.condition.as_sql(backend)
    #         ]
    #     return indexes

    # def _write_constraints(self, table, backend=None):
    #     """Parses the different constraints from
    #     a given table for a migration file"""
    #     indexes = {}
    #     for constraint in table.table_constraints:
    #         indexes[constraint.name] = [
    #             constraint.name,
    #             constraint.as_sql(backend)
    #         ]
    #     return indexes

    def _build_migration_table(self):
        """Creates a migrations table in the database
        which stores the different configuration for
        the current database"""
        table = Table(
            'lorelie_migrations',
            fields=[
                CharField('name', null=False, unique=True),
                CharField('db_name', null=False),
                JSONField(
                    'migration',
                    null=False,
                    validators=[migration_validator]
                ),
                DateTimeField('applied', auto_add=True)
            ],
            str_field='name'
        )
        return table

    def migrate(self, table_instances: TypeTableMap):
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

        backend = connections.get_last_connection()
        if backend is None:
            raise ValueError('No database connection found')

        backend.linked_to_table = 'sqlite'
        database_tables = backend.list_all_tables()
        # When the table is in the migration file
        # and not in the database tables that we
        # listed above, it needs to be created
        for table_name in self.migration_table_map:
            if not table_name in database_tables:
                self.tables_for_creation.add(table_name)

        # When the table is not in the migration
        # file but present in the database tables
        # that we listed above, it needs to be deleted
        for database_row in database_tables:
            if database_row['name'] not in self.migration_table_map:
                self.tables_for_deletion.add(database_row)

        if ('lorelie_migrations' not in database_tables or
                'lorelie_migrations' not in self.migration_table_map):
            migrations_table = self._build_migration_table()
            self.database._add_table(migrations_table)
            self.tables_for_creation.add('lorelie_migrations')
            self.schemas['lorelie_migrations'] = Schema(
                table=migrations_table,
                database=self.database
            )

        # Eventually create the tables
        if self.tables_for_creation:
            for table_name in self.tables_for_creation:
                table = table_instances.get(table_name, None)
                if table is None:
                    continue

                # This is the specific section
                # that actually creates the table
                # in the database
                table.prepare(self.database)
            self.has_migrations = True

        other_sqls_to_run: list[str] = []

        # TODO: For now do not run tables
        # for deletion when doing migrations
        # we'll implement this afterwards
        # if self.tables_for_deletion:
        #     sql_script = []
        #     for database_row in self.tables_for_deletion:
        #         sql = self.backend_class.DROP_TABLE.format(
        #             table=database_row['name']
        #         )
        #         sql_script.append(sql)

        #     sql = backend.build_script(*sql_script)
        #     other_sqls_to_run.append(sql)
        #     self.has_migrations = True

        # For existing tables, check that the
        # fields are the same and well set as
        # indicated in the migration file
        for database_row in database_tables:
            if (database_row['name'] in self.tables_for_creation or
                    database_row['name'] in self.tables_for_deletion):
                continue

            table_instance = table_instances.get(database_row['name'], None)
            if table_instance is None:
                continue

            self.check_fields(table_instance, backend)

        for name, table in table_instances.items():
            for index in table.indexes:
                other_sqls_to_run.append(index.as_sql(backend))

        # TODO: For now we will not remove obsolete
        # indexes too. We will implement this after too
        # Remove obsolete indexes
        database_indexes = backend.list_database_indexes()
        for row in database_indexes:
            if row not in table.indexes:
                # We cannot and should not drop autoindexes
                # which are created by sqlite. Anyways, it
                # raises an error
                if 'sqlite_autoindex' in row.name:
                    continue

                other_sqls_to_run.append(
                    backend.DROP_INDEX.format_map({
                        'value': row['name']
                    })
                )

        # The database might require another set of
        # parameters (ex. indexes, constraints) that we
        # are going to run here
        if other_sqls_to_run:
            Query.run_script(
                backend=backend,
                sql_tokens=other_sqls_to_run
            )

        self.tables_for_creation.clear()
        self.tables_for_deletion.clear()
        self.migrated = True

        schemas = []

        # This section will write to the migration file
        for name, table in table_instances.items():
            schema = self.schemas[table.name]
            self.JSON_MIGRATIONS_SCHEMA.tables.append(schema)

            for constraint in table.table_constraints:
                schema.constraints[constraint.name] = [
                    constraint.name,
                    constraint.as_sql(backend)
                ]

            for index in table.indexes:
                schema.indexes[index.index_name] = [
                    index.fields
                ]

            schemas.append(schema.prepare())

        with open(self.database.path.joinpath('migrations.json'), mode='w+') as f:
            self.JSON_MIGRATIONS_SCHEMA.id = secrets.token_hex(5)
            self.JSON_MIGRATIONS_SCHEMA.date = str(datetime.datetime.now())
            self.JSON_MIGRATIONS_SCHEMA.number += 1
            self.JSON_MIGRATIONS_SCHEMA.tables = schemas
            json.dump(
                self.JSON_MIGRATIONS_SCHEMA,
                f,
                indent=4,
                ensure_ascii=False,
                cls=DefaultJSonEncoder
            )

            # try:
            #     migrations_table.objects.create(
            #         db_name=self.database_name,
            #         name=self.JSON_MIGRATIONS_SCHEMA.id,
            #         migration=dataclasses.asdict(self.JSON_MIGRATIONS_SCHEMA)
            #     )
            # except Exception:
            #     raise

    def check_fields(self, table: TypeTable, backend: TypeSQLiteBackend):
        """TODO: Checks the migration file for fields
        in relationship with the table"""
        database_table_columns = backend.list_table_columns(table)

        columns_to_create = set()
        for field_name in table.fields_map.keys():
            if field_name not in database_table_columns:
                columns_to_create.add(field_name)

        # TODO: Drop columns that were dropped in the database

        self.schemas[table.name].prepare()
        backend.create_table_fields(table, columns_to_create)

    def blank_migration(self, using: JsonMigrationsSchema):
        """Creates a blank initial migration file"""
        with open(self.file, mode='w+') as f:
            json.dump(
                dataclasses.asdict(using),
                f,
                indent=4,
                ensure_ascii=False
            )
        return using

    def get_table_fields(self, name):
        table_index = self.database.table_map.index(name)
        return self.tables[table_index]['fields']

    def reconstruct_table_fields(self, table):
        reconstructed_fields = []
        fields = self.get_table_fields(table)
        for field in fields:
            instance = Field.create(
                field['name'],
                field['params'],
                verbose_name=field['verbose_name']
            )
            reconstructed_fields.append(instance)
        return reconstructed_fields

    def write_to_sql_file(self, statement_or_statements: str | list[str]):
        """Writes the different SQL statements performed by the migration
        class to a physical SQL file for reference and later usage

        Args:
            statement_or_statements (str | list[str]): The SQL statement or list of SQL statements
        """
        with open(self.database.path / 'migrations.sql', mode='a+') as f:
            with open(self.database.path / 'migrations.sql', mode='r') as fr:
                content = fr.read().splitlines()

            f.write(f'\n-- Migration executed on {datetime.datetime.now()}\n')

            if isinstance(statement_or_statements, list):
                for statement in statement_or_statements:
                    statement = statement + ';'
                    if statement in content:
                        continue

                    f.write(statement)
                    f.write('\n')
            else:
                if not statement_or_statements in content:
                    f.write(statement_or_statements + ';')
