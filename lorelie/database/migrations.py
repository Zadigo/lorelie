import datetime
import dataclasses
import json
import secrets
from collections import defaultdict
from dataclasses import dataclass, field
from functools import cached_property
from typing import TYPE_CHECKING, DefaultDict, Final, Optional, Type

from lorelie.backends import SQLiteBackend, connections
from lorelie.database.tables.base import Table
from lorelie.fields.base import CharField, DateTimeField, JSONField
from lorelie.lorelie_typings import TypeTableMap
from lorelie.queries import Query
from lorelie.utils.json_encoders import DefaultJSonEncoder

if TYPE_CHECKING:
    from lorelie.database.base import Database


@dataclass
class Schema:
    table: Optional[Table] = None
    database: Optional['Database'] = None

    def __hash__(self):
        return hash((self.table.name, self.database.database_name))

    def __iter__(self):
        for key, value in self.deconstruct().items():
            yield key, value

    def deconstruct(self):
        """Transform the instances into serializable
        data that can be written to a migration file"""
        return {**self.database.deconstruct()}


@dataclass
class JsonMigrationsSchema:
    id: Optional[str] = None
    date: Optional[str] = None
    number: Optional[int] = None
    migrated: Optional[bool] = False
    schema: list[Schema] = field(default_factory=list)

    def __post_init__(self):
        self.id = self.id or secrets.token_hex(5)
        self.date = self.date or str(datetime.datetime.now())
        self.number = self.number or 1

    def __iter__(self):
        schema = self.schema[0].deconstruct()

        template = {
            'id': self.id,
            'date': self.date,
            'number': self.number,
            'migrated': self.migrated,
            'schema': schema
        }

        for key, value in template.items():
            yield key, value

    @property
    def _table_names(self):
        return [schema.table.name for schema in self.schema]

    def get_schema(self, name: str) -> Optional[Schema]:
        for table in self.schema:
            if table.table and table.table.name == name:
                return table
        raise ValueError(f'Table {name} not found in migration schema')


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
        self.database = database
        self.database_name = database.database_name or 'memory'
        self.migrations_json_path = database.path / \
            f'{self.database_name}_migrations.json'
        self.migrations_sql_path = database.path / \
            f'{self.database_name}_migrations.sql'

        self.JSON_MIGRATIONS_SCHEMA = self.read_json_migrations
        self.file_id = self.JSON_MIGRATIONS_SCHEMA.id

        self.SQL_MIGRATIONS_SCHEMA = self.read_sql_migrations

        try:
            self.tables: list[str] = self.JSON_MIGRATIONS_SCHEMA.schema
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
        self.migrated = all([
            self.JSON_MIGRATIONS_SCHEMA.migrated,
            self.SQL_MIGRATIONS_SCHEMA is not None
        ])
        self.schemas: DefaultDict[str, Schema] = defaultdict(Schema)

    def __repr__(self):
        return f'<{self.__class__.__name__} {self.file_id}>'

    @property
    def in_memory(self):
        return self.database_name is None

    @cached_property
    def read_json_migrations(self):
        try:
            with open(self.migrations_json_path, mode='r') as f:
                data = json.load(f)
                return JsonMigrationsSchema(**data)
        except FileNotFoundError:
            # Create a blank migration file
            instance = JsonMigrationsSchema()
            return self.blank_migration(instance)

    @cached_property
    def read_sql_migrations(self):
        try:
            with open(self.migrations_sql_path, mode='r') as f:
                return f.read()
        except FileNotFoundError:
            with open(self.migrations_sql_path, mode='w+') as f:
                f.write('-- Lorelie SQL Migrations File\n')
            return ''

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

    def _build_migration_table(self, name: str = 'migrations'):
        """Creates a migrations table in the database
        which stores the different configuration for
        the current database"""
        table = Table(
            name,
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
        # When we reload the migration file from the JSON,
        # we need to make sure we reload the Python objects
        # for the rest of the code
        if self.JSON_MIGRATIONS_SCHEMA.schema and self.JSON_MIGRATIONS_SCHEMA.migrated:
            reloaded_schemas = []
            for item in self.JSON_MIGRATIONS_SCHEMA.schema:
                json_table = item['tables'][0]
                table = table_instances[json_table['name']]
                reloaded_schemas.append(
                    Schema(
                        table=table, 
                        database=self.database
                    )
                )
            self.JSON_MIGRATIONS_SCHEMA.schema = reloaded_schemas

        current_tables = set(self.JSON_MIGRATIONS_SCHEMA._table_names)
        incoming_table_names: set[str] = set(table_instances.keys())

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

        if incoming_table_names != current_tables:
            self.migrated = False

        # Safeguard that avoids calling
        # this function in a loop over and
        # over which can reduce performance
        if self.migrated:
            return True

            # Compare the incoming tables with the
        # existing ones from from the migration file
        # and determine whether the code should
        # proceed or not
        self.tables_for_creation = incoming_table_names.difference(
            current_tables
        )
        self.tables_for_deletion = current_tables.difference(
            incoming_table_names
        )

        # This will hold the different SQL statements
        # that will be executed to perform the
        # migration as single transaction
        sql_statements = []

        if self.tables_for_deletion:
            for name in self.tables_for_deletion:
                schema = self.schemas[name]
                sql_statements.extend(schema.table.drop_table_sql())

        if 'migrations' not in self.migration_table_map:
            self.tables_for_creation.add('migrations')

            migrations_table = self._build_migration_table()
            table_instances['migrations'] = migrations_table

            new_schema = self.schemas['migrations']
            new_schema.table = migrations_table
            new_schema.database = self.database
            self.database._add_table(migrations_table)

        if self.tables_for_creation:
            for name in self.tables_for_creation:
                table = table_instances.get(name, None)

                if table is None:
                    continue

                sql_statements.extend(
                    table.prepare(
                        self.database,
                        skip_creation=True
                    )
                )

        fields_to_check = defaultdict(dict)
        # Now here we check for existing tables
        # that might need to be altered. We start from
        # a macro level: indexes, constraints, fields
        # existing_tables = current_tables.intersection(incoming_table_names)
        for name in current_tables:
            schema = self.JSON_MIGRATIONS_SCHEMA.get_schema(name)
            if schema is not None:
                if len(schema.table.indexes) > 0:
                    pass

                if len(schema.table.table_constraints) > 0:
                    pass

                fields_to_check[name] = {
                    'existing_fields': {
                        field['name']: field
                        for field in schema.table.fields_map.values()
                    },
                    'incoming_fields': {
                        field.name: field
                        for field in table_instances[name].fields_map.values()
                    }
                }

        for _, schema in self.schemas.items():
            self.JSON_MIGRATIONS_SCHEMA.schema.append(schema)

        backend = connections.get_last_connection()
        Query.run_transaction(backend=backend, sql_tokens=sql_statements)

        # Finalize by writing to the migration files
        with open(self.migrations_json_path, mode='w+') as f:
            self.JSON_MIGRATIONS_SCHEMA.migrated = True
            self.JSON_MIGRATIONS_SCHEMA.id = secrets.token_hex(5)
            self.JSON_MIGRATIONS_SCHEMA.date = str(datetime.datetime.now())
            self.JSON_MIGRATIONS_SCHEMA.number += 1
            final_migration = dict(self.JSON_MIGRATIONS_SCHEMA)

            json.dump(
                final_migration,
                f,
                indent=4,
                ensure_ascii=False,
                cls=DefaultJSonEncoder
            )
            self.write_to_sql_file(sql_statements)

            self.tables_for_creation.clear()
            self.tables_for_deletion.clear()


    def blank_migration(self, using: JsonMigrationsSchema):
        """Creates a blank initial migration file"""
        with open(self.migrations_json_path, mode='w+') as f:
            json.dump(
                dataclasses.asdict(using),
                f,
                indent=4,
                ensure_ascii=False
            )
        return using

    def write_to_sql_file(self, statement_or_statements: str | list[str]):
        """Writes the different SQL statements performed by the migration
        class to a physical SQL file for reference and later usage

        Args:
            statement_or_statements (str | list[str]): The SQL statement or list of SQL statements
        """
        with open(self.migrations_sql_path, mode='a+') as f:
            with open(self.migrations_sql_path, mode='r') as fr:
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
