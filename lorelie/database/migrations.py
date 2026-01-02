import datetime
import dataclasses
import json
import secrets
from collections import defaultdict
from dataclasses import dataclass, field
from functools import cached_property
from typing import Any, Final, Optional, Type
from io import StringIO
from lorelie.backends import SQLiteBackend, connections
from lorelie.database.indexes import Index
from lorelie.database.tables.base import Table
from lorelie.fields.base import CharField, DateTimeField, JSONField
from lorelie.lorelie_typings import TypeConstraint, TypeDatabase, TypeDeconstructedField, TypeDeconstructedIndex, TypeField, TypeTable, TypeTableMap
from lorelie.queries import Query
from lorelie import lorelie_logger

TypeFieldsToCheck = defaultdict[str, dict[str, TypeField]]


@dataclass
class JsonMigrationsSchema:
    id: Optional[str] = None
    date: Optional[str] = None
    number: Optional[int] = None
    migrated: bool = False
    in_memory: bool = False
    schema: dict = field(default_factory=dict)

    def __post_init__(self):
        self.id = self.id or secrets.token_hex(5)
        self.date = self.date or str(datetime.datetime.now())
        self.number = self.number or 1

    def __iter__(self):
        template = {
            'id': self.id,
            'date': self.date,
            'number': self.number,
            'migrated': self.migrated,
            'in_memory': self.in_memory,
            'schema': self.schema
        }

        for key, value in template.items():
            yield key, value

    @property
    def _table_names(self) -> set[str]:
        tables = self.schema.get('tables', [])
        return {item['name'] for item in tables}

    def get_table_indexes(self, table_name: str) -> list[TypeDeconstructedIndex]:
        table = self.get_table(table_name)
        if table is None:
            return set()

        return table.get('indexes', [])

    def get_table(self, table_name: str) -> Optional[dict[str, Any]]:
        """Returns the table schema for a given table
        in the current migration schema"""
        tables = self.schema.get('tables', [])
        for item in tables:
            if item.get('name', '') == table_name:
                return item
        return None

    def get_table_fields(self, table_name: str) -> list[TypeDeconstructedField]:
        """Returns the fields map for a given table
        in the current migration schema"""
        json_table = self.get_table(table_name)
        if json_table:
            return json_table.get('fields', [])
        return None

    def get_table_field(self, table_name: str, field_name: str) -> TypeDeconstructedField:
        """Returns the field parameters for a given field
        in a given table from the current migration schema"""
        json_fields = self.get_table_fields(table_name)
        if json_fields:
            for field_type, name, params in json_fields:
                if name == field_name:
                    return (field_type, name, params)
        return None

    def table_has_field(self, table_name: str, field_name: str) -> bool:
        """Checks whether a given table has a field in the
        current migration schema"""
        return self.get_table_field(table_name, field_name) is not None


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

    def __init__(self, database: TypeDatabase):
        self.database = database
        self.database_name = database.database_name or 'memory'

        self.migrations_json_path = database.path / \
            f'{self.database_name}_migrations.json'
        self.migrations_sql_path = database.path / \
            f'{self.database_name}_migrations.sql'

        self.JSON_MIGRATIONS_SCHEMA = self.read_json_migrations
        self.file_id = self.JSON_MIGRATIONS_SCHEMA.id
        self.JSON_MIGRATIONS_SCHEMA.in_memory = database.in_memory

        self.SQL_MIGRATIONS_SCHEMA = self.read_sql_migrations

        self.fields_map = defaultdict(list)

        self.existing_tables = self.JSON_MIGRATIONS_SCHEMA._table_names
        self.tables_for_creation: set[str] = set()
        self.tables_for_deletion: set[str] = set()
        # Indacate the current migration run
        # is for updating existing tables
        # therefore skipping the creation
        # the migrations table
        self.for_update = False
        # Indicates that check function was
        # called at least once and that the
        # the underlying database can be
        # fully functionnal
        self.migrated = all([
            self.JSON_MIGRATIONS_SCHEMA.migrated,
            self.SQL_MIGRATIONS_SCHEMA is not None
        ])

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

    def _build_migration_table(self, name: str = 'migrations'):
        """Creates a migrations table in the database
        which stores the different configuration for
        the current database"""
        table = Table(
            name,
            fields=[
                CharField('name', unique=True),
                CharField('database'),
                JSONField('migration', validators=[migration_validator]),
                DateTimeField('applied', auto_add=True)
            ],
            str_field='name'
        )
        return table

    def _check_table_indexes(self, table: TypeTable):
        old_indexes = self.JSON_MIGRATIONS_SCHEMA.get_table_indexes(
            table.name
        )

        # https://sqlite.org/pragma.html
        create_sql_statements: list[str] = [
            'pragma optimize'
        ]

        # Search for new indexes on the table
        # instance that are not present
        # in the existing migration schema
        for index in table.indexes:
            for name, _, _ in old_indexes:
                if index.name != name:
                    lorelie_logger.info(
                        f"✅ Creating index '{index.name}' "
                        f"on table '{table.name}'..."
                    )
                    create_sql_statements.extend([index.as_sql(table.backend)])
                    continue

        # Search for indexes that are present
        # in the existing migration schema
        # but not in the current table instance
        remove_sql_statements: list[str] = []

        for name, fields, _ in old_indexes:
            if name not in table.indexes:
                old_index_instance = Index(name, fields)
                lorelie_logger.info(
                    f"❌ Dropping index '{name}' "
                    f"from table '{table.name}'..."
                )
                remove_sql_statements.extend(
                    table.drop_index_sql(old_index_instance)
                )

        return create_sql_statements + remove_sql_statements

    def _check_table_constraints(self, constraints: list[TypeConstraint]):
        return []

    def _check_existing_tables(self, table_instances: TypeTableMap, current_user_tables: set[str], sql_statements: list[str] = []):
        """"Helper method for checking the state of existing tables
        that might need to be altered"""
        if self.JSON_MIGRATIONS_SCHEMA.in_memory:
            return

        fields_to_check: TypeFieldsToCheck = defaultdict(dict)
        # Now here we check for existing tables
        # that might need to be altered. We start from
        # a macro level: indexes, constraints, fields
        for name in current_user_tables:
            table = table_instances[name]
            sql_statements.extend(self._check_table_indexes(table))
            sql_statements.extend(self._check_table_constraints(table))

            # Now check changes at the field level
            for field_name in table.fields_map.keys():
                pass

    def migrate(self, table_instances: TypeTableMap, dry_run: bool = False):
        lorelie_logger.info("🔄 Starting migration process...")

        incoming_table_names: set[str] = set(table_instances.keys())

        errors = []
        for name, table_instance in table_instances.items():
            if not isinstance(table_instance, Table):
                errors.append(
                    f"Value should be instance "
                    f"of Table. Got: {table_instance}"
                )

        if errors:
            raise ValueError(*errors)

        if not self.JSON_MIGRATIONS_SCHEMA.migrated and not table_instances:
            # The user is trying to migrate without any tables
            # and without having previously migrated the database
            lorelie_logger.warning(
                "🔴 No tables were found to migrate. Aborting migration process."
            )
            return False

        # Only tracks tables that are user-defined
        _current_user_tables = set(self.existing_tables)
        if 'migrations' in _current_user_tables:
            _current_user_tables.remove('migrations')

        # Check for tables that might have been
        # deleted from the incoming tables
        if incoming_table_names != _current_user_tables:
            self.migrated = False
            self.for_update = True

        # In memory databases need to be recreated all the time
        # since the tables are not persistent
        if self.JSON_MIGRATIONS_SCHEMA.in_memory:
            self.migrated = False
            self.for_update = True

        if self.migrated:
            lorelie_logger.info(
                "🔄 Reloading database schema from "
                f"the existing migration file: {self.migrations_json_path.absolute()}..."
            )

            lorelie_logger.info("✅ No tables were found to migrate.")

            statements = []

            # We need to reload the migration table
            self.database._add_table(self._build_migration_table())

            self._check_existing_tables(
                table_instances,
                self.existing_tables,
                statements
            )

            if statements:
                self.JSON_MIGRATIONS_SCHEMA.schema = self.database.deconstruct()

                self.write_to_json_file(
                    dry_run=dry_run,
                    sql_statements=statements
                )

            return True

        if self.JSON_MIGRATIONS_SCHEMA.in_memory:
            lorelie_logger.info(
                "🔄 In-memory database detected. "
                "Recreating all tables..."
            )
            self.tables_for_creation = incoming_table_names

            try:
                self.existing_tables.remove('migrations')
            except KeyError:
                pass
        else:
            # Compare the incoming tables with the
            # existing ones from from the migration file
            # and determine whether the code should
            # proceed or not
            self.tables_for_creation = incoming_table_names.difference(
                _current_user_tables
            )
            self.tables_for_deletion = _current_user_tables.difference(
                incoming_table_names
            )

        # This will hold the different SQL statements
        # that will be executed to perform the
        # migration as single transaction
        sql_statements = [
            "pragma encoding = 'UTF-8'",
            'pragma cache_size=-64000',
            # 'pragma journal_mode = WAL',
            # 'pragma synchronous = NORMAL',
        ]

        if self.tables_for_deletion:
            for name in self.tables_for_deletion:
                # Since the table is not present
                # in the incoming tables, we have to
                # create a dummy Table instance in order
                # to call the drop sql statement
                table = Table(name)
                table.backend = connections.get_last_connection()
                sql_statements.extend(table.drop_table_sql())
                _current_user_tables.remove(name)
                lorelie_logger.info(f"❌ Dropping table '{name}'...")

        if 'migrations' not in self.existing_tables:
            self.tables_for_creation.add('migrations')

            migrations_table = self._build_migration_table()
            table_instances['migrations'] = migrations_table
            self.database._add_table(migrations_table)

        if self.tables_for_creation:
            for name in self.tables_for_creation:
                table = table_instances.get(name, None)

                if table is None:
                    continue

                lorelie_logger.info(f"🛠 Creating table '{table.name}'...")

                sql_statements.extend(
                    table.prepare(
                        self.database,
                        skip_creation=True
                    )
                )

        # fields_to_check: defaultdict[str, dict[str, TypeField]] = defaultdict(dict)
        # Now here we check for existing tables
        # that might need to be altered. We start from
        # a macro level: indexes, constraints, fields
        self._check_existing_tables(
            table_instances,
            _current_user_tables,
            sql_statements
        )

        # for name in _current_user_tables:
        #     table = table_instances[name]
        #     sql_statements.extend(self._check_table_indexes(table))
        #     sql_statements.extend(self._check_table_constraints(table))

        #     # Now check changes at the field level
        #     for field_name in table.fields_map.keys():
        #         pass

        self.JSON_MIGRATIONS_SCHEMA.schema = self.database.deconstruct()

        if not dry_run:
            backend = connections.get_last_connection()

            try:
                Query.run_transaction(
                    backend=backend,
                    sql_tokens=sql_statements
                )
            except Exception as e:
                raise ExceptionGroup(
                    "An error occurred while executing "
                    "the migration transaction",
                    [e]
                )

        self.write_to_json_file(dry_run=dry_run, sql_statements=sql_statements)

        # buffer = StringIO()

        # self.JSON_MIGRATIONS_SCHEMA.migrated = True
        # self.JSON_MIGRATIONS_SCHEMA.id = secrets.token_hex(5)
        # self.JSON_MIGRATIONS_SCHEMA.date = datetime.datetime.now().isoformat()
        # self.JSON_MIGRATIONS_SCHEMA.number += 1

        # final_migration = dict(self.JSON_MIGRATIONS_SCHEMA)

        # json.dump(final_migration, buffer, indent=4, ensure_ascii=False)

        # if not dry_run:
        #     self.write_to_sql_file(sql_statements)

        #     with open(self.migrations_json_path, mode='w', encoding='utf-8') as f:
        #         value = buffer.getvalue()
        #         f.write(value)

        #         lorelie_logger.info(
        #             f"✅ Migration {self.JSON_MIGRATIONS_SCHEMA.number} "
        #             "applied successfully."
        #         )

        #         try:
        #             # Save the migrations state in the
        #             # migrations table
        #             table = self.database.get_table('migrations')
        #             table.objects.create(
        #                 name=f'Migration {self.JSON_MIGRATIONS_SCHEMA.number}',
        #                 database=self.database.database_name,
        #                 migration=final_migration
        #             )
        #         except Exception as e:
        #             raise TypeError(
        #                 "Could not log migration "
        #                 "in the migrations table."
        #             )

        return True

    def blank_migration(self, using: JsonMigrationsSchema):
        """Creates a blank initial migration file"""
        with open(self.migrations_json_path, mode='w+') as f:
            data = dataclasses.asdict(using)
            json.dump(data, f, indent=4, ensure_ascii=False)
            lorelie_logger.info(f"✅ Created new blank JSON migration file...")
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

            lorelie_logger.info(
                f"✅ Updated SQL migrations file "
                f"at: {self.migrations_sql_path.absolute()}"
            )

    def write_to_json_file(self, dry_run: bool = False, sql_statements: list[str] = []):
        """Writes the different JSON migration schema
        to a physical JSON file for reference and later usage"""
        buffer = StringIO()

        self.JSON_MIGRATIONS_SCHEMA.migrated = True
        self.JSON_MIGRATIONS_SCHEMA.id = secrets.token_hex(5)
        self.JSON_MIGRATIONS_SCHEMA.date = datetime.datetime.now().isoformat()
        self.JSON_MIGRATIONS_SCHEMA.number += 1

        final_migration = dict(self.JSON_MIGRATIONS_SCHEMA)

        json.dump(final_migration, buffer, indent=4, ensure_ascii=False)

        if not dry_run:
            self.write_to_sql_file(sql_statements)

            with open(self.migrations_json_path, mode='w', encoding='utf-8') as f:
                value = buffer.getvalue()
                f.write(value)

                lorelie_logger.info(
                    f"✅ Migration {self.JSON_MIGRATIONS_SCHEMA.number} "
                    "applied successfully."
                )

                try:
                    # Save the migrations state in the
                    # migrations table
                    table = self.database.get_table('migrations')
                    table.objects.create(
                        name=f'Migration {self.JSON_MIGRATIONS_SCHEMA.number}',
                        database=self.database.database_name,
                        migration=final_migration
                    )
                except Exception as e:
                    raise TypeError(
                        "Could not log migration "
                        "in the migrations table."
                    )
