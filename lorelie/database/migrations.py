import datetime
import json
import secrets
from collections import defaultdict
import dataclasses
from dataclasses import dataclass, field
from functools import cached_property

from lorelie.backends import SQLiteBackend, connections
from lorelie.fields.base import CharField, DateTimeField, Field, JSONField
from lorelie.queries import Query
from lorelie.database.tables.base import Table


@dataclass
class Schema:
    table: type = None
    database: type = None
    fields: list = field(default_factory=list)
    field_params: list = field(default_factory=list)
    indexes: dict = field(default_factory=dict)
    constraints: dict = field(default_factory=dict)

    def __iter__(self):
        for field in dataclasses.fields(self):
            item = getattr(self, field.name)

            if field.name == 'table':
                item.load_current_connection()
                yield ('name', item.name)
                yield ('field_params', list(item.build_all_field_parameters()))

                constraints = []
                for constraint in item.table_constraints:
                    sql = constraint.as_sql(self.table.backend)
                    constraints.append((constraint.name, sql))
                yield ('constraints', constraints)

                indexes = []
                for index in item.indexes:
                    sql = index.as_sql(self.table.backend)
                    indexes.append((index.name, sql))
                yield ('indexes', indexes)

            if field.name == 'fields':
                yield ('fields', getattr(self, field.name))

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


class Migrations:
    """This class manages the different
    states of a given database. It references
    existing tables, dropped tables and their
    fields and runs the different methods required
    to create or delete them eventually"""

    CACHE = {}
    backend_class = SQLiteBackend

    def __init__(self, database):
        self.file = database.path / 'migrations.json'
        self.database = database
        self.database_name = database.database_name or 'memory'
        self.CACHE = self.read_content
        self.file_id = self.CACHE['id']

        try:
            self.tables = self.CACHE['tables']
        except KeyError:
            raise KeyError('Migration file is not valid')

        self.migration_table_map = [table['name'] for table in self.tables]
        self.fields_map = defaultdict(list)

        self.tables_for_creation = set()
        self.tables_for_deletion = set()
        self.existing_tables = set()
        self.has_migrations = False
        # Indicates that check function was
        # called at least once and that the
        # the underlying database can be
        # fully functionnal
        self.migrated = False
        self.schemas = defaultdict(Schema)
        self.pending_migration = {}

    def __repr__(self):
        return f'<{self.__class__.__name__} {self.file_id}>'

    @property
    def in_memory(self):
        return self.database_name is None

    @cached_property
    def read_content(self):
        try:
            with open(self.file, mode='r') as f:
                return json.load(f)
        except FileNotFoundError:
            # Create a blank migration file
            return self.blank_migration()

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

    def create_migration_table(self):
        """Creates a migrations table in the database
        which stores the different configuration for
        the current database"""
        from lorelie.database.tables.base import Table
        table_fields = [
            CharField('name', null=False, unique=True),
            CharField('table_name', null=False),
            JSONField(
                'migration',
                null=False,
                validators=[migration_validator]
            ),
            DateTimeField('applied', auto_add=True)
        ]
        table = Table(
            'lorelie_migrations',
            fields=table_fields,
            str_field='name'
        )
        self.database._add_table(table)

    def migrate(self, table_instances):
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

        backend = connections.get_last_connection()
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
            self.create_migration_table()
            self.tables_for_creation.add('lorelie_migrations')

        # Eventually create the tables
        if self.tables_for_creation:
            for table_name in self.tables_for_creation:
                table = table_instances.get(table_name, None)
                if table is None:
                    continue
                
                # By security, skip any tables that would
                # overrun the previous check for existing
                # tables to not be created
                if table_name in database_tables:
                    continue

                # This is the specific section
                # that actually creates the table
                # in the database
                table.load_current_connection()
                table.prepare(self.database)
            self.has_migrations = True

        other_sqls_to_run = []

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

            # If the tables already exist, we
            # still need to have them prepared
            # by attaching the database to them
            # otherwise we might get errors when
            # trying to run operations later on
            table_instance.prepare(self.database, skip_creation=True)
            self.check_fields(table_instance, backend)

        database_indexes = backend.list_database_indexes()
        for name, table in table_instances.items():
            for index in table.indexes:
                other_sqls_to_run.append(index.as_sql(backend))

        # TODO: For now we will not remove obsolete
        # indexes too. We will implement this after too
        # Remove obsolete indexes
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

        if self.pending_migration:
            params = {
                'name': f'mig_{secrets.token_hex(5)}',
                'table_name': None,
                'migration': self.pending_migration
            }
            try:
                self.database.objects.create('lorelie_migrations', **params)
            except Exception:
                raise
            finally:
                self.pending_migration = {}

        # The database might require another set of
        # parameters (ex. indexes, constraints) that we
        # are going to run here
        Query.run_script(backend=backend, sql_tokens=other_sqls_to_run)

        self.tables_for_creation.clear()
        self.tables_for_deletion.clear()
        self.migrated = True

    def check_fields(self, table, backend):
        """Checks the migration file for fields
        in relationship with the table"""
        database_table_columns = backend.list_table_columns(table)

        columns_to_create = set()
        for name, field in table.fields_map.items():
            # Relationship fields are different and
            # require checking the related name as
            # opposed to the field name
            if getattr(field, 'is_relationship_field', False):
                if field.related_name not in database_table_columns:
                    columns_to_create.add(field.related_name)
                continue

            if name not in database_table_columns:
                columns_to_create.add(name)

        # TODO: Drop columns that were dropped in the database

        self.schemas[table.name].fields = list(
            map(
                lambda x: x['name'], 
                    database_table_columns
            )
        )
        backend.create_table_fields(table, columns_to_create)

    def blank_migration(self):
        """Creates a blank initial migration file"""
        migration_content = {}

        # file_path = PROJECT_PATH / 'migrations.json'
        file_path = self.database.path / 'migrations.json'
        if not file_path.exists():
            file_path.touch()

        with open(file_path, mode='w') as f:
            migration_content['id'] = secrets.token_hex(5)
            migration_content['date'] = str(datetime.datetime.now())
            migration_content['number'] = 1

            migration_content['tables'] = []
            json.dump(migration_content, f, indent=4, ensure_ascii=False)
            return migration_content

    def make_migrations(self, tables):
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
            with open(self.database.path.joinpath('migrations.json'), mode='w+') as f:
                cache_copy['id'] = secrets.token_hex(5)
                cache_copy['date'] = str(datetime.datetime.now())
                cache_copy['number'] = self.CACHE['number'] + 1
                cache_copy['tables'] = migration['tables']
                json.dump(cache_copy, f, indent=4, ensure_ascii=False)

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
