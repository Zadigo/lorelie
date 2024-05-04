from asgiref.sync import sync_to_async
from collections import OrderedDict, namedtuple

from lorelie.backends import SQLiteBackend
from lorelie.exceptions import ImproperlyConfiguredError
from lorelie.fields import AutoField, Field
from lorelie.migrations import Migrations
from lorelie.queries import Query, QuerySet


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
    query_class = Query
    backend_class = SQLiteBackend

    def __init__(self, database_name=None, inline_build=False):
        self.backend = None
        self.is_prepared = False
        # if inline_build:
        #     self.backend = self.backend_class(
        #         database_name=database_name,
        #         table=self
        #     )

    def __hash__(self):
        return hash((self.name))

    def __eq__(self, value):
        return self.name == value

    def validate_values(self, fields, values):
        """Validate an incoming value in regards
        to the related field the user is trying
        to set on the column. The returned values
        are quoted by default"""
        validates_values = []
        for i, field in enumerate(fields):
            if field == 'rowid' or field == 'id':
                continue

            field = self.fields_map[field]
            validated_value = self.backend.quote_value(
                field.to_database(list(values)[i])
            )
            validates_values.append(validated_value)
        return validates_values

    def set_current_table(self):
        from lorelie.backends import connections
        # setattr(self.backend, 'current_table', self)
        self.backend = connections.get_last_connection()


class Table(AbstractTable):
    """Represents a table in the database. This class
    can be used independently but would require creating
    and managing table creation

    To create a table without using `Database`:

    >>> table = Table('my_table', 'my_database', fields=[Field('url')])
    ... table.prepare()
    ... table.create(url='http://example.come')

    However, if you wish to manage a migration file and other table related
    tasks, wrapping tables in `Database` is the best option:

    >>> table = Table('my_table', 'my_database', fields=[Field('url')])
    ... database = Database('my_database', table)
    ... database.make_migrations()
    ... database.migrate()
    ... table.create(url='http://example.com')
    """

    def __init__(self, name, *, database_name=None, inline_build=False, fields=[], index=[], constraints=[]):
        self.name = name
        self.indexes = index
        self.constraints = constraints
        self.inline_build = inline_build
        super().__init__(
            database_name=database_name,
            inline_build=inline_build
        )
        self.fields_map = OrderedDict()

        for field in fields:
            if not isinstance(field, Field):
                raise ValueError(f'{field} should be an instance of Field')

            field.prepare(self)
            self.fields_map[field.name] = field

        # Automatically create an ID field
        self.fields_map['id'] = AutoField()

        field_names = list(self.fields_map.keys())
        field_names.append('rowid')
        self.field_names = field_names

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.name}>'

    def has_field(self, name):
        return name in self.fields_map

    def create_table_sql(self, fields):
        sql = self.backend.CREATE_TABLE.format_map({
            'table': self.name,
            'fields': fields
        })
        return [sql]

    def drop_table_sql(self, name):
        sql = self.backend.DROP_TABLE.format_map({
            'table': name
        })
        return [sql]

    def build_field_parameters(self):
        """Returns the paramaters for all
        the fields present on the table"""
        return [
            field.field_parameters()
            for field in self.fields_map.values()
        ]

    def prepare(self):
        """Prepares and creates a table for
        the database"""
        if not self.inline_build and self.backend is None:
            message = (
                "Calling the Table class outside of Database "
                "requires that you set 'inline_build' to True"
            )
            raise ImproperlyConfiguredError(self, message)

        field_params = self.build_field_parameters()
        field_params = [
            self.backend.simple_join(params)
            for params in field_params
        ]
        sql = self.create_table_sql(self.backend.comma_join(field_params))
        query = self.query_class(self.backend, sql, table=self)
        query.run(commit=True)
        self.is_prepared = True


class Databases:
    """A class that remembers the databases
    that were created and allows their retrieval
    if needed from other sections of the code"""

    def __init__(self):
        self.database_map = {}
        self.created_databases = list(self.database_map.values())

    def __getitem__(self, name):
        return self.database_map[name]

    def __contains__(self, value):
        return value in self.created_databases

    def register(self, database):
        if not isinstance(database, Database):
            raise ValueError('Value should be an instance of Database')
        self.database_map[database.database_name] = database


databases = Databases()


class DatabaseManager:
    """A manager is a class that implements query
    functionnalities for inserting, updating, deleting
    or retrieving data from the underlying database tables"""

    def __init__(self):
        self.table_map = {}
        self.database = None

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.database}>'

    def __get__(self, instance, cls=None):
        if not self.table_map:
            self.table_map = instance.table_map
            self.database = instance
        return self

    def first(self, table):
        """Returns the first row from
        a database table"""
        result = self.all(table)
        return result[0]

    def last(self, table):
        """Returns the last row from
        a database table"""
        result = self.all(table)
        return result[-1]

    def all(self, table):
        selected_table = self.table_map[table]
        selected_table.set_current_table()

        all_sql = selected_table.backend.SELECT.format_map({
            'fields': selected_table.backend.comma_join(['rowid', '*']),
            'table': selected_table.name
        })
        sql = [all_sql]
        query = selected_table.query_class(
            selected_table.backend,
            sql,
            table=selected_table
        )
        query._table = selected_table
        query.run()
        return query.result_cache

    def create(self, table, **kwargs):
        """Creates a new row in the table of 
        the current database

        >>> database.objects.create('celebrities', name='Kendall')
        """
        selected_table = self.table_map[table]
        selected_table.set_current_table()

        fields, values = selected_table.backend.dict_to_sql(
            kwargs, quote_values=False)
        values = selected_table.validate_values(fields, values)

        joined_fields = selected_table.backend.comma_join(fields)
        joined_values = selected_table.backend.comma_join(values)
        sql = selected_table.backend.INSERT.format(
            table=selected_table.name,
            fields=joined_fields,
            values=joined_values
        )
        query = selected_table.query_class(
            selected_table.backend,
            [sql],
            table=selected_table
        )
        query.run(commit=True)
        return self.last(selected_table.name)

    def filter(self, table, **kwargs):
        """Filter the data in the database based on
        a set of criteria

        >>> database.objects.filter('celebrities', name='Kendall')
        ... database.objects.filter('celebrities', name__eq='Kendall')
        ... database.objects.filter('celebrities', age__gt=15)
        ... database.objects.filter('celebrities', name__in=['Kendall'])
        """
        selected_table = self.table_map[table]
        selected_table.set_current_table()

        tokens = selected_table.backend.decompose_filters(**kwargs)
        filters = selected_table.backend.build_filters(tokens)

        if len(filters) > 1:
            filters = [
                selected_table.backend.wrap_parenthentis(
                    ' and '.join(filters)
                )
            ]

        select_sql = selected_table.backend.SELECT.format_map({
            'fields': selected_table.backend.comma_join(['rowid', '*']),
            'table': selected_table.name,
        })
        where_clause = selected_table.backend.WHERE_CLAUSE.format_map({
            'params': selected_table.backend.comma_join(filters)
        })
        # TODO: Use the Query class on the table
        # or whether to import and call it directly ?
        query = selected_table.query_class(
            selected_table.backend,
            [select_sql, where_clause],
            table=selected_table
        )
        query.run()
        return query.result_cache

    # async def aget(self, table, **kwargs):
    #     return await sync_to_async(self.get)(table, **kwargs)

    def get(self, table, **kwargs):
        """Returns a specific row from the database
        based on a set of criteria

        >>> instance.objects.get('celebrities', id__eq=1)
        ... instance.objects.get('celebrities', id=1)
        """
        selected_table = self.table_map[table]
        selected_table.set_current_table()

        base_return_fields = ['rowid', '*']
        filters = selected_table.backend.build_filters(
            selected_table.backend.decompose_filters(**kwargs)
        )

        # Functions SQL: select rowid, *, lower(url) from table
        select_sql = selected_table.backend.SELECT.format_map({
            'fields': selected_table.backend.comma_join(base_return_fields),
            'table': selected_table.name
        })
        sql = [select_sql]

        # FIXME: Use operator_join
        # Filters SQL: select rowid, * from table where url='http://'
        # joined_statements = ' and '.join(filters)
        joined_statements = selected_table.backend.operator_join(filters)
        where_clause = selected_table.backend.WHERE_CLAUSE.format_map({
            'params': joined_statements
        })
        sql.extend([where_clause])

        query = selected_table.query_class(
            selected_table.backend,
            sql,
            table=selected_table
        )
        query.run()

        if not query.result_cache:
            return None

        if len(query.result_cache) > 1:
            raise ValueError('Returned more than 1 value')

        return query.result_cache[0]

    def annotate(self, table, **kwargs):
        """Annotations implements the usage of
        functions in the query

        For example, if we want the iteration of each
        value in the database to be returned in lowercase
        or in uppercase

        >>> instance.objects.annotate('celebrities', lowered_name=Lower('name'))
        ... instance.objects.annotate('celebrities', uppered_name=Upper('name'))

        If we want to return only the year section of a date

        >>> self.annotate(year=ExtractYear('created_on'))
        """
        selected_table = self.table_map[table]
        selected_table.set_current_table()

        base_return_fields = ['rowid', '*']
        sql_functions_dict, special_function_fields, fields = selected_table.backend.build_annotation(
            **kwargs
        )
        base_return_fields.extend(fields)

        selected_table.field_names = selected_table.field_names + \
            list(kwargs.keys())

        sql = selected_table.backend.SELECT.format_map({
            'fields': selected_table.backend.comma_join(base_return_fields),
            'table': selected_table.name
        })

        # TODO: Adapt this section so that the Case function
        # can parsed and created
        if special_function_fields:
            groupby_sql = selected_table.backend.GROUP_BY.format_map({
                'conditions': selected_table.backend.comma_join(special_function_fields)
            })
            sql = selected_table.backend.simple_join([sql, groupby_sql])

        # TODO: Create a query and only run it when
        # we need with QuerySet for the other functions
        query = Query(
            selected_table.backend,
            [sql],
            table=selected_table
        )
        return QuerySet(query)

    def as_values(self, table, *args):
        """Returns data from the database as a list
        of dictionnary values

        >>> instance.objects.as_values('celebrities', 'id')
        ... [{'id': 1}]
        """
        selected_table = self.table_map[table]
        selected_table.set_current_table()

        sql = selected_table.backend.SELECT.format_map({
            'fields': selected_table.backend.comma_join(list(args)),
            'table': selected_table.name
        })
        query = Query(selected_table.backend, [sql], table=selected_table)

        def dict_iterator(values):
            for row in values:
                yield row._cached_data
        query.run()
        return list(dict_iterator(query.result_cache))

    def as_dataframe(self, table, *args):
        """Returns data from the database as a
        pandas DataFrame object

        >>> instance.objects.as_dataframe('celebrities', 'id')
        ... pandas.DataFrame
        """
        import pandas
        return pandas.DataFrame(self.as_values(table, *args))


class Database:
    """This class links and unifies independent
    tables together for a unique database and allows 
    its management via a migration file.

    Creating a new database can be done by doing the following steps:

    >>> table = Table('my_table', 'my_database', fields=[Field('url')])
    ... database = Database('my_database', table)
    ... database.make_migrations()
    ... database.migrate()

    Once the database is created, we can then run various operations
    on the tables:

    >>> database.objects.create('my_table', url='http://example.com')

    Connections to the database can either be opened at the table level
    or at the database level ???

    `make_migrations` writes the physical changes to the
    local tables into the `migrations.json` file

    `migrate` implements the changes from the migration
    file into the SQLite database. It syncs the changes from
    the file into the database such as deleting or updating
    existing tables
    """

    migrations = None
    migrations_class = Migrations
    backend_class = SQLiteBackend
    objects = DatabaseManager()

    def __init__(self, name, *tables):
        self.database_name = name
        self.migrations = self.migrations_class(database_name=name)

        new_connection = self.backend_class(
            database_name=name
        )

        self.table_map = {}
        for table in tables:
            if not isinstance(table, Table):
                raise ValueError('Value should be an instance of Table')

            # if table.backend is None:
            #     table.backend = table.backend_class(
            #         database_name=self.database_name,
            #         table=table
            #     )
            if table.backend is None:
                table.backend = new_connection
            self.table_map[table.name] = table

        self.table_instances = list(tables)
        databases.register(self)

    def __repr__(self):
        tables = list(self.table_map.values())
        return f'<{self.__class__.__name__} {tables}>'

    def __getitem__(self, table_name):
        return self.table_map[table_name]

    def __hash__(self):
        return hash((self.database_name))

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
        """Implements the changes to the migration
        file into the SQLite database"""
        self.migrations.check(self.table_map)
