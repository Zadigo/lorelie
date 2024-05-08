from asgiref.sync import sync_to_async

from lorelie.backends import SQLiteBackend
from lorelie.migrations import Migrations
from lorelie.queries import Query, QuerySet
from lorelie.tables import Table


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

    def _get_select_sql(self, selected_table, columns=['rowid', '*']):
        # This function creates and returns the base SQL line for
        # selecting values in the database: "select rowid, * where rowid=1"
        select_sql = selected_table.backend.SELECT.format_map({
            'fields': selected_table.backend.comma_join(columns),
            'table': selected_table.name,
        })
        return [select_sql]

    def before_action(self, table_name):
        table = self.table_map[table_name]
        table.backend.set_current_table(table)

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
        selected_table.load_current_connection()
        self.before_action(selected_table)

        # all_sql = selected_table.backend.SELECT.format_map({
        #     'fields': selected_table.backend.comma_join(['rowid', '*']),
        #     'table': selected_table.name
        # })
        # sql = [all_sql]

        sql = self._get_select_sql(selected_table)

        if bool(selected_table.ordering):
            ordering_sql = selected_table.ordering.as_sql(
                selected_table.backend
            )
            sql.append(ordering_sql)

        query = selected_table.query_class(
            selected_table.backend,
            sql,
            table=selected_table
        )
        query.run()
        return query.result_cache

    def create(self, table, **kwargs):
        """Creates a new row in the table of 
        the current database

        >>> database.objects.create('celebrities', name='Kendall')
        """
        selected_table = self.table_map[table]
        selected_table.load_current_connection()
        self.before_action(selected_table)

        fields, values = selected_table.backend.dict_to_sql(
            kwargs,
            quote_values=False
        )
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
        selected_table.load_current_connection()
        self.before_action(selected_table)

        tokens = selected_table.backend.decompose_filters(**kwargs)
        filters = selected_table.backend.build_filters(tokens)

        if len(filters) > 1:
            filters = [
                selected_table.backend.wrap_parenthentis(
                    ' and '.join(filters)
                )
            ]

        # select_sql = selected_table.backend.SELECT.format_map({
        #     'fields': selected_table.backend.comma_join(['rowid', '*']),
        #     'table': selected_table.name,
        # })
        select_sql = self._get_select_sql(selected_table)
        where_clause = selected_table.backend.WHERE_CLAUSE.format_map({
            'params': selected_table.backend.comma_join(filters)
        })
        select_sql.append(where_clause)

        # TODO: Use the Query class on the table
        # or whether to import and call it directly ?
        query = selected_table.query_class(
            selected_table.backend,
            select_sql,
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
        selected_table.load_current_connection()
        self.before_action(selected_table)

        # base_return_fields = ['rowid', '*']
        filters = selected_table.backend.build_filters(
            selected_table.backend.decompose_filters(**kwargs)
        )

        # Functions SQL: select rowid, *, lower(url) from table
        # select_sql = selected_table.backend.SELECT.format_map({
        #     'fields': selected_table.backend.comma_join(base_return_fields),
        #     'table': selected_table.name
        # })
        # sql = [select_sql]
        select_sql = self._get_select_sql(selected_table)

        # FIXME: Use operator_join
        # Filters SQL: select rowid, * from table where url='http://'
        # joined_statements = ' and '.join(filters)
        joined_statements = selected_table.backend.operator_join(filters)
        where_clause = selected_table.backend.WHERE_CLAUSE.format_map({
            'params': joined_statements
        })
        select_sql.extend([where_clause])

        query = selected_table.query_class(
            selected_table.backend,
            select_sql,
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

        >>> database.objects.annotate(year=ExtractYear('created_on'))

        We can also run cases:

        >>> condition = When('firstname=Kendall', 'Kylie')
        ... case = Case(condition, default='Custom name', output_field=CharField())
        ... instance.objects.annotate('celebrities', alt_name=case)
        """
        selected_table = self.table_map[table]
        selected_table.load_current_connection()
        self.before_action(selected_table)

        alias_fields = list(kwargs.keys())
        base_return_fields = ['rowid', '*']
        # sql_functions_dict, special_function_fields, fields = selected_table.backend.build_annotation(
        #     **kwargs
        # )
        # base_return_fields.extend(fields)

        annotation_map = selected_table.backend.build_annotation(**kwargs)
        annotation_sql = selected_table.backend.comma_join(annotation_map.joined_final_sql_fields)
        base_return_fields.append(annotation_sql)

        # base_return_fields.extend(annotation_map.joined_final_sql_fields)

        selected_table.field_names = selected_table.field_names + alias_fields
        # sql = selected_table.backend.SELECT.format_map({
        #     'fields': selected_table.backend.comma_join(base_return_fields),
        #     'table': selected_table.name
        # })
        select_sql_tokens = self._get_select_sql(
            selected_table,
            columns=base_return_fields
        )

        # TODO: Adapt this section so that the Case function
        # can be parsed and created
        if annotation_map.requires_grouping:
            grouping_fields = set(annotation_map.field_names)
            groupby_sql = selected_table.backend.GROUP_BY.format_map({
                'conditions': selected_table.backend.comma_join(grouping_fields)
            })
            select_sql_tokens.append(groupby_sql)

        # select_sql_tokens = [
        #     selected_table.backend.simple_join(select_sql_tokens)
        # ]

        # TODO: Create a query and only run it when
        # we need with QuerySet for the other functions
        query = Query(
            selected_table.backend,
            select_sql_tokens,
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
        selected_table.load_current_connection()
        self.before_action(selected_table)

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

    # def bulk_create(self, *objs):
    # def order_by(self, *fields):

    # async def async_all(self, table):
    #     return await sync_to_async(self.all)(table)


class Database:
    """This class links and unifies independent
    tables together for a unique database and allows 
    its management via a migration file.

    Creating a new database can be done by doing the following steps:

    >>> table = Table('my_table', fields=[Field('url')])
    ... database = Database(table, name='my_database')

    Providing a name is optional. If present, an sqlite dabase is created
    in the local project's path otherwise it is created in memory and
    therefore deleted once the project is teared down.

    Migrating the database is an optional step that helps tracking
    the tables and in the fields that were created in a `migrations.json`
    local file:

    >>> database.make_migrations()
    ... database.migrate()

    Once the database is created, we can then run various operations
    on the tables that it contains:

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

    def __init__(self, *tables, name=None):
        self.database_name = name
        self.migrations = self.migrations_class(database_name=name)

        # Create a connection to populate the
        # connection pool for the rest of the
        # operations
        self.backend_class(database_name=name)

        self.table_map = {}
        for table in tables:
            if not isinstance(table, Table):
                raise ValueError('Value should be an instance of Table')

            table.load_current_connection()
            self.table_map[table.name] = table

        self.table_instances = list(tables)
        databases.register(self)

    def __repr__(self):
        tables = list(self.table_map.values())
        return f'<{self.__class__.__name__} {tables}>'

    def __getitem__(self, table_name):
        return self.table_map[table_name]

    # TODO: This allows us to use for ex database.celebrities_table
    # directly on the Database instance
    # def __getattribute__(self, name):
    #     if name.endswith('_table'):
    #         lhv, _ = name.split('_table')
    #         return self.table_map[lhv]
    #     return super().__getattribute__(name)

    def __hash__(self):
        return hash((self.database_name))
    
    @property
    def in_memory(self):
        """If the database does not have a
        concrete name then it is `memory`"""
        return self.database_name is None

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

    def foreign_key(self, left_table, right_table, on_delete, related_name=None):
        pass

    def many_to_many(self, left_table, right_table, primary_key=True, related_name=None):
        pass

    def one_to_one_key(self, left_table, right_table, on_delete, related_name=None):
        pass
