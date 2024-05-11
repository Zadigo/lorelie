import dataclasses
import datetime
import pathlib
from collections import defaultdict
from functools import partial
from typing import OrderedDict, Union

import pytz
from asgiref.sync import sync_to_async

from lorelie.fields.relationships import ForeignKeyField
from lorelie.aggregation import Avg, Count
from lorelie.backends import SQLiteBackend
from lorelie.exceptions import TableExistsError
from lorelie.expressions import OrderBy
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
        # Tells if the manager was
        # created via as_manager
        self.auto_created = True

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.database}>'

    def __get__(self, instance, cls=None):
        if not self.table_map:
            self.table_map = instance.table_map
            self.database = instance
        return self

    @classmethod
    def as_manager(cls, table_map={}, database=None):
        instance = cls()
        instance.table_map = table_map
        instance.database = database
        instance.auto_created = False
        return instance

    def _get_select_sql(self, selected_table, columns=['rowid', '*']):
        # This function creates and returns the base SQL line for
        # selecting values in the database: "select rowid, * where rowid=1"
        select_sql = selected_table.backend.SELECT.format_map({
            'fields': selected_table.backend.comma_join(columns),
            'table': selected_table.name,
        })
        return [select_sql]

    def _get_first_or_last_sql(self, selected_table, first=True):
        """Returns the general SQL that returns the first
        or last value from the database"""
        select_sql = self._get_select_sql(selected_table)

        if first:
            ordering_column = ['id']
        else:
            ordering_column = ['-id']

        ordering = OrderBy(ordering_column)
        order_by_sql = ordering.as_sql(selected_table.backend)
        select_sql.extend(order_by_sql)

        limit_sql = selected_table.backend.LIMIT.format(value=1)
        select_sql.extend([limit_sql])
        return select_sql

    def before_action(self, table_name):
        try:
            table = self.table_map[table_name]
        except KeyError:
            raise TableExistsError(table_name)
        else:
            table.backend.set_current_table(table)
            table.load_current_connection()
            return table

    def first(self, table):
        """Returns the first row from
        a database table"""
        selected_table = self.before_action(table)
        select_sql = self._get_first_or_last_sql(selected_table)
        query = self.database.query_class(select_sql, table=selected_table)
        query.run()
        return query.result_cache[0]

    def last(self, table):
        """Returns the last row from
        a database table"""
        selected_table = self.before_action(table)
        select_sql = self._get_first_or_last_sql(selected_table, first=False)
        query = self.database.query_class(select_sql, table=selected_table)
        query.run()
        return query.result_cache[0]

    def all(self, table):
        selected_table = self.before_action(table)
        select_sql = self._get_select_sql(selected_table)

        if bool(selected_table.ordering):
            ordering_sql = selected_table.ordering.as_sql(
                selected_table.backend
            )
            select_sql.extend(ordering_sql)

        query = self.database.query_class(select_sql, table=selected_table)
        return QuerySet(query)
        # query.run()
        # return query.result_cache

    def create(self, table, **kwargs):
        """Creates a new row in the table of 
        the current database

        >>> db.objects.create('celebrities', firstname='Kendall')
        """
        selected_table = self.before_action(table)

        fields, values = selected_table.backend.dict_to_sql(
            kwargs,
            quote_values=False
        )
        values = selected_table.validate_values(fields, values)

        # TODO: Create functions for datetimes and timezones
        current_date = datetime.datetime.now(tz=pytz.UTC)
        if selected_table.auto_add_fields:
            for field in selected_table.auto_add_fields:
                fields.append(field)
                date = selected_table.backend.quote_value(str(current_date))
                values.append(date)

        joined_fields = selected_table.backend.comma_join(fields)
        joined_values = selected_table.backend.comma_join(values)
        sql = selected_table.backend.INSERT.format(
            table=selected_table.name,
            fields=joined_fields,
            values=joined_values
        )

        query = self.database.query_class([sql], table=selected_table)
        query.run(commit=True)
        return self.last(selected_table.name)

    def filter(self, table, *args, **kwargs):
        """Filter the data in the database based on
        a set of criteria using filter keyword arguments

        >>> db.objects.filter('celebrities', firstname='Kendall')
        ... db.objects.filter('celebrities', age__gt=20)
        ... db.objects.filter('celebrities', firstname__in=['Kendall'])

        Filtering can also be done using more complexe logic via database
        functions such as the `Q` function:

        >>> db.objects.filter('celebrities', Q(firstname='Kendall') | Q(firstname='Kylie'))
        ... db.objects.filter('celebrities', Q(firstname='Margot') | Q(firstname='Kendall') & Q(followers__gte=1000))
        """
        selected_table = self.before_action(table)

        tokens = selected_table.backend.decompose_filters(**kwargs)
        filters = selected_table.backend.build_filters(tokens)

        if args:
            for expression in args:
                filters.extend(expression.as_sql(selected_table.backend))

        if len(filters) > 1:
            filters = [
                selected_table.backend.wrap_parenthentis(
                    ' and '.join(filters)
                )
            ]

        select_sql = self._get_select_sql(selected_table)
        where_clause = selected_table.backend.WHERE_CLAUSE.format_map({
            'params': selected_table.backend.comma_join(filters)
        })
        select_sql.append(where_clause)

        query = self.database.query_class(
            select_sql,
            table=selected_table
        )
        query.run()
        # return query.result_cache
        return QuerySet(query)

    # async def aget(self, table, **kwargs):
    #     return await sync_to_async(self.get)(table, **kwargs)

    def get(self, table, **kwargs):
        """Returns a specific row from the database
        based on a set of criteria

        >>> instance.objects.get('celebrities', id__eq=1)
        ... instance.objects.get('celebrities', id=1)
        """
        selected_table = self.before_action(table)

        filters = selected_table.backend.build_filters(
            selected_table.backend.decompose_filters(**kwargs)
        )

        select_sql = self._get_select_sql(selected_table)
        joined_statements = selected_table.backend.operator_join(filters)
        where_clause = selected_table.backend.WHERE_CLAUSE.format_map({
            'params': joined_statements
        })
        select_sql.extend([where_clause])

        query = self.database.query_class(
            select_sql,
            table=selected_table
        )
        query.run()

        if not query.result_cache:
            return None

        if len(query.result_cache) > 1:
            raise ValueError("Get returnd more than one value")
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
        selected_table = self.before_action(table)

        alias_fields = list(kwargs.keys())
        base_return_fields = ['rowid', '*']
        annotation_map = selected_table.backend.build_annotation(**kwargs)
        annotation_sql = selected_table.backend.comma_join(
            annotation_map.joined_final_sql_fields
        )
        base_return_fields.append(annotation_sql)

        select_sql = self._get_select_sql(
            selected_table,
            columns=base_return_fields
        )

        if annotation_map.requires_grouping:
            grouping_fields = set(annotation_map.field_names)
            groupby_sql = selected_table.backend.GROUP_BY.format_map({
                'conditions': selected_table.backend.comma_join(grouping_fields)
            })
            select_sql.append(groupby_sql)

        # TODO: Create a query and only run it when
        # we need with QuerySet for the other functions
        query = self.database.query_class(select_sql, table=selected_table)
        query.alias_fields = list(alias_fields)
        return QuerySet(query)

    def values(self, table, *args):
        """Returns data from the database as a list
        of dictionnary values

        >>> instance.objects.as_values('celebrities', 'id')
        ... [{'id': 1}]
        """
        selected_table = self.before_action(table)

        columns = list(args) or ['rowid', '*']
        select_sql = self._get_select_sql(selected_table, columns=columns)
        query = self.database.query_class(select_sql, table=selected_table)

        # TODO: Improve this section
        def dict_iterator(values):
            for row in values:
                yield row._cached_data

        query.run()
        return list(dict_iterator(query.result_cache))

    def dataframe(self, table, *args):
        """Returns data from the database as a
        pandas DataFrame object

        >>> instance.objects.as_dataframe('celebrities', 'id')
        ... pandas.DataFrame
        """
        import pandas
        return pandas.DataFrame(self.values(table, *args))

    def order_by(self, table, *fields):
        """Returns data ordered by the fields specified
        by the user. It can be sorted in ascending order:

        >>> instance.objects.order_by('celebrities', 'firstname')

        Or, descending order:

        >>> instance.objects.order_by('celebrities', '-firstname')
        """
        selected_table = self.before_action(table)

        ordering = OrderBy(fields)
        ordering_sql = ordering.as_sql(selected_table.backend)

        select_sql = self._get_select_sql(selected_table)
        select_sql.extend(ordering_sql)

        query = selected_table.query_class(select_sql, table=selected_table)
        query.run()
        return QuerySet(query)

    def aggregate(self, table, *args, **kwargs):
        """Returns a dictionnary of aggregate values
        calculated from the database

        >>> db.objects.aggregate('celebrities', Count('id'))
        ... {'age__count': 1}

        >>> db.objects.aggregate('celebrities', count_age=Count('id'))
        ... {'count_age': 1}
        """
        selected_table = self.before_action(table)

        functions = list(args)

        # Functions used in args will get an
        # automatic aggregate name that we
        # will implement in the kwargs
        none_aggregate_functions = []
        for function in functions:
            if not isinstance(function, (Count, Avg)):
                none_aggregate_functions.count(function)
                continue
            kwargs[function.aggregate_name] = function

        if none_aggregate_functions:
            raise ValueError("Aggregate requires aggregate functions")

        aggregate_sqls = []
        annotation_map = selected_table.backend.build_annotation(**kwargs)
        aggregate_sqls.extend(annotation_map.joined_final_sql_fields)

        select_sql = self._get_select_sql(
            selected_table,
            columns=aggregate_sqls
        )

        query = self.database.query_class(select_sql, table=selected_table)
        query.run()
        return getattr(query.result_cache[0], '_cached_data', {})

    def count(self, table):
        """Returns the number of items present
        in the database

        >>> db.objects.count('celebrities')
        """
        result = self.aggregate(table, Count('id'))
        return result.get('id__count')

    # def distinct(self, table, *columns):
    #     selected_table = self.before_action(table)
    #     select_sql
    # def bulk_create(self, *objs):
    # def dates()
    # def datetimes
    # def difference()
    # def earliest()
    # def latest()
    # def exclude()
    # def extra()
    # def only()
    # def get_or_create(self, table, defaults={}, **kwargs):
    #     selected_table = self.before_action(table)

    #     columns, values = selected_table.backend.dict_to_sql(defaults)
    #     joined_columns = selected_table.backend.comma_join(columns)
    #     joined_values = selected_table.backend.comma_join(values)

    #     replace_sql = selected_table.backend.REPLACE.format_map({
    #         'table': selected_table.name,
    #         'fields': joined_columns,
    #         'values': joined_values
    #     })
    #     print(replace_sql)
    # def select_for_update()
    # def select_related()
    # def fetch_related()
    # def update(self, table, **kwargs):
    #     """Updates multiples rows in the database at once

    #     >>> db.objects.update('celebrities', firstname='Kendall')
    #     """
    #     selected_table = self.before_action(table)

    #     update_sql = selected_table.backend.UPDTATE.format_map({
    #         table: selected_table.name
    #     })

    #     columns_to_set = []
    #     columns, values = selected_table.backend.dict_to_sql(kwargs)
    # def update_or_create()
    # def resolve_expression()

    # async def async_all(self, table):
    #     return await sync_to_async(self.all)(table)


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
