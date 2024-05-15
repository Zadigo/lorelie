import datetime
import inspect
import re
from functools import partial

import pytz

from lorelie.aggregation import Avg, Count
from lorelie.database.nodes import OrderByNode, SelectNode, WhereNode
from lorelie.exceptions import MigrationsExistsError, TableExistsError
from lorelie.expressions import OrderBy
from lorelie.fields.base import Value
from lorelie.queries import Query, QuerySet


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

    def _get_select_sql(self, selected_table, columns=['rowid', '*'], distinct=False):
        # This function creates and returns the base SQL line for
        # selecting values in the database: "select rowid, * where rowid=1"
        select_sql = selected_table.backend.SELECT.format_map({
            'fields': selected_table.backend.comma_join(columns),
            'table': selected_table.name,
        })
        if distinct:
            select_sql = re.sub(r'^select', 'select distinct', select_sql)
        return [select_sql]

    def _get_first_or_last_sql(self, selected_table, first=True):
        """Returns the general SQL that returns the first
        or last value from the database"""
        pass

    def before_action(self, table_name):
        try:
            table = self.table_map[table_name]
        except KeyError:
            if not self.database.migrations.migrated:
                raise MigrationsExistsError()
            raise TableExistsError(table_name)
        else:
            table.backend.set_current_table(table)
            table.load_current_connection()
            return table

    def first(self, table):
        """Returns the first row from
        a database table"""
        selected_table = self.before_action(table)

        select_node = SelectNode(selected_table)
        orderby_node = OrderByNode(selected_table, 'id')

        query = self.database.query_class(table=selected_table)
        query.add_sql_nodes([select_node, orderby_node])
        queryset = QuerySet(query)
        return queryset[-0]

    def last(self, table):
        """Returns the last row from
        a database table"""
        selected_table = self.before_action(table)

        select_node = SelectNode(selected_table, limit=1)
        orderby_node = OrderByNode(selected_table, '-id')

        query = self.database.query_class(table=selected_table)
        query.add_sql_nodes([select_node, orderby_node])
        queryset = QuerySet(query)
        return queryset[-0]

    def all(self, table):
        selected_table = self.before_action(table)
        select_node = SelectNode(selected_table)

        query = selected_table.query_class(table=selected_table)

        if selected_table.ordering:
            orderby_node = OrderByNode(
                selected_table, *selected_table.ordering)
            query.add_sql_node(orderby_node)

        query.add_sql_node(select_node)
        return QuerySet(query)

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

        query = self.database.query_class(table=selected_table)
        query.add_sql_nodes([sql])
        query.run(commit=True)
        return self.last(table)

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

        select_node = SelectNode(selected_table)
        where_node = WhereNode(*args, **kwargs)

        query = selected_table.query_class(table=selected_table)
        query.add_sql_nodes([select_node, where_node])
        return QuerySet(query)

    def get(self, table, *args, **kwargs):
        """Returns a specific row from the database
        based on a set of criteria

        >>> instance.objects.get('celebrities', id__eq=1)
        ... instance.objects.get('celebrities', id=1)
        """
        selected_table = self.before_action(table)

        # filters = selected_table.backend.build_filters(
        #     selected_table.backend.decompose_filters(**kwargs)
        # )

        # select_sql = self._get_select_sql(selected_table)
        # joined_statements = selected_table.backend.operator_join(filters)
        # where_clause = selected_table.backend.WHERE_CLAUSE.format_map({
        #     'params': joined_statements
        # })
        # select_sql.extend([where_clause])

        # query = self.database.query_class(
        #     select_sql,
        #     table=selected_table
        # )
        # query.run()

        # if not query.result_cache:
        #     return None

        select_node = SelectNode(selected_table)
        where_node = WhereNode(*args, **kwargs)

        query = selected_table.query_class(table=selected_table)
        query.add_sql_nodes([select_node, where_node])
        queryset = QuerySet(query)

        if len(queryset) > 1:
            raise ValueError("Get returnd more than one value")
        return queryset[-0]

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
        annotated_sql_fields = selected_table.backend.comma_join(
            annotation_map.joined_final_sql_fields
        )
        base_return_fields.append(annotated_sql_fields)

        select_node = SelectNode(selected_table, *base_return_fields)

        query = self.database.query_class(table=selected_table)
        query.add_sql_nodes([select_node])

        # if annotation_map.requires_grouping:
        #     grouping_fields = set(annotation_map.field_names)
        #     groupby_sql = selected_table.backend.GROUP_BY.format_map({
        #         'conditions': selected_table.backend.comma_join(grouping_fields)
        #     })
        #     query.select_map.group_by = groupby_sql

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
        select_node = SelectNode(selected_table, *columns)
        query = self.database.query_class(table=selected_table)
        query.add_sql_node(select_node)

        queryset = QuerySet(query)

        def dictionnaries():
            for row in queryset:
                yield row._cached_data

        return list(dictionnaries())

        # select_sql = self._get_select_sql(selected_table, columns=columns)
        # query = self.database.query_class(select_sql, table=selected_table)

        # # TODO: Improve this section
        # def dict_iterator(values):
        #     for row in values:
        #         yield row._cached_data

        # query.run()
        # return list(dict_iterator(query.result_cache))

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

    # def foreign_table(self, relationship):
    #     result = relationship.split('__')
    #     if len(result) != 2:
    #         raise ValueError(
    #             'Foreign table should contain the left table and right table e.g. left__right')
    #     left_table, right_table = result
    #     return ForeignTablesManager(left_table, right_table, self)

    def distinct(self, table, *columns):
        """Returns items from the database which are
        distinct

        >>> db.objects.distinct('celebrities', 'firstname')
        """
        selected_table = self.before_action(table)
        select_sql = self._get_select_sql(
            selected_table,
            columns=columns,
            distinct=True
        )
        query = self.database.query_class(select_sql, table=selected_table)
        return QuerySet(query)

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


class ForeignTablesManager:
    def __init__(self, left_table, right_table, manager, reversed=False):
        self.manager = manager
        self.reversed = reversed
        self.left_table = manager.database.get_table(left_table)
        self.right_table = manager.database.get_table(right_table)
        relationships = getattr(manager.database, 'relationships', None)
        self.lookup_name = f'{left_table}_{right_table}'
        self.relationship = relationships[self.lookup_name]

    def __getattr__(self, name):
        methods = {}
        for name, value in self.manager.__dict__:
            if value.startswith('__'):
                continue

            if inspect.ismethod(value):
                methods[name] = value
        try:
            method = methods[name]
        except KeyError:
            raise AttributeError('Method does not exist')
        else:
            return partial(method, table=self.right_table.name)
