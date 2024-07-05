import collections
import dataclasses
import datetime
from dataclasses import is_dataclass

import pytz
from asgiref.sync import sync_to_async

from lorelie.database import registry
from lorelie.database.functions.aggregation import (Avg,
                                                    CoefficientOfVariation,
                                                    Count, Max,
                                                    MeanAbsoluteDifference,
                                                    Min, StDev, Sum, Variance)
from lorelie.database.nodes import (InsertNode, OrderByNode, SelectNode,
                                    UpdateNode, WhereNode)
from lorelie.exceptions import (FieldExistsError, MigrationsExistsError,
                                TableExistsError)
from lorelie.queries import EmptyQuerySet, Query, QuerySet, ValuesIterable


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
        self._test_current_table_on_manager = None

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
        pass

    def _get_first_or_last_sql(self, selected_table, first=True):
        """Returns the general SQL that returns the first
        or last value from the database"""
        pass

    def _validate_auto_fields(self, table, params, update_only=False):
        # There might be cases where the
        # user does not pass any values
        # in the create fields but that
        # there are auto_add and auto_update
        # fields in the database. We have to
        # send the current dates for these
        d = datetime.datetime.now()
        if not update_only:
            for field in table.auto_add_fields:
                if field in params:
                    continue
                params[field] = str(d)

        for field in table.auto_update_fields:
            if field in params:
                continue
            params[field] = str(d)
        return params

    def pre_save(self, selected_table, fields, values):
        """Pre-save stores the pre-processed data
        into a namedtuple that is then sent to the
        `clean` method on the table which then allows
        the user to modify the data before sending it
        to the database"""
        named = collections.namedtuple(selected_table.name, fields)
        data_dict = {}
        for i, field in enumerate(fields):
            if field == 'id' or field == 'rowid':
                continue
            data_dict[field] = values[i]
        return named(**data_dict)

    def before_action(self, table_name):
        try:
            table = self.table_map[table_name]
        except KeyError:
            if not self.database.is_ready:
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
        return QuerySet(query)[0]

    def last(self, table):
        """Returns the last row from
        a database table"""
        selected_table = self.before_action(table)

        select_node = SelectNode(selected_table)
        orderby_node = OrderByNode(selected_table, '-id')

        query = self.database.query_class(table=selected_table)
        query.add_sql_nodes([select_node, orderby_node])
        return QuerySet(query)[0]

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
        """The create function facilitates the creation 
        of a new row in the specified table within the 
        current database

        >>> db.objects.create('celebrities', firstname='Kendall')
        """
        selected_table = self.before_action(table)
        kwargs = self._validate_auto_fields(selected_table, kwargs)
        values, kwargs = selected_table.validate_values_from_dict(kwargs)
        query = self.database.query_class(table=selected_table)

        insert_node = InsertNode(
            selected_table,
            insert_values=kwargs,
            returning=selected_table.field_names
        )

        query.add_sql_node(insert_node)
        return QuerySet(query)[0]

        # fields, values = selected_table.backend.dict_to_sql(
        #     kwargs,
        #     quote_values=False
        # )
        # values, _ = selected_table.validate_values(fields, values)

        # # pre_saved_values = self.pre_save(selected_table, fields, values)
        # joined_fields = selected_table.backend.comma_join(fields)
        # joined_values = selected_table.backend.comma_join(values)

        # query = self.database.query_class(table=selected_table)
        # # insert_node = InsertNode(selected_table, insert_values=kwargs)
        # insert_sql = selected_table.backend.INSERT.format(
        #     table=selected_table.name,
        #     fields=joined_fields,
        #     values=joined_values
        # )

        # # See: https://www.sqlitetutorial.net/sqlite-returning/
        # return_fields = selected_table.backend.comma_join(
        #     selected_table.field_names
        # )
        # query.add_sql_nodes([insert_sql, f'returning {return_fields}'])
        # # query.run(commit=True)
        # # TODO: This raises a sqlite3.OperationalError: cannot
        # # commit transaction - SQL statements in progress
        # queryset = QuerySet(query)
        # queryset.use_commit = True
        # return list(queryset)[-0]

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

        if selected_table.ordering:
            orderby_node = OrderByNode(
                selected_table,
                *selected_table.ordering
            )
            query.add_sql_node(orderby_node)

        return QuerySet(query)

    def get(self, table, *args, **kwargs):
        """Returns a specific row from the database
        based on a set of criteria

        >>> instance.objects.get('celebrities', id__eq=1)
        ... instance.objects.get('celebrities', id=1)
        """
        selected_table = self.before_action(table)

        select_node = SelectNode(selected_table)
        where_node = WhereNode(*args, **kwargs)

        query = selected_table.query_class(table=selected_table)
        query.add_sql_nodes([select_node, where_node])
        queryset = QuerySet(query)

        if len(queryset) > 1:
            raise ValueError(
                "Get returnd more than one value. "
                f"It returned {len(queryset)} items"
            )

        if not queryset:
            return None

        return list(queryset)[-0]

    def annotate(self, table, *args, **kwargs):
        """method allows the usage of advanced functions or expressions in a query to 
        add additional fields to your querysets based on the values of existing fields

        Returning each values of the name in lower or uppercase:

        >>> db.objects.annotate('celebrities', lowered_name=Lower('name'))
        ... db.objects.annotate('celebrities', uppered_name=Upper('name'))

        Returning only the year for a given column:

        >>> database.objects.annotate(year=ExtractYear('created_on'))

        We can also run cases. For example, when a price is equals to 1,
        then create temporary column named custom price with either 2 or 3:

        >>> condition = When('price=1', 2)
        ... case = Case(condition, default=3, output_field=CharField())
        ... db.objects.annotate('celebrities', custom_price=case)

        Suppose you have two columns `price` and `tax` we can return a new
        column with `price + tax`:

        >>> db.objects.annotate('products', new_price=F('price') + F('tax'))

        You can also add a constant value to a column:

        >>> db.objects.annotate('products', new_price=F('price') + 10)

        The `Value` expression can be used to return a specific value in a column:        

        >>> db.objects.annotate('products', new_price=Value(1))

        Finally, `Q` objects are used to encapsulate a collection 
        of keyword arguments and can be used to evaluate conditions. 
        For instance, to annotate a result indicating whether the price 
        is greater than 1:

        >>> db.objects.annotate('products', result=Q(price__gt=1))

        Using expressions without an alias field name will raise an error.

        Aggregate functions can also be used in annotations, but they will return 
        the result for each element grouped by a specified field. For example, to 
        count the number of occurrences of each `price`:

        >>> db.objects.annotate('products', Count('price'))

        The above will return the price count for each products. If there are
        two products with a price of 1 we will then get `[{'price': 1, 'count_price': 2}]`
        """
        selected_table = self.before_action(table)

        for func in args:
            internal_type = getattr(func, 'internal_type', None)
            if internal_type is None:
                raise ValueError(
                    f"{func} should be an instance of Functions, "
                    "BaseExpression or CombinedExpression"
                )

            if internal_type == 'expression':
                raise ValueError(
                    f'"{func}" requires an alias field name '
                    'in order to be used with annotate'
                )

            if internal_type == 'function':
                kwargs.update({func.alias_field_name: func})

        if not kwargs:
            return self.all(table)

        alias_fields = list(kwargs.keys())

        for alias, func in kwargs.items():
            internal_type = getattr(func, 'internal_type')
            if internal_type == 'expression':
                func.alias_field_name = alias

        annotation_map = selected_table.backend.build_annotation(kwargs)
        annotated_sql_fields = selected_table.backend.comma_join(
            annotation_map.joined_final_sql_fields
        )

        return_fields = ['*', annotated_sql_fields]
        select_node = SelectNode(selected_table, *return_fields)

        query = self.database.query_class(table=selected_table)
        query.alias_fields = list(alias_fields)
        query.add_sql_node(select_node)

        if annotation_map.requires_grouping:
            groupby_sql = selected_table.backend.GROUP_BY.format_map({
                'conditions': 'id'
            })
            query.select_map.groupby = groupby_sql

        if selected_table.ordering:
            orderby_node = OrderByNode(
                selected_table, *selected_table.ordering)
            query.add_sql_node(orderby_node)
        return QuerySet(query)

        # for func in args:
        #     if not isinstance(func, (Functions, BaseExpression)):
        #         raise ValueError(
        #             'Func should be an instnae of Functions or BaseExpression')

        #     if isinstance(func, CombinedExpression):
        #         raise ValueError('CombinedExpressions require an alias name')

        #     kwargs.update({func.alias_field_name: func})

        # if not kwargs:
        #     return self.all(table)

        # alias_fields = list(kwargs.keys())

        # for field in alias_fields:
        #     # Combined expressions alias field names
        #     # are added afterwards once the user sets
        #     # the name for the expression
        #     if isinstance(kwargs[field], CombinedExpression):
        #         kwargs[field].alias_field_name = field

        # annotation_map = selected_table.backend.build_annotation(**kwargs)
        # annotated_sql_fields = selected_table.backend.comma_join(
        #     annotation_map.joined_final_sql_fields
        # )
        # return_fields = ['*', annotated_sql_fields]

        # select_node = SelectNode(selected_table, *return_fields)

        # query = self.database.query_class(table=selected_table)
        # query.add_sql_nodes([select_node])

        # if annotation_map.requires_grouping:
        #     # grouping_fields = set(annotation_map.field_names)
        #     # groupby_sql = selected_table.backend.GROUP_BY.format_map({
        #     #     'conditions': selected_table.backend.comma_join(grouping_fields)
        #     # })
        #     groupby_sql = selected_table.backend.GROUP_BY.format_map({
        #         'conditions': 'id'
        #     })
        #     query.select_map.groupby = groupby_sql

        # query.alias_fields = list(alias_fields)
        # return QuerySet(query)

    def values(self, table, *fields):
        """Returns data from the database as a list
        of dictionnary values

        >>> instance.objects.as_values('celebrities', 'id')
        ... [{'id': 1}]
        """
        selected_table = self.before_action(table)

        # columns = list(fields) or ['rowid', '*']
        columns = list(fields)
        select_node = SelectNode(selected_table, *columns)
        query = self.database.query_class(table=selected_table)
        query.add_sql_node(select_node)

        if selected_table.ordering:
            orderby_node = OrderByNode(
                selected_table, *selected_table.ordering)
            query.add_sql_node(orderby_node)

        queryset = QuerySet(query)

        # def dictionnaries():
        #     for row in queryset:
        #         yield row._cached_data

        # return list(dictionnaries())
        return list(ValuesIterable(queryset, fields=columns))

    def dataframe(self, table, *fields):
        """This method returns data from the database as a pandas 
        DataFrame object. This allows for easy manipulation and 
        analysis of the data using pandas' powerful data handling 
        capabilities

        >>> instance.objects.as_dataframe('celebrities', 'id')
        ... pandas.DataFrame
        """
        import pandas
        return pandas.DataFrame(self.values(table, *fields))

    def order_by(self, table, *fields):
        """Returns data ordered by the fields specified
        by the user. It can be sorted in ascending order:

        >>> instance.objects.order_by('celebrities', 'firstname')

        Or, descending order:

        >>> instance.objects.order_by('celebrities', '-firstname')
        """
        selected_table = self.before_action(table)

        select_node = SelectNode(selected_table)
        order_by_node = OrderByNode(selected_table, *fields)

        query = selected_table.query_class(table=selected_table)
        query.add_sql_nodes([select_node, order_by_node])
        return QuerySet(query)

    def aggregate(self, table, *args, **kwargs):
        """Returns data ordered by the fields specified by the user. 
        You can specify the sorting order by providing the field names. 
        Prefixing a field with a hyphen (-) sorts the data in descending order, 
        while providing the field name without a prefix sorts the data 
        in ascending order

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
            if not isinstance(function, (Count, Avg, Sum, MeanAbsoluteDifference, CoefficientOfVariation, Variance, StDev, Max, Min)):
                none_aggregate_functions.count(function)
                continue
            kwargs[function.aggregate_name] = function

        if none_aggregate_functions:
            raise ValueError(
                "Aggregate requires aggregate functions"
            )

        aggregate_sqls = []
        annotation_map = selected_table.backend.build_annotation(**kwargs)
        aggregate_sqls.extend(annotation_map.joined_final_sql_fields)

        select_node = SelectNode(selected_table, *aggregate_sqls)

        query = selected_table.query_class(table=selected_table)
        query.add_sql_node(select_node)
        query.alias_fields = annotation_map.alias_fields
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
        select_node = SelectNode(selected_table, *columns, distinct=True)
        query = selected_table.query_class(table=selected_table)
        query.add_sql_node(select_node)
        if selected_table.ordering:
            ordering_node = OrderByNode(
                selected_table, *selected_table.ordering)
            query.add_sql_node(ordering_node)
        return QuerySet(query)

    def bulk_create(self, table, objs):
        """Creates multiple objects in the database at once
        using a list of datasets or dictionnaries

        >>> @dataclasses.dataclass
        ... class Celebrity:
        ...     name: str

        >>> db.objects.bulk_create('celebrities', [Celebrity('Taylor Swift')])
        ... [<Celebrity: 1>]
        """
        selected_table = self.before_action(table)

        invalid_objects_counter = 0
        for obj in objs:
            if not is_dataclass(obj):
                invalid_objects_counter = invalid_objects_counter + 1
                continue

        if invalid_objects_counter > 0:
            raise ValueError(
                "Objects used in bulk create should be an "
                "instance of dataclass"
            )

        for obj in objs:
            fields = dataclasses.fields(obj)
            for field in fields:
                if not selected_table.has_field(field.name):
                    raise FieldExistsError(field, selected_table)

        columns_to_use = set()
        values_to_create = []

        for obj in objs:
            dataclass_values = []
            dataclass_fields = dataclasses.fields(obj)

            dataclass_data = {}
            for dataclass_field in dataclass_fields:
                columns_to_use.add(dataclass_field.name)

                value = getattr(obj, dataclass_field.name)
                dataclass_data[dataclass_field.name] = value

            dataclass_values.append(dataclass_data)
            values_to_create.extend(dataclass_values)

        # TODO: We have to call validate values

        insert_node = InsertNode(
            selected_table,
            batch_values=values_to_create
        )

        query = selected_table.query_class(table=selected_table)
        query.add_sql_node(insert_node)

        columns_to_use.add('id')
        query.add_sql_node(
            f'returning {selected_table.backend.comma_join(columns_to_use)}')

        query.run(commit=True)
        return QuerySet(query)

    def dates(self, table, field, field_to_sort='year', ascending=True):
        values = self.datetimes(
            table,
            field,
            field_to_sort=field_to_sort,
            ascending=ascending
        )
        return list(map(lambda x: x.date(), values))

    def datetimes(self, table, field, field_to_sort='year', ascending=True):
        selected_table = self.before_action(table)

        select_node = SelectNode(selected_table, field)
        query = selected_table.query_class(table=selected_table)
        query.add_sql_node(select_node)
        query.run()

        def date_iterator(row):
            d = datetime.datetime.strptime(
                row[field], '%Y-%m-%d %H:%M:%S.%f%z')
            return d

        dates = map(date_iterator, query.result_cache)
        return list(dates)

    # def difference()
    # def earliest()
    # def latest()

    def exclude(self, table, *args, **kwargs):
        """Selects all the values from the database
        that match the filters

        >>> db.objects.exclude(firstname='Kendall')"""
        selected_table = self.before_action(table)

        select_node = SelectNode(selected_table)
        where_node = ~WhereNode(*args, **kwargs)

        query = selected_table.query_class(table=selected_table)
        query.add_sql_nodes([select_node, where_node])

        if selected_table.ordering:
            ordering_node = OrderByNode(
                selected_table, *selected_table.ordering)
            query.add_sql_node(ordering_node)
        return QuerySet(query)

    # def extra()
    # def only()

    def get_or_create(self, table, create_defaults={}, **kwargs):
        """Tries to get a row in the database using the coditions
        passed in kwargs. It then uses the `defaults`
        parameter to create the values that do not exist.

        If `defaults` is not specified, the values passed in kwargs
        will become the default `defaults`.

        >>> defaults = {'age': 24}
        ... db.objects.get_or_create('celebrities', create_defaults=defaults, firstname='Margot')

        If the queryset returns multiple elements, an error is raised.
        """
        selected_table = self.before_action(table)

        select_node = SelectNode(selected_table)
        where_node = WhereNode(**kwargs)
        sql = [select_node, where_node]

        query = selected_table.query_class(table=selected_table)
        query.add_sql_nodes(sql)
        queryset = QuerySet(query)

        if queryset.exists():
            if len(queryset) > 1:
                raise ValueError('Returned more than one values')
            return queryset[-0]
        else:
            create_defaults.update(kwargs)
            create_defaults = self._validate_auto_fields(
                selected_table,
                create_defaults
            )
            _, create_defaults = selected_table.validate_values_from_dict(
                create_defaults
            )

            insert_node = InsertNode(
                selected_table,
                insert_values=create_defaults,
                returning=selected_table.field_names
            )
            new_query = query.create(table=selected_table)
            new_query.add_sql_node(insert_node)
            return QuerySet(new_query)[0]
            # new_query.run(commit=True)
            # return new_query.result_cache[0]

    # def select_for_update()
    # def select_related()
    # def fetch_related()

    def update_or_create(self, table, create_defaults={}, **kwargs):
        """Updates a row in the database selected on the
        filters determined by kwargs. It then uses the `create_defaults`
        parameter to create the values that do not exist.

        If `create_defaults` is not specified, the values passed in kwargs
        will become the default `create_defaults`.

        >>> create_defaults = {'age': 24}
        ... db.objects.update_or_create('celebrities', create_defaults=create_defaults, firstname='Margot')

        If the queryset returns multiple elements (from the get conditions specified
        via kwargs), an error is raised.
        """
        selected_table = self.before_action(table)

        select_node = SelectNode(selected_table)
        query = selected_table.query_class(table=selected_table)
        query.add_sql_node(select_node)

        if kwargs:
            # The kwargs allows us to get one item
            # in the database that we can update.
            # If no kwargs are provided then we assume
            # all products want to be updated at once
            # which will force us to raise an error
            # (ValueError) below
            where_node = WhereNode(**kwargs)
            query.add_sql_node(where_node)
        else:
            raise ValueError(
                "You need to define parameters "
                "to search and update a specific product "
                "in the database"
            )

        # TODO: There's a x2 select called
        # on the database
        queryset = QuerySet(query)

        if not create_defaults:
            # We do not care if the user passes
            # Q functions in the kwargs since we
            # should not be able to use these
            # in the get_or_create or update_or_create.
            # We'll just let the error raise itself.
            create_defaults.update(**kwargs)

        _, create_defaults = selected_table.validate_values_from_dict(
            create_defaults
        )
        ids = list(map(lambda x: x['id'], queryset))

        if len(ids) > 1:
            # TODO: Check for cases where kwargs is not provided
            # but there's only one element in the database
            raise ValueError('Get returned more than one value')

        returning_fields = selected_table.field_names

        if queryset.exists():
            create_defaults = self._validate_auto_fields(
                selected_table,
                create_defaults,
                update_only=True
            )
            # TODO: Add a returning like
            # the InsertNode
            update_node = UpdateNode(
                selected_table,
                create_defaults,
                id__in=ids
            )

            new_query = query.create(table=selected_table)
            new_query.add_sql_node(update_node)

            returning_fields = selected_table.backend.comma_join(
                returning_fields
            )
            new_query.add_sql_node(f'returning {returning_fields}')
        else:
            create_defaults = self._validate_auto_fields(
                selected_table,
                create_defaults,
            )
            insert_node = InsertNode(
                selected_table,
                insert_values=create_defaults,
                returning=returning_fields
            )
            new_query = query.create(table=selected_table)
            new_query.add_sql_node(insert_node)
            # new_query.run(commit=True)

        # We have to execute the query before
        # hand. The reason for this is the insert
        # and update nodes need to be comitted
        # immediately otherwise the QuerySet would
        # delay their evaaluation which would not
        # then modify the data in the database
        return QuerySet(new_query)[0]

    async def afirst(self, table):
        return await sync_to_async(self.first)(table)

    async def alast(self, table):
        return await sync_to_async(self.last)(table)

    async def aall(self, table):
        return await sync_to_async(self.all)(table)

    async def acreate(self, table, **kwargs):
        return await sync_to_async(self.create)(table, **kwargs)


class ForeignTablesManager:
    """This is the main manager used to access/reverse
    access tables linked by a relationship"""

    def __init__(self, relationship_map, reverse=False):
        self.reverse = reverse
        self.relationship_map = relationship_map
        self.left_table = relationship_map.left_table
        self.right_table = relationship_map.right_table

        if not self.right_table.is_foreign_key_table:
            raise ValueError(
                "Trying to access a table which has no "
                "foreign key relationship with the related "
                f"table: {self.left_table} -> {self.right_table}"
            )
        self.current_row = None

    def __repr__(self):
        direction = '->'
        if self.reverse:
            direction = '<-'
        return f'<{self.__class__.__name__} [from {direction} to]>'

    # def __getattr__(self, name):
    #     methods = {}
    #     for name, value in self.manager.__dict__:
    #         if value.startswith('__'):
    #             continue

    #         if inspect.ismethod(value):
    #             methods[name] = value
    #     try:
    #         method = methods[name]
    #     except KeyError:
    #         raise AttributeError('Method does not exist')
    #     else:
    #         return partial(method, table=self.right_table.name)

    def all(self):
        select_node = SelectNode(self.right_table)
        query = Query(table=self.right_table)
        query.add_sql_node(select_node)
        return QuerySet(query)

    def last(self):
        select_node = SelectNode(self.right_table)
        orderby_node = OrderByNode(self.right_table, '-id')

        query = self.right_table.database.query_class(table=self.right_table)
        query.add_sql_nodes([select_node, orderby_node])
        queryset = QuerySet(query)
        return queryset[-0]

    def create(self, **kwargs):
        fields, values = self.right_table.backend.dict_to_sql(
            kwargs,
            quote_values=False
        )
        values = self.right_table.validate_values(fields, values)

        # pre_saved_values = self.pre_save(self.right_table, fields, values)

        # TODO: Create functions for datetimes and timezones
        current_date = datetime.datetime.now(tz=pytz.UTC)
        if self.right_table.auto_add_fields:
            for field in self.right_table.auto_add_fields:
                fields.append(field)
                date = self.right_table.backend.quote_value(str(current_date))
                values.append(date)

        fields.insert(0, self.relationship.backward_related_field)
        values.insert(0, self.current_row.id)

        joined_fields = self.right_table.backend.comma_join(fields)
        joined_values = self.right_table.backend.comma_join(values)

        query = self.right_table.database.query_class(table=self.right_table)

        insert_sql = self.right_table.backend.INSERT.format(
            table=self.right_table.name,
            fields=joined_fields,
            values=joined_values
        )

        query.add_sql_nodes([insert_sql])
        query.run(commit=True)
        return self.last()
