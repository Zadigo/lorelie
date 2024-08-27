import collections
import dataclasses
import datetime
from dataclasses import is_dataclass

from asgiref.sync import sync_to_async

from lorelie.database.functions.aggregation import Count
from lorelie.database.functions.dates import Extract
from lorelie.database.nodes import (InsertNode, IntersectNode, JoinNode,
                                    OrderByNode, SelectNode, UpdateNode,
                                    WhereNode)
from lorelie.exceptions import FieldExistsError, MigrationsExistsError
from lorelie.queries import Query, QuerySet, ValuesIterable


class ManagerMixin:
    def get_query(self, nodes, **kwargs):
        query = Query(**kwargs)
        if nodes:
            query.add_sql_nodes(nodes)
        return query

    def resolve_queryset_from_query(self, nodes, **kwargs):
        query = self.get_query(nodes, **kwargs)
        return QuerySet(query)


class DatabaseManager:
    """A manager is a class that implements query
    functionnalities for inserting, updating, deleting
    or retrieving data from the underlying database tables"""

    def __init__(self):
        self.table_map = {}
        self.database = None
        self.table = None
        # Tells if the manager was
        # created via as_manager
        self.auto_created = True
        self._test_current_table_on_manager = None

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.database}>'

    def __get__(self, instance, cls=None):
        # FIXME: "objects" is on the cls which
        # means that it's the same object and
        # therefore keeps the same table in mind
        # NOTE: Maybe we can create a unique inidivual
        # manager instance for each table instead of
        # using a shared global objects instance for
        # each table
        self.table = instance

        try:
            self.database = instance.attached_to_database
        except Exception as e:
            raise ExceptionGroup(
                e.args[0],
                [
                    MigrationsExistsError()
                ]
            )
        else:
            self.table_map = self.database.table_map
            self.table.backend.current_table = instance
        finally:
            return self

    @classmethod
    def as_manager(cls):
        instance = cls()
        instance.auto_created = False
        return instance

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

    def first(self):
        """Returns the first row from
        a database table"""
        select_node = SelectNode(self.table)
        orderby_node = OrderByNode(self.table, 'id')

        query = self.database.query_class(table=self.table)
        query.add_sql_nodes([select_node, orderby_node])
        return QuerySet(query)[0]

    def last(self):
        """Returns the last row from
        a database table"""
        select_node = SelectNode(self.table)
        orderby_node = OrderByNode(self.table, '-id')

        query = self.database.query_class(table=self.table)
        query.add_sql_nodes([select_node, orderby_node])
        return QuerySet(query)[0]

    def all(self):
        select_node = SelectNode(self.table)
        query = self.table.query_class(table=self.table)

        if self.table.ordering:
            orderby_node = OrderByNode(
                self.table,
                *self.table.ordering
            )
            query.add_sql_node(orderby_node)

        query.add_sql_node(select_node)
        return QuerySet(query)

    def create(self, **kwargs):
        """The create function facilitates the creation 
        of a new row in the specified table within the 
        current database

        >>> table.objects.create('celebrities', firstname='Kendall')
        """
        kwargs = self._validate_auto_fields(self.table, kwargs)
        validated_data = self.table.pre_save_setup_from_dict(kwargs)

        query = self.database.query_class(table=self.table)
        insert_node = InsertNode(
            self.table,
            insert_values=validated_data,
            returning=self.table.field_names
        )

        query.add_sql_node(insert_node)
        return QuerySet(query)[0]

    def filter(self, *args, **kwargs):
        """Filter the data in the database based on
        a set of criteria using filter keyword arguments

        >>> table.objects.filter('celebrities', firstname='Kendall')
        ... table.objects.filter('celebrities', age__gt=20)
        ... table.objects.filter('celebrities', firstname__in=['Kendall'])

        Filtering can also be done using more complexe logic via database
        functions such as the `Q` function:

        >>> table.objects.filter('celebrities', Q(firstname='Kendall') | Q(firstname='Kylie'))
        ... table.objects.filter('celebrities', Q(firstname='Margot') | Q(firstname='Kendall') & Q(followers__gte=1000))
        """
        select_node = SelectNode(self.table)
        where_node = WhereNode(*args, **kwargs)

        query = self.table.query_class(table=self.table)
        query.add_sql_nodes([select_node, where_node])

        if self.table.ordering:
            orderby_node = OrderByNode(
                self.table,
                *self.table.ordering
            )
            query.add_sql_node(orderby_node)

        return QuerySet(query)

    def get(self, *args, **kwargs):
        """Returns a specific row from the database
        based on a set of criteria

        >>> instance.objects.get('celebrities', id__eq=1)
        ... instance.objects.get('celebrities', id=1)
        """
        select_node = SelectNode(self.table)
        where_node = WhereNode(*args, **kwargs)

        query = self.table.query_class(table=self.table)
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

    def annotate(self, *args, **kwargs):
        """The annotate method allows the usage of advanced functions or expressions in 
        a query by adding additional fields to your querysets based on the values 
        of existing fields

        For example, returning each values of the name in lower or uppercase:

        >>> table.objects.annotate('celebrities', lowered_name=Lower('name'))
        ... table.objects.annotate('celebrities', uppered_name=Upper('name'))

        Returning only the year for a given column:

        >>> database.objects.annotate(year=ExtractYear('created_on'))

        We can also run cases. For example, when a price is equal to 1,
        then create temporary column named custom price with either 2 or 3:

        >>> condition = When('price=1', 2)
        ... case = Case(condition, default=3, output_field=CharField())
        ... table.objects.annotate('celebrities', custom_price=case)

        Suppose you have two columns `price` and `tax` we can return a new
        column with `price + tax`:

        >>> table.objects.annotate('products', new_price=F('price') + F('tax'))

        You can also add a constant value to a column:

        >>> table.objects.annotate('products', new_price=F('price') + 10)

        The `Value` expression can be used to return a specific value in a column:        

        >>> table.objects.annotate('products', new_price=Value(1))

        Finally, `Q` objects are used to encapsulate a collection 
        of keyword arguments and can be used to evaluate conditions. 
        For instance, to annotate a result indicating whether the price 
        is greater than 1:

        >>> table.objects.annotate('products', result=Q(price__gt=1))

        Using expressions without an alias field name will raise an error.

        Aggregate functions can also be used in annotations, but they will return 
        the result for each element grouped by a specified field. For example, to 
        count the number of occurrences of each `price`:

        >>> table.objects.annotate('products', Count('price'))

        The above will return the price count for each products. If there are
        two products with a price of 1 we will then get `[{'price': 1, 'count_price': 2}]`
        """
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
            return self.all(self.table)

        alias_fields = list(kwargs.keys())

        for alias, func in kwargs.items():
            if self.table.has_field(alias):
                raise ValueError(
                    "Alias field names cannot override table "
                    f"columns: {alias} -> {self.table.field_names}"
                )

            internal_type = getattr(func, 'internal_type')
            if internal_type == 'expression':
                func.alias_field_name = alias

        annotation_map = self.table.backend.build_annotation(kwargs)
        annotated_sql_fields = self.table.backend.comma_join(
            annotation_map.joined_final_sql_fields
        )

        return_fields = ['*', annotated_sql_fields]
        select_node = SelectNode(self.table, *return_fields)

        query = self.database.query_class(table=self.table)
        query.alias_fields = list(alias_fields)
        query.add_sql_node(select_node)

        if annotation_map.requires_grouping:
            groupby_sql = self.table.backend.GROUP_BY.format_map({
                'conditions': 'id'
            })
            query.select_map.groupby = groupby_sql

        if self.table.ordering:
            orderby_node = OrderByNode(
                self.table,
                *self.table.ordering
            )
            query.add_sql_node(orderby_node)
        return QuerySet(query)

    def values(self, *fields):
        """Returns data from the database as a list
        of dictionnary values

        >>> instance.objects.as_values('celebrities', 'id')
        ... [{'id': 1}]
        """
        # columns = list(fields) or ['rowid', '*']
        columns = list(fields)
        select_node = SelectNode(self.table, *columns)
        query = self.database.query_class(table=self.table)
        query.add_sql_node(select_node)

        if self.table.ordering:
            orderby_node = OrderByNode(
                self.table, *self.table.ordering)
            query.add_sql_node(orderby_node)

        queryset = QuerySet(query)

        # def dictionnaries():
        #     for row in queryset:
        #         yield row._cached_data

        # return list(dictionnaries())
        return list(ValuesIterable(queryset, fields=columns))

    def dataframe(self, *fields):
        """This method returns data from the database as a pandas 
        DataFrame object. This allows for easy manipulation and 
        analysis of the data using pandas' powerful data handling 
        capabilities

        >>> instance.objects.as_dataframe('celebrities', 'id')
        ... pandas.DataFrame
        """
        import pandas
        return pandas.DataFrame(self.values(*fields))

    def order_by(self, *fields):
        """Returns data ordered by the fields specified
        by the user. It can be sorted in ascending order:

        >>> instance.objects.order_by('celebrities', 'firstname')

        Or, descending order:

        >>> instance.objects.order_by('celebrities', '-firstname')
        """
        select_node = SelectNode(self.table)
        order_by_node = OrderByNode(self.table, *fields)

        query = self.table.query_class(table=self.table)
        query.add_sql_nodes([select_node, order_by_node])
        return QuerySet(query)

    def aggregate(self, *args, **kwargs):
        """Returns data ordered by the fields specified by the user. 
        You can specify the sorting order by providing the field names. 
        Prefixing a field with a hyphen (-) sorts the data in descending order, 
        while providing the field name without a prefix sorts the data 
        in ascending order

        >>> table.objects.aggregate('celebrities', Count('id'))
        ... {'age__count': 1}

        >>> table.objects.aggregate('celebrities', count_age=Count('id'))
        ... {'count_age': 1}
        """
        functions = list(args)

        # Functions used in args will get an
        # automatic aggregate name that we
        # will implement in the kwargs
        none_aggregate_functions = []
        for function in functions:
            allows_aggregation = getattr(function, 'allow_aggregation', False)
            if not allows_aggregation:
                none_aggregate_functions.append(function)
                continue
            kwargs[function.aggregate_name] = function

        if none_aggregate_functions:
            raise ValueError(
                "Aggregate requires aggregate functions"
            )

        aggregate_sqls = []
        annotation_map = self.table.backend.build_annotation(kwargs)
        aggregate_sqls.extend(annotation_map.joined_final_sql_fields)

        select_node = SelectNode(self.table, *aggregate_sqls)

        query = self.table.query_class(table=self.table)
        query.add_sql_node(select_node)
        query.alias_fields = annotation_map.alias_fields
        query.run()
        return getattr(query.result_cache[0], '_cached_data', {})

    def count(self):
        """Returns the number of items present
        in the database

        >>> table.objects.count('celebrities')
        """
        result = self.aggregate(Count('id'))
        return result.get('id__count')

    # def foreign_table(self, relationship):
    #     result = relationship.split('__')
    #     if len(result) != 2:
    #         raise ValueError(
    #             'Foreign table should contain the left table and right table e.g. left__right')
    #     left_table, right_table = result
    #     return ForeignTablesManager(left_table, right_table, self)

    def distinct(self, *columns):
        """Returns items from the database which are
        distinct

        >>> table.objects.distinct('celebrities', 'firstname')
        """
        select_node = SelectNode(self.table, *columns, distinct=True)
        query = self.table.query_class(table=self.table)
        query.add_sql_node(select_node)

        if self.table.ordering:
            ordering_node = OrderByNode(
                self.table,
                *self.table.ordering
            )
            query.add_sql_node(ordering_node)
        return QuerySet(query)

    def bulk_create(self, objs):
        """Creates multiple objects in the database at once
        using a list of datasets or dictionnaries

        >>> @dataclasses.dataclass
        ... class Celebrity:
        ...     name: str

        >>> table.objects.bulk_create('celebrities', [Celebrity('Taylor Swift')])
        ... [<Celebrity: 1>]
        """
        invalid_objects_counter = 0
        for obj in objs:
            if not is_dataclass(obj):
                invalid_objects_counter = invalid_objects_counter + 1
                continue

        if invalid_objects_counter > 0:
            raise ValueError(
                "Objects used in bulk create should be an "
                "instance of a dataclass"
            )

        for obj in objs:
            fields = dataclasses.fields(obj)
            for field in fields:
                if not self.table.has_field(field.name):
                    raise FieldExistsError(field, self.table)

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

            # TODO: We have to validate auto fields
            # self._validate_auto_fields(self.table, )

        # TODO: We have to call validate values

        insert_node = InsertNode(
            self.table,
            batch_values=values_to_create,
            returning=self.table.field_names
        )

        query = self.table.query_class(table=self.table)
        query.add_sql_node(insert_node)
        query.run(commit=True)
        return QuerySet(query)

    def dates(self, field, field_to_sort='year', ascending=True):
        values = self.datetimes(
            field,
            field_to_sort=field_to_sort,
            ascending=ascending
        )
        return list(map(lambda x: x.date(), values))

    def datetimes(self, field, field_to_sort='year', ascending=True):
        qs1 = self.annotate(**{field_to_sort: Extract(field, field_to_sort)})
        qs2 = qs1.order_by(field_to_sort if ascending else f'-{field_to_sort}')
        return list(map(lambda x: x[field], qs2))

    def difference(self):
        return NotImplemented

    def earliest(self, *fields):
        return NotImplemented

    def latest(self, *fields):
        # selected_table = self.before_action(table)
        # select_node = SelectNode(selected_table, *fields, limit=1)
        # order_by_node = OrderByNode(selected_table, *fields)
        # query = selected_table.query_class(table=selected_table)
        # query.add_sql_nodes(select_node, order_by_node)
        # return QuerySet(query)[0]
        return NotImplemented

    def exclude(self, *args, **kwargs):
        """Selects all the values from the database
        that match the filters

        >>> table.objects.exclude(firstname='Kendall')"""
        select_node = SelectNode(self.table)
        where_node = ~WhereNode(*args, **kwargs)

        query = self.table.query_class(table=self.table)
        query.add_sql_nodes([select_node, where_node])

        if self.table.ordering:
            ordering_node = OrderByNode(
                self.table,
                *self.table.ordering
            )
            query.add_sql_node(ordering_node)
        return QuerySet(query)

    def extra(self):
        return NotImplemented

    def only(self):
        return NotImplemented

    def get_or_create(self, create_defaults={}, **kwargs):
        """Tries to get a row in the database using the coditions
        passed in kwargs. It then uses the `defaults`
        parameter to create the values that do not exist.

        If `defaults` is not specified, the values passed in kwargs
        will become the default `defaults`.

        >>> defaults = {'age': 24}
        ... table.objects.get_or_create('celebrities', create_defaults=defaults, firstname='Margot')

        If the queryset returns multiple elements, an error is raised.
        """
        select_node = SelectNode(self.table)
        where_node = WhereNode(**kwargs)
        sql = [select_node, where_node]

        query = self.table.query_class(table=self.table)
        query.add_sql_nodes(sql)
        queryset = QuerySet(query)

        if queryset.exists():
            if len(queryset) > 1:
                raise ValueError('Returned more than one values')
            return queryset[-0]
        else:
            create_defaults.update(kwargs)
            create_defaults = self._validate_auto_fields(
                self.table,
                create_defaults
            )
            _, create_defaults = self.table.pre_save_setup_from_dict(
                create_defaults
            )

            insert_node = InsertNode(
                self.table,
                insert_values=create_defaults,
                returning=self.table.field_names
            )
            new_query = query.create(table=self.table)
            new_query.add_sql_node(insert_node)
            return QuerySet(new_query)[0]

    # def select_for_update()
    # def select_related()
    # def fetch_related()

    def update_or_create(self, create_defaults={}, **kwargs):
        """Updates a row in the database selected on the
        filters determined by kwargs. It then uses the `create_defaults`
        parameter to create the values that do not exist.

        If `create_defaults` is not specified, the values passed in kwargs
        will become the default `create_defaults`.

        >>> create_defaults = {'age': 24}
        ... table.objects.update_or_create('celebrities', create_defaults=create_defaults, firstname='Margot')

        If the queryset returns multiple elements (from the get conditions specified
        via kwargs), an error is raised.
        """
        select_node = SelectNode(self.table)
        query = self.table.query_class(table=self.table)
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

        _, create_defaults = self.table.pre_save_setup_from_dict(
            create_defaults
        )
        ids = list(map(lambda x: x['id'], queryset))

        if len(ids) > 1:
            # TODO: Check for cases where kwargs is not provided
            # but there's only one element in the database
            raise ValueError('Get returned more than one value')

        returning_fields = self.table.field_names

        if queryset.exists():
            create_defaults = self._validate_auto_fields(
                self.table,
                create_defaults,
                update_only=True
            )
            # TODO: Add a returning like
            # the InsertNode
            update_node = UpdateNode(
                self.table,
                create_defaults,
                id__in=ids
            )

            new_query = query.create(table=self.table)
            new_query.add_sql_node(update_node)

            returning_fields = self.table.backend.comma_join(
                returning_fields
            )
            new_query.add_sql_node(f'returning {returning_fields}')
        else:
            create_defaults = self._validate_auto_fields(
                self.table,
                create_defaults,
            )
            insert_node = InsertNode(
                self.table,
                insert_values=create_defaults,
                returning=returning_fields
            )
            new_query = query.create(table=self.table)
            new_query.add_sql_node(insert_node)
            # new_query.run(commit=True)

        # We have to execute the query before
        # hand. The reason for this is the insert
        # and update nodes need to be comitted
        # immediately otherwise the QuerySet would
        # delay their evaaluation which would not
        # then modify the data in the database
        return QuerySet(new_query)[0]

    def intersect(self, qs1, qs2):
        """The intersect function allows you to combine 
        the result sets of two queries and returns 
        distinct rows that appear in both result sets. 
        This is similar to the SQL `INTERSECT` operator, 
        which is used to find the common records between 
        two `SELECT` statements

        >>> qs1 = table.objects.all('celebrities')
        ... qs2 = table.objects.all('celebrities')
        ... qs3 = table.objects.intersect('celebrities', qs1, qs2)
        """
        if not isinstance(qs1, QuerySet):
            raise ValueError(f'{qs1} should be an instance of QuerySet')

        if not isinstance(qs2, QuerySet):
            raise ValueError(f'{qs2} should be an instance of QuerySet')

        if not qs1.query.is_evaluated:
            qs1.load_cache()

        if not qs2.query.is_evaluated:
            qs2.load_cache()

        node = IntersectNode(qs1.query.sql, qs2.query.sql)
        query = self.table.query_class(table=self.table)
        query.add_sql_node(node)
        return QuerySet(query)

    async def afirst(self):
        return await sync_to_async(self.first)()

    async def alast(self):
        return await sync_to_async(self.last)()

    async def aall(self):
        return await sync_to_async(self.all)()

    async def acreate(self, **kwargs):
        return await sync_to_async(self.create)(**kwargs)

    async def afilter(self, **kwargs):
        return await sync_to_async(self.filter)(**kwargs)

    async def aget(self, *args, **kwargs):
        return await sync_to_async(self.get)(*args, **kwargs)

    async def aannotate(self, *args, **kwargs):
        return await sync_to_async(self.annotate)(*args, **kwargs)

    async def avalues(self, *fields):
        return await sync_to_async(self.values)(*fields)

    async def adataframe(self, *fields):
        return await sync_to_async(self.dataframe)(*fields)

    async def abulk_create(self, *objs):
        return await sync_to_async(self.dataframe)(*objs)

    async def aorder_by(self, *fields):
        return await sync_to_async(self.order_by)(*fields)

    async def acount(self):
        return await sync_to_async(self.count)()

    async def adates(self, field, field_to_sort=None, ascending=True):
        return await sync_to_async(self.dates)(field, field_to_sort=field_to_sort, ascending=ascending)

    async def adatetimes(self, field, field_to_sort=None, ascending=True):
        return await sync_to_async(self.datetimes)(field, field_to_sort=field_to_sort, ascending=ascending)

    async def adifference(self):
        return await sync_to_async(self.difference)()

    async def adistinct(self, *columns):
        return await sync_to_async(self.distinct)(*columns)

    async def aearliest(self, *fields):
        return await sync_to_async(self.earliest)(*fields)

    async def alatest(self, *fields):
        return await sync_to_async(self.latest)(*fields)

    async def aonly(self):
        return await sync_to_async(self.only)()

    async def aexclude(self, *args, **kwargs):
        return await sync_to_async(self.exclude)(*args, **kwargs)

    async def aextra(self):
        return await sync_to_async(self.extra)()

    # async def aselect_for_update(self, table):
    #     return await sync_to_async(self.select_for_update)(table)

    # async def aselect_related(self, table):
    #     return await sync_to_async(self.select_related)(table)

    # async def afetch_related(self, table):
    #     return await sync_to_async(self.fetch_related)(table)

    async def aupdate_or_create(self, create_defaults={}, **kwargs):
        return await sync_to_async(self.update_or_create)(create_defaults=create_defaults, **kwargs)

    async def aresolve_expression(self):
        return await sync_to_async(self.resolve_expression)()

    async def aaggregate(self, *args, **kwargs):
        return await sync_to_async(self.aggregate)(*args, **kwargs)


class ForeignTablesManager(ManagerMixin):
    """This is the main manager used to access/reverse
    access tables linked by a relationship"""
    reverse = False

    def __init__(self, relationship_map):
        self.parent_table = None
        self.joined_table = None
        self.relationship_map = relationship_map
        self.row_instance = None

    def __repr__(self):
        direction = '->'
        if self.reverse:
            direction = '<-'
        return f'<{self.__class__.__name__} [from {direction} to]>'

    @classmethod
    def new(cls, parent_table, relationship_map):
        instance = cls(relationship_map)
        instance.parent_table = parent_table
        instance.joined_table = relationship_map.right_table
        return instance

    def all(self):
        select_node = SelectNode(self.parent_table)
        join_node = JoinNode(self.parent_table, self.relationship_map)
        return self.resolve_queryset_from_query([select_node, join_node], table=self.parent_table)

    def filter(self, *args, **kwargs):
        select_node = SelectNode(self.parent_table)
        join_node = JoinNode(self.parent_table, self.relationship_map)
        where_node = WhereNode(*args, **kwargs)
        nodes = [select_node, join_node, where_node]
        return self.resolve_queryset_from_query(nodes, table=self.parent_table)

    # def create(self, **kwargs):
    #     # kwargs = self._validate_auto_fields(self.table, kwargs)
    #     # values, kwargs = self.table.pre_save_setup_from_dict(kwargs)

    #     related_table = self.relationship_map.right_table

    #     query = Query(table=related_table)
    #     kwargs[self.relationship_map.forward_field_name] = self.row_instance.pk
    #     insert_node = InsertNode(
    #         related_table,
    #         insert_values=kwargs,
    #         returning=related_table.field_names
    #     )

    #     query.add_sql_node(insert_node)
    #     return QuerySet(query)[0]


class ForwardForeignTableManager(ForeignTablesManager):
    """A manager that manages the relationship from
    the parent table to the child table `parent -> child`
    """

    def __init__(self, relationship_map, **kwargs):
        super().__init__(relationship_map)
        self.parent_table = relationship_map.left_table


class BackwardForeignTableManager(ForeignTablesManager):
    """A manager that manages the relationship from
    the child table to the parent tabel `parent <- child`
    """

    def __init__(self, relationship_map, **kwargs):
        super().__init__(relationship_map)
        self.reverse = True
