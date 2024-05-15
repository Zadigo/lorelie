from re import S
import sqlite3
from sqlite3 import OperationalError

from lorelie.aggregation import Count
from lorelie.database.nodes import BaseNode, SelectMap, WhereNode, OrderByNode


class Query:
    """This class represents an sql statement query
    and is responsible for executing the query on the
    database. The return data is stored on
    the `result_cache`
    """

    def __init__(self, table=None, backend=None):
        self.table = table
        self.backend = table.backend if table is not None else backend

        from lorelie.backends import connections
        if self.backend is None:
            self.backend = connections.get_last_connection()

        from lorelie.backends import SQLiteBackend
        if not isinstance(self.backend, SQLiteBackend):
            raise ValueError(
                "Backend connection should be an "
                "instance SQLiteBackend"
            )

        self.sql = None
        self.result_cache = []
        self.alias_fields = []
        self.is_evaluated = False
        self.statements = []
        self.select_map = SelectMap()

    def __repr__(self):
        return f'<{self.__class__.__name__} [{self.sql}]>'

    @classmethod
    def run_multiple(cls, backend, *sqls, **kwargs):
        """Runs multiple queries against the database"""
        for sql in sqls:
            instance = cls(backend, sql, **kwargs)
            instance.run(commit=True)
            yield instance

    @classmethod
    def create(cls, table=None, backend=None):
        """Creates a new `Query` class to be executed"""
        return cls(table=table, backend=backend)

    @classmethod
    def run_script(cls, backend=None, table=None, sql_tokens=[]):
        template = 'begin; {statements} commit;'
        instance = cls(table=table, backend=backend)
        instance.add_sql_nodes(sql_tokens)

        statements = []
        for statement in instance.statements:
            statements.append(instance.backend.finalize_sql(statement))

        joined_statements = instance.backend.simple_join(statements)
        script = template.format(statements=joined_statements)

        result = instance.backend.connection.executescript(script)
        instance.backend.connection.commit()
        instance.result_cache = list(result)
        return instance

    def add_sql_node(self, node):
        if not isinstance(node, (BaseNode, str)):
            raise ValueError('Node should be an instance of BaseNode or <str>')

        if isinstance(node, BaseNode):
            if node.node_name == 'order_by':
                if self.select_map.order_by is not None:
                    node = self.select_map.order_by & node
            self.select_map[node.node_name] = node

        self.statements.append(node)

    def add_sql_nodes(self, nodes):
        if not isinstance(nodes, list):
            raise ValueError('Node should be an instance of BaseNode or <str>')
        for node in nodes:
            self.add_sql_node(node)

    def pre_sql_setup(self):
        """Prepares a statement before it is sent
        to the database by joining the sql statements
        and implement a `;` to the end

        >>> ["select url from seen_urls", "where url='http://'"]
        ... "select url from seen_urls where url='http://';"
        """
        text_statements = []

        if self.select_map.should_resolve_map:
            statement = self.select_map.resolve(self.backend)
            text_statements.extend(statement)
        else:
            for statement in self.statements:
                if isinstance(statement, BaseNode):
                    text_statements.extend(statement.as_sql(self.backend))
                elif isinstance(statement, str):
                    text_statements.append(statement)

        sql = self.backend.simple_join(text_statements)
        finalized_sql = self.backend.finalize_sql(sql)
        is_valid = sqlite3.complete_statement(finalized_sql)
        if not is_valid:
            pass
        self.sql = finalized_sql

    def run(self, commit=False):
        """Runs an sql statement and stores the
        return data in the `result_cache`"""
        self.pre_sql_setup()

        try:
            result = self.backend.connection.execute(self.sql)
        except OperationalError as e:
            raise
        except Exception as e:
            print(e)
        else:
            if commit:
                self.backend.connection.commit()
            self.result_cache = list(result)

    def transform_to_python(self):
        """Transforms the values returned by the
        database into Python objects"""
        from lorelie.fields.base import AliasField
        for row in self.result_cache:
            for name in row._fields:
                value = row[name]
                if name in self.alias_fields:
                    instance = AliasField(name)
                    field = instance.get_data_field(value)
                else:
                    field = self.table.get_field(name)
                setattr(row, name, field.to_python(value))


class ValuesIterable:
    """An iterator that generates a dictionnary
    key value pair from queryset"""

    def __init__(self, queryset, fields=[]):
        self.queryset = queryset
        self.fields = fields

    def __iter__(self):
        for row in self.queryset:
            yield row._cached_data

        # self.queryset.load_cache()

        # if not self.fields:
        #     self.fields = self.queryset.query._table.field_names

        # for row in self.queryset.result_cache:
        #     result = {}
        #     for field in self.fields:
        #         result[field] = row[field]
        #     yield result


class QuerySet:
    def __init__(self, query, skip_transform=False):
        if not isinstance(query, Query):
            raise ValueError(f"{query} should be an instance of Query")

        self.query = query
        self.result_cache = []
        self.values_iterable_class = ValuesIterable
        # There are certain cases where we want
        # to use QuerySet but it's not affialiated
        # to any table ex. returning a QuerySet of
        # table indexes. This allows to skip the
        # python transform of the data
        self.skip_transform = skip_transform

    def __repr__(self):
        self.load_cache()
        return f'<{self.__class__.__name__} {self.result_cache}>'

    def __str__(self):
        self.load_cache()
        return str(self.result_cache)

    def __getitem__(self, index):
        self.load_cache()
        try:
            return self.result_cache[index]
        except KeyError:
            return None

    def __iter__(self):
        self.load_cache()
        for item in self.result_cache:
            yield item

    def __contains__(self, value):
        return value in self.result_cache

    def __len__(self):
        self.load_cache()
        return len(self.result_cache)

    @property
    def dataframe(self):
        import pandas
        return pandas.DataFrame(self.values())

    @property
    def sql_statement(self):
        return self.query.sql

    def load_cache(self):
        if not self.result_cache:
            self.query.run()
            if not self.skip_transform:
                self.query.transform_to_python()
            self.result_cache = self.query.result_cache

    def _test_chaining_on_queryset(self):
        self.query.add_sql_node('order by firstname')
        return self

    def first(self):
        self.query.select_map.limit = 1
        return self

    def last(self):
        self.query.select_map.limit = 1
        return self

    def all(self):
        return self

    def filter(self, *args, **kwargs):
        backend = self.query._backend
        filters = backend.decompose_filters(**kwargs)

    def get(self, **kwargs):
        pass

    def annotate(self, **kwargs):
        pass

    def values(self, *fields):
        results = list(self.values_iterable_class(self, fields=fields))

        return_values = []
        for result in results:
            item = {}
            for field in fields:
                if field in self.query.alias_fields:
                    value = result[field]

                if self.query.table.has_field(field):
                    value = result[field]
                item[field] = value
            return_values.append(item)
        return return_values

    def dataframe(self, *fields):
        import pandas
        return pandas.DataFrame(self.values(*fields))

    def order_by(self, *fields):
        orderby_node = OrderByNode(self.query.table, *fields)
        if not self.query.is_evaluated:
            self.query.add_sql_node(orderby_node)
        return self.__class__(self.query)

    #     ascending_fields = set()
    #     descending_fields = set()

    #     for field in fields:
    #         if field.startswith('-'):
    #             field = field.removeprefix('-')
    #             descending_fields.add(field)
    #         else:
    #             ascending_fields.add(field)

    #     # There might a case where the result_cache
    #     # is not yet loaded especially using
    #     # chained statements
    #     # e.g. table.annotate().order_by()
    #     # In that specific case, the QuerySet
    #     # of annotate would have cache
    #     # Solution 2: Delegate the execution
    #     # of the final query from annotate
    #     # to the query of order_by in that
    #     # sense we would not execute two
    #     # different queries but just one
    #     # single one modified
    #     self.load_cache()

    #     previous_sql = self.query._backend.de_sqlize_statement(self.query._sql)
    #     ascending_statements = [
    #         self.query._backend.ASCENDING.format_map({'field': field})
    #         for field in ascending_fields
    #     ]
    #     descending_statements = [
    #         self.query._backend.DESCENDING.format_map({'field': field})
    #         for field in descending_fields
    #     ]
    #     final_statement = ascending_statements + descending_statements
    #     order_by_clause = self.query._backend.ORDER_BY.format_map({
    #         'conditions': self.query._backend.comma_join(final_statement)
    #     })
    #     sql = [previous_sql, order_by_clause]
    #     new_query = self.query.create(
    #         self.query._backend,
    #         sql,
    #         table=self.query._table
    #     )
    #     # new_query.run()
    #     # return QuerySet(new_query)
    #     # return new_query.result_cache
    #     return QuerySet(new_query)

    def aggregate(self, *args, **kwargs):
        for func in args:
            if not isinstance(func, (Count)):
                continue
            kwargs.update({func.aggregate_name: func})

        result_set = {}
        for alias_key, func in kwargs.items():
            result_set[alias_key] = func.use_queryset(alias_key, self)
        return result_set

    def count(self):
        return self.__len__()

    def exclude(self, **kwargs):
        pass

    def update(self, **kwargs):
        """Update a set a columns from the queryset

        >>> queryset = db.objects.filter(firstname='Kendall')
        ... queryset.update(age=26)
        ... 1
        """
        backend = self.query._backend
        columns, values = backend.dict_to_sql(kwargs)

        conditions = []
        for i, column in enumerate(columns):
            conditions.append(backend.EQUALITY.format_map({
                'field': column,
                'value': values[i]
            }))

        joined_conditions = backend.comma_join(conditions)

        update_sql = backend.UPDATE.format_map({
            'table': self.query._table.name
        })

        update_set_clause = backend.UPDATE_SET.format(params=joined_conditions)

        resultset = self.all()
        where_clause_sql = []
        for item in resultset:
            where_clause_sql.append(backend.EQUALITY.format_map({
                'field': 'id',
                'value': item['id']
            }))

        where_clause = backend.WHERE_CLAUSE.format_map({
            'params': backend.operator_join(where_clause_sql, operator='or')
        })

        update_sql_tokens = [
            update_sql,
            update_set_clause,
            where_clause
        ]
        query = backend.current_table.query_class(
            update_sql_tokens,
            table=self.query._table
        )
        query.run(commit=True)
        return query.result_cache
