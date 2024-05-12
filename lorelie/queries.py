from sqlite3 import OperationalError

from lorelie.aggregation import Count


class Query:
    """This class represents an sql statement query
    and is responsible for executing the query on the
    database. The return data is stored on
    the `result_cache`
    """

    def __init__(self, sql_tokens, backend=None, table=None):
        self._table = table
        self._backend = table.backend if table is not None else backend

        from lorelie.backends import connections
        if self._backend is None:
            self._backend = connections.get_last_connection()

        from lorelie.backends import SQLiteBackend
        if not isinstance(self._backend, SQLiteBackend):
            raise ValueError(
                "Backend connection should be an "
                "instance SQLiteBackend"
            )

        self._sql = None
        self._sql_tokens = sql_tokens
        self.result_cache = []
        self.alias_fields = []

    def __repr__(self):
        return f'<{self.__class__.__name__} [{self._sql}]>'

    @classmethod
    def run_multiple(cls, backend, *sqls, **kwargs):
        """Runs multiple queries against the database"""
        for sql in sqls:
            instance = cls(backend, sql, **kwargs)
            instance.run(commit=True)
            yield instance

    @classmethod
    def create(cls, backend, sql_tokens, table=None):
        """Creates a new `Query` class to be executed"""
        return cls(backend, sql_tokens, table=table)

    @classmethod
    def run_script(cls, sql_tokens, backend=None, table=None):
        instance = cls(sql_tokens, backend=backend, table=table)
        if sql_tokens:
            result = instance._backend.connection.executescript(sql_tokens[0])
            instance._backend.connection.commit()
            instance.result_cache = list(result)
        return instance

    def prepare_sql(self):
        """Prepares a statement before it is sent
        to the database by joining the sql statements
        and implement a `;` to the end

        >>> ["select url from seen_urls", "where url='http://'"]
        ... "select url from seen_urls where url='http://';"
        """
        sql = self._backend.simple_join(self._sql_tokens)
        self._sql = self._backend.finalize_sql(sql)

    def run(self, commit=False):
        """Runs an sql statement and stores the
        return data in the `result_cache`"""
        self.prepare_sql()

        try:
            result = self._backend.connection.execute(self._sql)
        except OperationalError as e:
            raise
        except Exception as e:
            print(e)
        else:
            if commit:
                self._backend.connection.commit()
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
                    field = self._table.get_field(name)
                setattr(row, name, field.to_python(value))


class ValuesIterable:
    """An iterator that generates a dictionnary
    key value pair from queryset"""

    def __init__(self, queryset, fields=[]):
        self.queryset = queryset
        self.fields = fields

    def __iter__(self):
        self.queryset.load_cache()

        if not self.fields:
            self.fields = self.queryset.query._table.field_names

        for row in self.queryset.result_cache:
            result = {}
            for field in self.fields:
                result[field] = row[field]
            yield result


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
        return self.query._sql

    def load_cache(self):
        if not self.result_cache:
            # TODO: Run the query only when the
            # queryset is evaluated as oppposed
            # to running it in the methods: filters etc.
            self.query.run()
            self.query.transform_to_python()
            self.result_cache = self.query.result_cache

    def first(self):
        return self.all()[-0]

    def last(self):
        return self.all()[-1]

    def all(self):
        return self

    def filter(self, *args, **kwargs):
        backend = self.query._backend
        filters = backend.decompose_filters(**kwargs)

        items = self.all()
        for item in items:
            for column, operator, value in filters:
                if operator == '=':
                    if item[column] == value:
                        continue

                if operator == '>':
                    if item[column] > value:
                        continue

                if operator == '<':
                    if item[column] < value:
                        continue

                if operator == '>=':
                    if item[column] >= value:
                        continue

                if operator == '<=':
                    if item[column] <= value:
                        continue

                if operator == '!=':
                    if item[column] != value:
                        continue

                if operator == 'contains':
                    if item[column] in value:
                        continue

                if operator == 'startswith':
                    if item[column].startswith(value):
                        continue

                if operator == 'endswith':
                    if item[column].endswith(value):
                        continue

                if operator == 'between':
                    continue

                if operator == 'endswith':
                    if item[column] is None:
                        continue

    def get(self, **kwargs):
        pass

    def annotate(self, **kwargs):
        pass

    def values(self, *fields):
        return list(self.values_iterable_class(self, fields=fields))

    def dataframe(self, *fields):
        import pandas
        return pandas.DataFrame(self.values(*fields))

    # def order_by(self, *fields):
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
    #         self.query._backend.DESCENDNIG.format_map({'field': field})
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
