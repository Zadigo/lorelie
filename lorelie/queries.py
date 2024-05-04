

class Query:
    """This class represents an sql statement query
    and is responsible for executing the query on the
    database. The return data is stored on
    the `result_cache`
    """

    def __init__(self, table=None, sql_tokens=[], backend=None):
        from lorelie.tables import Table
        from lorelie.backends import connections

        if not isinstance(table, (Table, str)):
            raise ValueError(
                'Table should be either a string or an instance or table')

        self._table = table
        self._sql = None
        self._sql_tokens = sql_tokens
        self.result_cache = []
        self._backend = backend or connections.get_last_connection()

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
    def run_script(cls, backend, sql_tokens):
        instance = cls(backend, sql_tokens)
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
        except Exception as e:
            print(e)
        else:
            if commit:
                self._backend.connection.commit()
            self.result_cache = list(result)

    def prepare_query(self, backend):
        from lorelie.backends import SQLiteBackend
        if not isinstance(backend, SQLiteBackend):
            raise ValueError('Backend should be an instance of SQLiteBackend')
        self._backend = backend

    def create_not_like_clause(self, name, condition):
        not_like_clause = self._backend.NOT_LIKE.format_map({
            'field': name,
            'wildcard': self._backend.quote_value(condition)
        })
        return not_like_clause

    def coerce_tokens(self, *tokens):
        self._sql_tokens.extend(list(tokens))

    def create_order_by_clause(self):
        pass

    def create_where_clause(self, filters):
        built_filters = self._backend.build_filters(self._backend.decompose_filters(
            **filters
        ))
        joined_statements = self._backend.operator_join(built_filters)
        where_clause = self._backend.WHERE_CLAUSE.format_map({
            'params': joined_statements
        })
        return where_clause

    def create_select(self, fields=[]):
        from lorelie.tables import Table

        if self._table is None:
            raise ValueError('Select requires a table name')

        base_return_fields = fields or ['rowid', '*']

        table_name = None
        if isinstance(self._table, Table):
            table_name = self._table.name
        else:
            table_name = self._table

        select_sql = self._backend.SELECT.format_map({
            'fields': self._backend.comma_join(base_return_fields),
            'table': table_name
        })
        self._sql_tokens.append(select_sql)


class QuerySet:
    def __init__(self, query):
        if not isinstance(query, Query):
            raise ValueError(f"{query} should be an instance of Query")

        self.query = query
        self.result_cache = []

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
        except:
            return None

    def __iter__(self):
        self.load_cache()
        for item in self.result_cache:
            yield item

    def load_cache(self):
        if not self.result_cache:
            # TODO: Run the query only when the
            # queryset is evaluated as oppposed
            # to running it in the methods: filters etc.
            self.query.run()
            self.result_cache = self.query.result_cache

    def exclude(self, **kwargs):
        pass

    def order_by(self, *fields):
        ascending_fields = set()
        descending_fields = set()
        for field in fields:
            if field.startswith('-'):
                field = field.removeprefix('-')
                descending_fields.add(field)
            else:
                ascending_fields.add(field)

        # There might a case where the result_cache
        # is not yet loaded especially using
        # chained statements
        # e.g. table.annotate().order_by()
        # In that specific case, the QuerySet
        # of annotate would have cache
        # Solution 2: Delegate the execution
        # of the final query from annotate
        # to the query of order_by in that
        # sense we would not execute two
        # different queries but just one
        # single one modified
        self.load_cache()

        previous_sql = self.query._backend.de_sqlize_statement(self.query._sql)
        ascending_statements = [
            self.query._backend.ASCENDING.format_map({'field': field})
            for field in ascending_fields
        ]
        descending_statements = [
            self.query._backend.DESCENDNIG.format_map({'field': field})
            for field in descending_fields
        ]
        final_statement = ascending_statements + descending_statements
        order_by_clause = self.query._backend.ORDER_BY.format_map({
            'conditions': self.query._backend.comma_join(final_statement)
        })
        sql = [previous_sql, order_by_clause]
        new_query = self.query.create(
            self.query._backend,
            sql,
            table=self.query._table
        )
        # new_query.run()
        # return QuerySet(new_query)
        # return new_query.result_cache
        return QuerySet(new_query)

    def values(self, *fields):
        self.load_cache()
        return_values = []
        if not fields:
            fields = self.query._table.field_names

        for row in self.result_cache:
            result = {}
            for field in fields:
                result[field] = row[field]
            return_values.append(result)
        return return_values
