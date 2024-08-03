import sqlite3
from functools import total_ordering
from sqlite3 import IntegrityError, OperationalError

from lorelie import log_queries, lorelie_logger
from lorelie.database.nodes import (BaseNode, OrderByNode, SelectMap,
                                    SelectNode, WhereNode)


class Query:
    """This class represents an SQL statement query and is responsible for executing 
    the query on the database. It handles the retrieval of data, which is then stored 
    in the `result_cache` attribute for subsequent use. 

    The class offers methods for executing multiple queries against the database, running scripts, 
    and preparing statements before sending them to the database. It also includes functionality 
    to transform the retrieved data into Python objects.
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

        self.backend.set_current_table(table)
        self.sql = None
        self.result_cache = []
        self.alias_fields = []
        self.is_evaluated = False
        self.statements = []
        self.select_map = SelectMap()
        # Since this is a special table that was not created
        # locally, we need to indicate to the __repr__ of the
        # rows that they will not be able to use the current_table
        # property to get the table name
        self.map_to_sqlite_table = False

    def __repr__(self):
        return f'<{self.__class__.__name__} [{self.sql}]>'

    @classmethod
    def create(cls, table=None, backend=None):
        """Creates a new `Query` class to be executed"""
        return cls(table=table, backend=backend)

    @classmethod
    def run_script(cls, backend=None, table=None, sql_tokens=[]):
        template = 'begin; {statements} commit;'
        instance = cls(table=table, backend=backend)

        statements = []
        for token in sql_tokens:
            if not isinstance(token, str):
                token = token.as_sql(backend)
            finalized_token = instance.backend.finalize_sql(token)
            statements.append(finalized_token)

        joined_statements = instance.backend.simple_join(statements)
        script = template.format(statements=joined_statements)
        instance.add_sql_node(script)

        try:
            result = instance.backend.connection.executescript(script)
        except OperationalError as e:
            print(e, script)
            raise
        except IntegrityError as e:
            print(e, script)
            raise
        except Exception as e:
            print(e, script)
            raise
        else:
            # print(script)
            instance.backend.connection.commit()
            instance.result_cache = list(result)
            instance.is_evaluated = True
        finally:
            log_queries.append(script, table=table, backend=backend)

            # Logging should not be set to True
            #  in a production environment since there
            # could be sensitive data passed in the queries
            # to the log. Warn the user about this
            if instance.backend.log_queries:
                lorelie_logger.warning(
                    "Logging queries in a production environment is high risk "
                    "and should be disabled. Logging sensitive data in to a log"
                    "file can cause severe security issues to you data"
                )
                for query in log_queries:
                    lorelie_logger.info(f"\"{query}\"")

            return instance

    @property
    def return_single_item(self):
        return self.result_cache[-0]

    def add_sql_node(self, node):
        if not isinstance(node, (BaseNode, str)):
            raise ValueError(
                f"{node} should be an instance "
                "of BaseNode or <str>"
            )

        if isinstance(node, BaseNode):
            if node.node_name == 'order_by':
                if self.select_map.order_by is not None:
                    node = self.select_map.order_by & node
            # FIXME: Other nodes are added to the select map?
            self.select_map[node.node_name] = node

        self.statements.append(node)

    def add_sql_nodes(self, nodes):
        if not isinstance(nodes, list):
            raise ValueError(
                f"{nodes} should be an instance "
                " of list or tuple"
            )

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
        # print(self.sql)
        try:
            result = self.backend.connection.execute(self.sql)
        except OperationalError as e:
            print(e, self.sql)
            raise
        except IntegrityError as e:
            print(e, self.sql)
            raise
        except Exception as e:
            print(e, self.sql)
            raise
        else:
            if commit:
                self.backend.connection.commit()

            self.result_cache = list(result)

            # Since some tables are not created locally,
            # we need to indicate to the __repr__ of the
            # rows that they will not be able to use the
            # current_table of the backend property to get
            # the table name
            if self.map_to_sqlite_table:
                updated_rows = []
                for row in self.result_cache:
                    setattr(row, 'linked_to_table', 'sqlite_schema')
                    updated_rows.append(row)
                self.result_cache = updated_rows

            self.is_evaluated = True
        finally:
            log_queries.append(
                self.sql,
                table=self.table,
                backend=self.backend
            )

            # Logging should not be set to True
            #  in a production environment since there
            # could be sensitive data passed in the queries
            # to the log. Warn the user about this
            if self.backend.log_queries:
                lorelie_logger.warning(
                    "Logging queries in a production environment is high risk "
                    "and should be disabled. Logging sensitive data in to a log"
                    "file can cause severe security issues to you data"
                )
                for query in log_queries:
                    try:
                        lorelie_logger.info(f"\"{query}\"")
                    except:
                        lorelie_logger.warning('Could not log query')

    def transform_to_python(self):
        """Transforms the values returned by the
        database into Python objects"""
        from lorelie.fields.base import AliasField
        for row in self.result_cache:
            # The sqlite_schema is not created
            # locally so no sense to do a transform
            if self.map_to_sqlite_table:
                continue

            for name in row._fields:
                value = row[name]
                if name in self.alias_fields:
                    instance = AliasField(name)
                    field = instance.get_data_field(value)
                elif name.endswith('_id'):
                    # TODO: Deal with related name
                    # fields e.g. products_id. For
                    # now just return the id of the
                    # related field
                    return row[name]
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
        if not self.fields:
            self.fields = list(self.queryset.query.table.field_names)

        if self.queryset.query.alias_fields:
            self.fields = list(self.fields) + \
                list(self.queryset.query.alias_fields)

        for row in self.queryset:
            result = {}
            for field in self.fields:
                result[field] = row[field]
            yield result


@total_ordering
class EmptyQuerySet:
    def __repr__(self):
        return '<Queryset []>'

    def __len__(self):
        return 0

    def __contains__(self):
        return False

    def __iter__(self):
        return []

    def __eq__(self):
        return False

    def __gt__(self):
        return False

    def __gte__(self):
        return False


class QuerySet:
    """Represents a set of results obtained from executing an SQL query. 
    It provides methods for manipulating and retrieving data from the database
    """

    def __init__(self, query, skip_transform=False):
        if not isinstance(query, Query):
            raise ValueError(f"'{query}' should be an instance of Query")

        self.query = query
        self.result_cache = []
        self.values_iterable_class = ValuesIterable
        # There are certain cases where we want
        # to use QuerySet but it's not affialiated
        # to any table ex. returning a QuerySet of
        # table indexes. This allows to skip the
        # python transform of the data
        self.skip_transform = skip_transform
        # Methods that require a commit to return
        # objects (get_or_create etc.) can indicate
        # to the QuerySet that commits needs to be
        # set to True
        self.use_commit = False
        # Flag which can be used to indicate
        # to the QuerySet to use an alias view
        # to query items from the database as
        # oppposed to using the table name
        self.alias_view_name = None
        # Despite existing data in the cache,
        # force the cache to be reloaded from
        # the existing database
        self.force_reload_cache = False

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
        except (KeyError, IndexError):
            return None

    def __iter__(self):
        self.load_cache()
        for item in self.result_cache:
            yield item

    def __contains__(self, value):
        self.load_cache()
        return any(map(lambda x: value in x, self.result_cache))

    def __eq__(self, value):
        self.load_cache()
        if not isinstance(value, QuerySet):
            return NotImplemented
        return value == self

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

    def check_alias_view_name(self):
        if self.alias_view_name is not None:
            # Replace the previous SelectNode
            # with the new one by using the
            # previous parameters of the
            # previous SelectNode
            old_select = self.query.select_map.select
            new_node = SelectNode(
                self.query.table,
                *old_select.fields,
                distinct=old_select.distinct,
                limit=old_select.limit,
                view_name=self.alias_view_name
            )
            self.query.select_map.select = new_node
            self.force_reload_cache = True
            return True
        return False

    def load_cache(self):
        if self.force_reload_cache or not self.result_cache:
            self.query.run(commit=self.use_commit)
            if not self.skip_transform:
                self.query.transform_to_python()
            self.result_cache = self.query.result_cache

    def first(self):
        self.query.select_map.limit = 1
        self.query.select_map.order_by = OrderByNode(self.query.table, 'id')
        return self[-1]

    def last(self):
        self.query.select_map.limit = 1
        self.query.select_map.order_by = OrderByNode(self.query.table, '-id')
        return self

    def all(self):
        self.check_alias_view_name()
        return self

    def filter(self, *args, **kwargs):
        backend = self.query.backend
        filters = backend.decompose_filters(**kwargs)
        build_filters = backend.build_filters(filters, space_characters=False)

        self.check_alias_view_name()
        if self.query.select_map.should_resolve_map:
            try:
                # Try to update and existing where
                # clause otherwise create a new one
                self.query.select_map.where(*args, **kwargs)
            except TypeError:
                self.query.select_map.where = WhereNode(*args, **kwargs)
        return QuerySet(self.query)

    def get(self, *args, **kwargs):
        if not args and not kwargs:
            queryset = QuerySet(self.query)
        else:
            try:
                self.query.select_map.where(*args, **kwargs)
            except:
                self.query.select_map.where = WhereNode(*args, **kwargs)
            else:
                queryset = QuerySet(self.query)

        if len(queryset) > 1:
            raise ValueError("Queryset returned multiple values")

        if not queryset:
            return None

        return queryset[-0]

    def annotate(self, **kwargs):
        pass

    def values(self, *fields):
        return list(self.values_iterable_class(self, fields=fields))

    def dataframe(self, *fields):
        import pandas
        return pandas.DataFrame(self.values(*fields))

    def order_by(self, *fields):
        orderby_node = OrderByNode(self.query.table, *fields)
        if not self.query.is_evaluated:
            self.query.add_sql_node(orderby_node)
        return self.__class__(self.query)

    def aggregate(self, *args, **kwargs):
        for func in args:
            allows_aggregate = getattr(func, 'allow_aggregation')
            if not allows_aggregate:
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
        backend = self.query.backend
        columns, values = backend.dict_to_sql(kwargs)

        conditions = []
        for i, column in enumerate(columns):
            conditions.append(backend.EQUALITY.format_map({
                'field': column,
                'value': values[i]
            }))

        joined_conditions = backend.comma_join(conditions)

        update_sql = backend.UPDATE.format_map({
            'table': self.query.table.name
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
        query = backend.current_table.query_class(table=self.query.table)
        query.add_sql_nodes(update_sql_tokens)
        query.run(commit=True)
        return query.result_cache

    def exists(self):
        return len(self) > 0
