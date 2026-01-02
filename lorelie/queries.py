import sqlite3
from functools import total_ordering
from sqlite3 import IntegrityError, OperationalError
from typing import Any, Iterator, Optional, Type
from lorelie import log_queries, lorelie_logger
from lorelie.database.nodes import (AnnotationMap, BaseNode, OrderByNode, SelectMap,
                                    SelectNode, WhereNode)
from lorelie.lorelie_typings import TypeDatabaseManager, TypeExpression, TypeFunction, TypeNode, TypeQuerySet, TypeRow, TypeSQLiteBackend, TypeTable, TypeQuery


class Query:
    """This class represents an SQL statement query and is responsible for executing 
    the query on the database. It handles the retrieval of data, which is then stored 
    in the `result_cache` attribute for subsequent use. 

    The class offers methods for executing multiple queries against the database, running scripts, 
    and preparing statements before sending them to the database. It also includes functionality 
    to transform the retrieved data into Python objects.

    Args:
        table (TypeTable, optional): The table associated with the query. Defaults to None.
        backend (TypeSQLiteBackend, optional): The database backend to use. Defaults to None.
    """

    def __init__(self, table: Optional[TypeTable] = None, backend: Optional[TypeSQLiteBackend] = None):
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
        self.sql: Optional[str] = None
        self.result_cache: list[TypeRow] = []
        # Alias fields are fields that do not exist
        # in the table and are created virtually. They need
        # to be tracked so that during the transform_to_python
        # method we can transform them properly
        self.annotation_map: Optional[AnnotationMap] = None
        self.is_evaluated: bool = False
        self.statements: list[str] = []
        self.select_map: SelectMap = SelectMap()
        # Since this is a special table that was not created
        # locally, we need to indicate to the __repr__ of the
        # rows that they will not be able to use the current_table
        # property to get the table name
        self.map_to_sqlite_table: bool = False
        self.is_transactional: bool = False

    def __repr__(self):
        return f'<{self.__class__.__name__} [{self.sql}]>'

    @classmethod
    def create(cls, table: TypeTable = None, backend: TypeSQLiteBackend = None):
        """Creates a new `Query` class to be executed"""
        return cls(table=table, backend=backend)

    @classmethod
    def run_transaction(cls, backend: Optional[TypeSQLiteBackend] = None, table: Optional[TypeTable] = None, sql_tokens: list[TypeNode | str] = []):
        """Runs a script made of multiple sql statements

        Args:
            backend (TypeSQLiteBackend, optional): The database backend to use. Defaults to None.
            table (TypeTable, optional): The table associated with the query. Defaults to None.
            sql_tokens (list, optional): A list of SQL statements or tokens to execute. Defaults to [].
            callback (Callable[[str], None], optional): A callback function to be called after execution. Defaults to None.
        """
        template = 'begin; {statements} commit;'
        instance = cls(table=table, backend=backend)
        # Ensure that all the statements terminate with a `;`
        # since we are going to use a transaction script
        instance.statements = [
            instance.backend.finalize_sql(token)
            for token in sql_tokens
        ]

        instance.pre_sql_setup()
        script = template.format(statements=instance.sql)

        # Clear previous individual statements
        # in order to keep the SQL script only
        instance.statements = []
        instance.add_sql_node(script)

        try:
            cursor = instance.backend.connection.executescript(script)
        except OperationalError as e:
            lorelie_logger.error(e, exc_info=True)
            raise
        except IntegrityError as e:
            lorelie_logger.error(e, exc_info=True)
            raise
        except Exception as e:
            lorelie_logger.error(e, exc_info=True)
            raise
        else:
            instance.backend.connection.commit()
            instance.result_cache = list(cursor)
            instance.is_evaluated = True
        finally:
            instance.is_transactional = True
            log_queries.mask_values = instance.backend.mask_values
            log_queries.append(script, table=table, backend=backend)

            # Logging should not be set to True
            #  in a production environment since there
            # could be sensitive data passed in the queries
            # to the log. Warn the user about this
            if instance.backend.log_queries:
                for query in log_queries:
                    lorelie_logger.info(f"▶️ \"{query}\"")

            return instance

    @property
    def return_single_item(self):
        return self.result_cache[-0]

    def add_sql_node(self, node: BaseNode | str):
        if not isinstance(node, (BaseNode, str)):
            raise ValueError(
                f"{node} should be an instance "
                "of BaseNode or str"
            )

        if isinstance(node, BaseNode):
            if node.node_name == 'order_by':
                if self.select_map.order_by is not None:
                    node = self.select_map.order_by & node
            # FIXME: Other nodes are added to the select map?
            self.select_map[node.node_name] = node

        self.statements.append(node)

    def add_sql_nodes(self, nodes: list[TypeNode | str]):
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
            print(e, 'SQL:', self.sql)
            raise
        except IntegrityError as e:
            print(e, 'SQL:', self.sql)
            raise
        except Exception as e:
            print(e, 'SQL:', self.sql)
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
                # updated_rows = []
                for row in self.result_cache:
                    setattr(row, 'linked_to_table', 'sqlite_schema')
                #     updated_rows.append(row)
                # self.result_cache = updated_rows

            self.is_evaluated = True
        finally:
            log_queries.mask_values = self.backend.mask_values
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
                    "🟠 Logging queries in a production environment is high risk "
                    "and should be disabled. Logging sensitive data in to a log"
                    "file can cause severe security issues to you data"
                )
                lorelie_logger.warning(
                    "🟠 Logging sensitive data in to a log"
                    "file can cause severe security issues to you data"
                )

                for query in log_queries:
                    lorelie_logger.info(f"▶️ \"{query}\"")

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

                if self.annotation_map is not None and name in self.annotation_map.alias_fields:
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

    def __init__(self, queryset: TypeQuerySet, fields: list[str] = []):
        self.queryset = queryset
        self.fields = fields

    def __repr__(self):
        return f'<ValuesIterable {list(self.__iter__())}>'

    def __iter__(self):
        if not self.fields:
            self.fields = list(self.queryset.query.table.field_names)

        if self.queryset.query.annotation_map.alias_fields:
            self.fields = (
                list(self.fields) +
                list(self.queryset.query.annotation_map.alias_fields)
            )

        for row in self.queryset:
            result: dict[str, Any] = {}

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

    Args:
        query (Query): The Query object associated with the QuerySet.
        skip_transform (bool, optional): Whether to skip transforming the data to Python objects. Defaults
    """

    def __init__(self, query: TypeQuery, skip_transform: bool = False):
        if not isinstance(query, Query):
            raise ValueError(f"'{query}' should be an instance of Query")

        self.query = query
        self.result_cache: list[TypeRow] = []
        self.values_iterable_class: Type[ValuesIterable] = ValuesIterable
        # There are certain cases where we want
        # to use QuerySet but it's not affiliated
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
        self.alias_view_name: Optional[str] = None
        # Despite existing data in the cache,
        # force the cache to be reloaded from
        # the existing database
        self.force_reload_cache = False

    # def __repr__(self):
    #     # NOTE: In test environments, sometimes
    #     # the __repr__ is called before any of the other
    #     # methods since the VS Code shows the class representation
    #     # in the variable explorer. Therefore, if test results
    #     # can be confusing because of that. Disable __repr__
    #     # calls in test environments.
    #     self.load_cache()
    #     return f'<{self.__class__.__name__} {self.result_cache}>'

    def __str__(self):
        self.load_cache()
        return str(self.result_cache)

    def __getitem__(self, index: int) -> TypeRow | None:
        self.load_cache()
        try:
            return self.result_cache[index]
        except (KeyError, IndexError):
            return None

    def __iter__(self) -> Iterator[TypeRow]:
        self.load_cache()
        for item in self.result_cache:
            yield item

    def __contains__(self, value: Any):
        self.load_cache()
        return any(map(lambda x: value in x, self.result_cache))

    def __eq__(self, value: Any):
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
            # Transaction queries do no return data
            # so in that case, we need use a different
            # query to load the underlying data
            if self.query.is_evaluated and self.query.is_transactional:
                new_query = Query(
                    table=self.query.table,
                    backend=self.query.backend
                )
                new_query.add_sql_node(
                    SelectNode(self.query.table, '*')
                )
                new_query.run()
                self.query = new_query

            self.query.run(commit=self.use_commit)
            if not self.skip_transform:
                self.query.transform_to_python()
            self.result_cache = self.query.result_cache

    def get_master_queryset(self) -> TypeQuerySet:
        # This technique allows us to get the main master queryset
        # without evaluaing it. It populates the SelectMap. This allows then
        # allows us to apply modificatons on the undeerlying query before it is evaluated
        master_objects: TypeDatabaseManager = getattr(
            self.query.table,
            'objects'
        )
        return master_objects.all()

    def first(self) -> TypeRow | None:
        master_qs = self.get_master_queryset()

        self.query.select_map.limit = 1

        other_by_node = OrderByNode(self.query.table, 'id')
        self.query.select_map.add_ordering(other_by_node)
        return self[0]

    def last(self) -> TypeRow | None:
        master_qs = self.get_master_queryset()

        self.query.select_map.limit = 1

        other_by_node = OrderByNode(self.query.table, '-id')
        self.query.select_map.add_ordering(other_by_node)
        return self[0]

    def all(self):
        # self.check_alias_view_name()
        # return self
        return self.get_master_queryset()

    def filter(self, *args: TypeExpression, **kwargs: TypeExpression):
        master_objects: TypeDatabaseManager = getattr(
            self.query.table,
            'objects'
        )

        if self.query.select_map.where is not None:
            # Triggered by qs.filter(...).filter(...)
            self.query.select_map.where(*args, **kwargs)
        else:
            # Triggered by qs.all().filter(...)
            master_qs = master_objects.all()
            where_node = WhereNode(*args, **kwargs)
            self.query.select_map.add_where(where_node)
        self.force_reload_cache = True
        return self

    def get(self, *args, **kwargs) -> TypeRow | None:
        _self = self.filter(*args, **kwargs)
        if _self.count() > 1:
            raise ValueError("get() returned more than one row")
        return _self[0]

    def annotate(self, *args: TypeFunction, **kwargs: TypeFunction):
        # Triggered by qs.all().annotate(...) or
        # qs.filter(...).annotate(...) or qs.all().annotate(...)
        master_objects: TypeDatabaseManager = getattr(
            self.query.table,
            'objects'
        )
        if self.query.annotation_map is None:
            return master_objects.annotate(*args, **kwargs)
        else:
            other_qs = master_objects.annotate(*args, **kwargs)
            _, _, fields, _ = other_qs.query.select_map.select.deconstruct()
            # We need to recreate a new SelectNode alltohether
            # by combining the fields a fake non-evaluated
            # quereset (other_qs) and the current one (self.query)
            new_fields = set(fields + self.query.select_map.select.fields)

            new_select = SelectNode(
                self.query.table,
                *new_fields,
            )
            new_annotation_map = other_qs.query.annotation_map & self.query.annotation_map
            self.query.annotation_map = new_annotation_map
            self.query.select_map.select = new_select
        return self

    def values(self, *fields):
        return self.values_iterable_class(self, fields=fields)

    def get_dataframe(self, *fields):
        import pandas
        return pandas.DataFrame(self.values(*fields))

    def order_by(self, *fields: str):
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
