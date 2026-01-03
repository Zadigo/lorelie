import datetime
import pathlib
import sqlite3
from typing import Any, Optional, Set

import pytz

from lorelie import converters
from lorelie import registry
from lorelie.database.functions.aggregation import (CoefficientOfVariation,
                                                    MeanAbsoluteDifference,
                                                    StDev, Variance)
from lorelie.database.functions.text import MD5Hash, RegexSearch, SHA1Hash, SHA224Hash, SHA256Hash, SHA384Hash, SHA512Hash
from lorelie.database.manager import ForeignTablesManager
from lorelie.database.nodes import (DeleteNode, SelectNode, UpdateNode,
                                    WhereNode)
from lorelie.exceptions import ConnectionExistsError
from lorelie.expressions import Q
from lorelie.lorelie_typings import (TypeDatabase, TypeRow, TypeSQLiteBackend,
                                     TypeStrOrPathLibPath, TypeTable)
from lorelie.queries import Query, QuerySet
from lorelie.database.expressions.mixins import SQL


class Connections:
    """A class that remembers the different 
    connections that were created to the
    SQLite database"""

    connections_map: dict[str, TypeSQLiteBackend] = {}
    created_connections: Set[TypeSQLiteBackend] = set()

    def __repr__(self):
        return f'<Connections: count={len(self.connections_map.keys())}>'

    def __getitem__(self, name):
        return self.connections_map[name]

    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self):
        return False

    def get_last_connection(self):
        """Return the last connection from the
        connection pool"""
        try:
            return list(self.created_connections)[-1]
        except IndexError:
            raise ConnectionExistsError()

    def register(self, connection: TypeSQLiteBackend, name: Optional[str] = None):
        if name is None:
            name = 'default'

        self.connections_map[name] = connection
        self.created_connections.add(connection)


connections = Connections()


class BaseRow:
    """Adds additional functionalities to
    the default SQLite `Row`. Rows allows the data 
    that comes from the database to be interfaced adding
    extra python functionnalities to the generic value

    >>> row = database.objects.get(firstname='Kendall')

    The value of a column can get retrieved as:

    >>> row['rowid']
    ... 1

    >>> row.firstname
    ... "Kendall"

    Or changed using __setitem__ as:

    >>> row.firstname = 'Julie'
    ... row.save()

    Args:
        fields: The list of fields that are part of the row
        data: The actual data that is part of the row
        cursor: The sqlite3 cursor that generated the row

    Returns:
        BaseRow: An instance of the BaseRow class
    """

    def __init__(self, fields: list[str], data: dict, cursor: Optional[sqlite3.Cursor] = None):
        # Indicate that this specific row
        # values have been changed and could
        # eligible for saving
        self.mark_for_update = False
        self.cursor = cursor
        self._fields = fields
        self._cached_data = data
        self._backend: TypeSQLiteBackend = connections.get_last_connection()

        table = getattr(self._backend, 'current_table', None)
        self.linked_to_table: Optional[str] = getattr(table, 'name', None)

        self.updated_fields: dict[str, Any] = {}
        self.pk: Optional[int] = data.get('rowid', data.get('id', None))

        for key, value in self._cached_data.items():
            setattr(self, key, value)

    def __repr__(self):
        self._backend.set_current_table_from_row(self)
        # By default, show the rowid or id in the representation
        # of the value a given column e.g. <id: 1> which can
        # be changed for example to <id: Kendall Jenner> if the
        # user chooses to use that column to represent the column
        try:
            str_field = self._backend.current_table.str_field
        except:
            str_field = 'pk'

            is_type_index = self._cached_data.get('type') == 'index'
            if is_type_index:
                str_field = 'name'

        is_type_column = 'cid' in self._fields
        if is_type_column:
            str_field = 'type'

        # The rowid is not necessarily implemented by default in the
        # created sqlite tables. Hence why we test for the id field
        name_to_show = getattr(self, str_field, None)
        if name_to_show is None:
            # There might be situations where the user
            # restricts the amount of fields to return
            # from the database which will return None
            # when trying to get str_field. So get the
            # first field from the list of fields
            name_to_show = getattr(self, self._fields[-0])

        if self.linked_to_table is not None and 'sqlite_' in self.linked_to_table:
            return f'<SQLITE: {name_to_show}>'

        return f'<{self.linked_to_table.title()}: {name_to_show}>'

    def __setitem__(self, name, value):
        self.mark_for_update = True
        # We don't really care if the user
        # sets a field that does not actually
        # exist on the database. We'll simply
        # invalidate the field in the final SQL
        self.updated_fields[name] = value
        self.__dict__[name] = value

    def __getitem__(self, name):
        # It seems like when an ID field
        # is specified as primary key, the
        # RowID
        if name == 'rowid':
            return self.pk
        value = getattr(self, name)
        # Before returning the value,
        # get the field responsible for
        # converting said value to a Python
        # usable object
        return value

    def __hash__(self):
        values = list(map(lambda x: getattr(self, x, None), self._fields))
        return hash((self.pk, *values))

    def __contains__(self, value):
        return value in self._cached_data.values()

    # def __getattr__(self, key: str) -> Union[Any, ForeignTablesManager]:
    #     # TODO: Improve foreign key relations handling
    #     if key.endswith('_rel'):
    #         backend = self.__dict__['_backend']
    #         right_table_name, _ = key.split('_')
    #         manager = ForeignTablesManager(
    #             right_table_name,
    #             backend.current_table
    #         )
    #         setattr(manager, 'current_row', self)
    #         return manager
    #     return key

    def foreign_key(self, table_name: str) -> ForeignTablesManager:
        """Returns the foreign key manager for a given
        table name"""

    def save(self):
        """Changes the data on the actual row
        by calling `save_row_object`

        >>> row = database.objects.last('my_table')
        ... row.name = 'Kendall'
        ... row.save()
        ... row['name'] = 'Kylie'
        ... row.save()
        """
        try:
            self._backend.save_row_object(self)
        except AttributeError:
            raise ExceptionGroup(
                'Row does not seem to be affiliated to a table and database.',
                [
                    Exception('Could not save row object')
                ]
            )
        else:
            self.updated_fields.clear()
        finally:
            self.mark_for_update = False

    def delete(self):
        """Deletes the row from the database

        >>> row = database.objects.last('my_table')
        ... row.delete()
        """
        try:
            self._backend.delete_row_object(self)
        except AttributeError:
            raise ExceptionGroup(
                'Row does not seem to be affiliated to a table',
                [
                    Exception('Could not delete row object')
                ]
            )
        else:
            return self

    def refresh_from_database(self):
        """This function is designed to update the object's data 
        with the latest values from the database. This is useful when 
        the column values have changed in the database, and you want to 
        ensure that your object reflects these changes."""
        table = registry.known_tables[self.linked_to_table]

        select_node = SelectNode(table, *self._fields, limit=1)
        where_node = WhereNode(id=self.pk)

        query_class = Query(table=table)
        query_class.add_sql_nodes([select_node, where_node])
        query_class.run()

        refreshed_row = query_class.result_cache[0]
        for field in self._fields:
            new_value = getattr(refreshed_row, field)
            setattr(self, field, new_value)
            self._cached_data[field] = new_value
        return self


def row_factory(backend: TypeSQLiteBackend):
    """Base function for generation custom SQLite Row
    that implements additional functionnalities on the 
    results of the database. This function overrides the 
    default class used for the data in the database."""
    def inner_factory(cursor: sqlite3.Cursor, row):
        fields = [column[0] for column in cursor.description]
        data = {key: value for key, value in zip(fields, row)}
        return BaseRow(fields, data, cursor=cursor)
    return inner_factory


class SQLiteBackend(SQL):
    """A class that wraps the sqlite3 backend and adds additional
    functionalities to it.

    Args:
        database_or_name: Either the database instance or the name of the database to connect to.
        log_queries: Whether to log the queries that are executed on this backend.
        mask_values: Whether to mask sensitive values in the logs on insert operations. Defaults to False.

    Returns:
        SQLiteBackend: An instance of the SQLiteBackend class

    Examples:
        >>> connection = SQLiteBackend('my_database', log_queries=True)
        >>> connection = SQLiteBackend(pathlib.Path('/path/to/database.sqlite'), log_queries=True)
        >>> connection = SQLiteBackend(database_instance, log_queries=True)
    """

    def __init__(self, database_or_name: Optional[TypeDatabase | TypeStrOrPathLibPath] = None, log_queries: bool = False, mask_values: bool = False):
        self.database_name: Optional[str] = None
        self.database_path: Optional[pathlib.Path] = None
        self.database_instance: Optional[TypeDatabase] = None
        self.connection_timestamp = datetime.datetime.now().timestamp()
        self.in_memory_connection: bool = False
        self.mask_values = mask_values

        sqlite3.register_converter('date', converters.convert_date)
        sqlite3.register_converter('datetime', converters.convert_datetime)
        sqlite3.register_converter('timestamp', converters.convert_timestamp)
        sqlite3.register_converter('boolean', converters.convert_boolean)

        params = {
            'check_same_thread': False,
            'autocommit': True,
            'detect_types': sqlite3.PARSE_DECLTYPES
        }

        if isinstance(database_or_name, str):
            self.database_name = database_or_name
            self.database_path = pathlib.Path(
                database_or_name).with_suffix('.sqlite')
            connection = sqlite3.connect(self.database_path, **params)
        elif isinstance(database_or_name, pathlib.Path):
            if database_or_name.is_dir():
                raise ValueError(
                    "Path should be a path to "
                    f"a database file: {database_or_name}"
                )

            if database_or_name.suffix != '.sqlite':
                database_or_name = database_or_name.with_suffix('.sqlite')

            self.database_path = database_or_name
            self.database_name = database_or_name.stem
            connection = sqlite3.connect(self.database_path, **params)
        elif hasattr(database_or_name, 'database_name'):
            name: str = getattr(database_or_name, 'database_name', None)
            path: pathlib.Path = getattr(database_or_name, 'path', None)
            self.database_instance = database_or_name

            if name is not None:
                self.database_name = name
                self.database_path = path.joinpath(name).with_suffix('.sqlite')
                connection = sqlite3.connect(self.database_path, **params)
            else:
                # This means we are creating an in-memory
                # database connection
                self.in_memory_connection = True
                self.database_name = ':memory:'
                connection = sqlite3.connect(':memory:', **params)
        else:
            # This means we are creating an in-memory
            # database connection
            self.in_memory_connection = True
            self.database_name = ':memory:'
            connection = sqlite3.connect(':memory:', **params)

        MD5Hash.create_function(connection)
        SHA1Hash.create_function(connection)
        SHA224Hash.create_function(connection)
        SHA256Hash.create_function(connection)
        SHA384Hash.create_function(connection)
        SHA512Hash.create_function(connection)
        RegexSearch.create_function(connection)

        MeanAbsoluteDifference.create_function(connection)
        Variance.create_function(connection)
        StDev.create_function(connection)
        CoefficientOfVariation.create_function(connection)

        connection.row_factory = row_factory(self)

        self.connection = connection
        self.current_table: Optional[TypeTable] = None
        self.log_queries = log_queries

        connections.register(self, name=self.database_name)

    def __hash__(self):
        return hash((self.database_name))

    def set_current_table(self, table: TypeTable):
        """Track the current table that is being updated
        or queried at the connection level for other parts
        of the project that require this knowledge"""
        if self.current_table is None:
            self.current_table = table
        elif self.current_table != table:
            self.current_table = table

    def set_current_table_from_row(self, row: TypeRow):
        """Sets the current table using the table name that
        is attached to the row if current table is None 
        otherwhise skip this action"""
        if self.current_table is None:
            if 'sqlite_' in row.linked_to_table:
                return

            self.current_table = self.database_instance.get_table(
                row.linked_to_table
            )

    def list_table_columns(self, table: TypeTable):
        query = Query(table=table)
        query.map_to_sqlite_table = True
        query.add_sql_node(f'pragma table_info({table.name})')
        return QuerySet(query)

    def create_table_fields(self, table: TypeTable, columns_to_create):
        field_params = []
        if columns_to_create:
            while columns_to_create:
                column_to_create = columns_to_create.pop()
                field = table.fields_map[column_to_create]
                field_params.append(field.field_parameters())

            statements = [self.simple_join(param) for param in field_params]
            for i, statement in enumerate(statements):
                if i > 1:
                    statement = f'add table {statement}'
                statements[i] = statement

            alter_sql = self.ALTER_TABLE.format_map({
                'table': table.name,
                'params': self.simple_join(statements)
            })
            query = Query(table=table)
            query.add_sql_nodes([alter_sql])
            query.run(commit=True)

    def list_all_tables(self):
        select_clause = self.SELECT.format(
            fields=self.comma_join(['rowid', 'name']),
            table='sqlite_schema'
        )
        not_like_clause = self.NOT_LIKE.format(
            field='name',
            wildcard=self.quote_value('sqlite_%')
        )
        where_clause = self.WHERE_CLAUSE.format(
            params=self.simple_join([
                self.EQUALITY.format(
                    field='type',
                    value=self.quote_value('table')
                ),
                self.AND.format(rhv=not_like_clause)
            ])
        )
        query = Query(backend=self)
        query.map_to_sqlite_table = True
        query.add_sql_nodes([select_clause, where_clause])
        return QuerySet(query)

    def list_database_indexes(self):
        base_fields = ['type', 'name', 'tbl_name', 'sql']
        select_sql = self.SELECT.format_map({
            'fields': self.comma_join(base_fields),
            'table': 'sqlite_master'
        })
        where_clause = self.WHERE_CLAUSE.format_map({
            'params': self.EQUALITY.format_map({
                'field': 'type',
                'value': self.quote_value('index')
            })
        })
        query = Query(backend=self)
        query.map_to_sqlite_table = True
        query.add_sql_nodes([select_sql, where_clause])
        return QuerySet(query, skip_transform=True)

    def list_table_indexes(self, table: TypeTable):
        # sql = f'PRAGMA index_list({self.quote_value(table.name)})'
        sql = f'PRAGMA index_list({table.name})'
        query = Query(table=table)
        query.map_to_sqlite_table = True
        query.add_sql_node(sql)
        return QuerySet(query)

    def save_row_object(self, row: TypeRow):
        """Creates the SQL statement required for
        saving a row in the database
        """
        self.set_current_table_from_row(row)

        # TODO: Centralize the update of auto update
        # fields on the table level if possible instead
        # of having them all around the application
        if self.current_table.auto_update_fields:
            value = str(datetime.datetime.now(tz=pytz.UTC))
            for field in self.current_table.auto_update_fields:
                row.updated_fields.update({field: value})

        update_node = UpdateNode(
            self.current_table,
            row.updated_fields,
            Q(id=row.id)
        )

        query = Query(backend=self)
        query.add_sql_node(update_node)
        query.run(commit=True)
        return query

    def delete_row_object(self, row: TypeRow):
        """Creates the SQL statement required for
        deleting a row in the database
        """
        delete_node = DeleteNode(
            self.current_table,
            id=row.id
        )

        query = Query(backend=self)
        query.add_sql_node(delete_node)
        query.run(commit=True)
        return query
