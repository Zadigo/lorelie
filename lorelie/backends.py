import dataclasses
import datetime
import itertools
import pathlib
import re
import sqlite3
from collections import defaultdict
from dataclasses import field
from typing import Any, Optional, Sequence, Set

import pytz

from lorelie import converters
from lorelie.database import registry
from lorelie.database.expression_filters import ExpressionFiltersMixin
from lorelie.database.functions.aggregation import (CoefficientOfVariation,
                                                    MeanAbsoluteDifference,
                                                    StDev, Variance)
from lorelie.database.functions.text import MD5Hash, SHA256Hash
from lorelie.database.manager import ForeignTablesManager
from lorelie.database.nodes import (DeleteNode, SelectNode, UpdateNode,
                                    WhereNode)
from lorelie.exceptions import ConnectionExistsError
from lorelie.expressions import Q
from lorelie.lorelie_typings import (TypeDatabase, TypeLogicalOperators,
                                     TypeRow, TypeSQLiteBackend,
                                     TypeStrOrPathLibPath, TypeTable)
from lorelie.queries import Query, QuerySet


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

    def __getattr__(self, key):
        # TODO: Improve foreign key relations handling
        if key.endswith('_rel'):
            backend = self.__dict__['_backend']
            right_table_name, _ = key.split('_')
            manager = ForeignTablesManager(
                right_table_name,
                backend.current_table
            )
            setattr(manager, 'current_row', self)
            return manager
        return key

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


@dataclasses.dataclass
class AnnotationMap:
    sql_statements_dict: dict = field(default_factory=dict)
    alias_fields: list = field(default_factory=list)
    field_names: list = field(default_factory=list)
    annotation_type_map: dict = field(default_factory=dict)

    @property
    def joined_final_sql_fields(self):
        statements = []
        for alias, sql in self.sql_statements_dict.items():
            if self.annotation_type_map[alias] == 'Case':
                statements.append(f'{sql}')
                continue
            statements.append(f'{sql} as {alias}')
        return list(itertools.chain(statements))

    @property
    def requires_grouping(self):
        values = list(self.annotation_type_map.values())
        return any([
            'Count' in values,
            'Length' in values
        ])


class SQL(ExpressionFiltersMixin):
    """Base SQL compiler"""

    ALTER_TABLE = 'alter table {table} add column {params}'
    CREATE_TABLE = 'create table if not exists {table} ({fields})'
    CREATE_INDEX = 'create index {name} on {table} ({fields})'
    DROP_TABLE = 'drop table if exists {table}'
    DROP_INDEX = 'drop index if exists {value}'
    DELETE = 'delete from {table}'
    INSERT = 'insert into {table} ({fields}) values({values})'
    SELECT = 'select {fields} from {table}'
    UPDATE = 'update {table}'
    UPDATE_SET = 'set {params}'
    REPLACE = 'replace into {table} ({fields}) values({values})'

    AND = 'and {rhv}'
    OR = 'or {rhv}'

    CONDITION = '{field}{operator}{value}'
    EQUALITY = '{field}={value}'
    LIKE = '{field} like {conditions}'
    BETWEEN = '{field} between {lhv} and {rhv}'
    IN = '{field} in ({values})'
    NOT_LIKE = '{field} not like {wildcard}'
    WHERE_CLAUSE = 'where {params}'
    WHERE_NOT = 'where not ({params})'

    WILDCARD_MULTIPLE = '%'
    WILDCARD_SINGLE = '_'

    ASCENDING = '{field} asc'
    DESCENDING = '{field} desc'

    ORDER_BY = 'order by {conditions}'
    GROUP_BY = 'group by {conditions}'

    AVERAGE = 'avg({field})'
    COUNT = 'count({field})'
    STRFTIME = 'strftime({format}, {value})'

    CHECK_CONSTRAINT = 'check({conditions})'

    CASE = 'case {field} {conditions} end {alias}'
    WHEN = 'when {condition} then {value}'

    LIMIT = 'limit {value}'
    OFFSET = 'offset {value}'

    SQL_REGEXES = [
        re.compile(r'^select\s(.*)\sfrom\s(.*)\s(where)?\s(.*);?$')
    ]

    @staticmethod
    def quote_value(value: Any):
        """Quote a given value to be used in an SQL statement.
        Numbers are not quoted. None is converted to ''."""
        if value is None:
            return "''"

        if callable(value):
            value = value()

        if isinstance(value, (list, tuple)):
            value = str(value)

        if isinstance(value, (int, float)):
            return value

        if isinstance(value, str) and value.startswith("'"):
            return value

        # To handle special characters like
        # single quotes ('), we have to escape
        # them by doubling them up for the final
        # sql string
        if isinstance(value, str) and "'" in value:
            value = value.replace("'", "''")

        # By default, quote the value
        return f"'{value}'"

    @staticmethod
    def comma_join(values: list[Any]):
        def check_value_type(value):
            if callable(value):
                return str(value())

            if isinstance(value, (int, float, list, tuple)):
                return str(value)
            return value

        return ', '.join(map(check_value_type, values))

    @staticmethod
    def operator_join(values: list[str], operator: TypeLogicalOperators = 'and'):
        """Joins a set of values using a valid
        operator: and, or

        >>> self.operator_join(["name='Kendall'", "surname='Jenner'"])
        ... "name='Kendall' and surname='Jenner'"
        """
        return f' {operator} '.join(values)

    @staticmethod
    def simple_join(values: list[str | Any], space_characters: bool = True):
        """Joins a set of tokens with a simple space

        >>> self.simple_join(["select * from table", "where name = 'Kendall'"])
        ... "select * from table where name = 'Kendall'"
        """
        def check_integers(value):
            if isinstance(value, (int, float)):
                return str(value)

            if getattr(value, 'to_python', None) is not None:
                return value.to_python()
            return value
        values = map(check_integers, values)

        if space_characters:
            return ' '.join(values)
        return ''.join(values)

    @staticmethod
    def finalize_sql(sql: str):
        """Ensures that a statement ends
        with `;` to be considered valid"""
        if sql.endswith(';'):
            return sql
        return f'{sql};'

    @staticmethod
    def de_sqlize_statement(sql: str):
        """Returns an sql statement without 
        `;` at the end"""
        if sql.endswith(';'):
            return sql.removesuffix(';')
        return sql

    @staticmethod
    def wrap_parenthentis(value: str):
        """Wraps a given value in parenthentis"""
        return f"({value})"

    @staticmethod
    def build_alias(condition: str, alias: str):
        """Returns the alias statement for a given sql
        statement like in `count(name) as top_names`"""
        return f'{condition} as {alias}'

    def build_dot_notation(self, values: Sequence[Sequence[str]]):
        """Transforms a set of values to a valid
        SQL dot notation

        >>> self.build_dot_notation([('followers', 'id', '=', '1')])
        ... 'followers.id = 1'
        """
        notations = []
        for sub_items in values:
            if not isinstance(sub_items, (list, tuple)):
                raise ValueError(
                    f"Expected list or array. Got: {sub_items}"
                )

            # The tuple needs exactly four
            # elements in order to create a
            # valid notation: el1.el2=el4
            # the operator is el3
            if len(sub_items) != 4:
                raise ValueError(
                    f"Invalid number of items in '{sub_items}' "
                    "in order to create a valid dot notation"
                )

            dot_notation = []
            for i, sub_value in enumerate(sub_items):
                # As soon as we get the operator, stop the
                # dotting. Get the remaing items from the
                # array that were not parsed starting from
                # the iteration index i
                if sub_value in list(self.base_filters.values()):
                    remaining_bits = list(sub_items[i:])
                    remaining_bits[-1] = self.quote_value(sub_items[-1])
                    dot_notation.extend(remaining_bits)
                    break

                if i > 0:
                    dot_notation.append('.')
                dot_notation.append(sub_value)

            final_notation = self.simple_join(
                dot_notation,
                space_characters=False
            )
            notations.append(final_notation)
        return notations

    def parameter_join(self, data: dict[str, Any]):
        """Takes a list of fields and values
        and returns string of key/value parameters
        ready to be used in an sql statement

        >>> self.parameter_join(['firstname', 'lastname'], ['Kendall', 'Jenner'])
        ... "firstname='Kendall', lastname='Jenner'"
        """
        fields, values = self.dict_to_sql(data)
        result = []
        for i, field in enumerate(fields):
            equality = self.EQUALITY.format(field=field, value=values[i])
            result.append(equality)
        return self.comma_join(result)

    def quote_values(self, values: list[Any]):
        """Quotes multiple values at once"""
        return list(map(lambda x: self.quote_value(x), values))

    def quote_startswith(self, value: str):
        """Creates a startswith wildcard and returns
        the quoted condition

        >>> self.quote_startswith(self, 'kendall')
        ... "'kendall%'"
        """
        value = str(value) + '%'
        return self.quote_value(value)

    def quote_endswith(self, value: str):
        """Creates a endswith wildcard and returns
        the quoted condition

        >>> self.quote_endswith(self, 'kendall')
        ... "'%kendall'"
        """
        value = '%' + str(value)
        return self.quote_value(value)

    def quote_like(self, value: str):
        """Creates a like wildcard and returns
        the quoted condition

        >>> self.quote_like(self, 'kendall')
        ... "'%kendall%'"
        """
        value = f'%{value}%'
        return self.quote_value(value)

    def dict_to_sql(self, data: dict[str, Any], quote_values: bool = True):
        """Convert a dictionnary containing a key
        pair of columns and values into a tuple
        of columns and value list. The values from
        the dictionnary are quoted by default

        >>> self.dict_to_sql({'name': 'Kendall'})
        ... (['name'], ["'Kendall'"])
        """
        fields = list(data.keys())
        if quote_values:
            quoted_value = list(
                map(
                    lambda x: self.quote_value(x),
                    data.values()
                )
            )
            return fields, quoted_value
        return fields, data.values()

    def build_script(self, *sqls: str):
        return '\n'.join(map(lambda x: self.finalize_sql(x), sqls))

    def build_annotation(self, conditions):
        """For each database function, creates a special
        statement as in `count(column_name) as my_special_name`.
        If we have Count function in our conditions, we have to
        identify it since it requires a special """
        annotation_map = AnnotationMap()

        for alias, func in conditions.items():
            annotation_map.alias_fields.append(alias)
            annotation_map.annotation_type_map[alias] = func.__class__.__name__
            sql_resolution = func.as_sql(self.current_table.backend)

            # Expressions return an array. Maybe in
            # future iterations we should normalize
            # returning a string or ensure that functions
            # return a list
            if isinstance(sql_resolution, list):
                sql_resolution = self.comma_join(sql_resolution)

            annotation_map.sql_statements_dict[alias] = sql_resolution

        # TODO: Functions returns strings and expressions
        # returns arrays - We need to normalize that.
        # TODO: Also CombinedExpressions vs Functions
        # TODO: Put resolve_expression to detect expressions
        # and resolve_functions to detect functions
        return annotation_map

    def decompose_sql_statement(self, sql: str):
        regexes = {
            'select': {
                'columns': re.compile(r'^select\s(.*)\sfrom'),
                'table': re.compile(r'from\s(\w+)'),
                'where': re.compile(r'where\s(.*)\s?'),
                'group_by': re.compile(r'group\sby\s(.*)\s?'),
                'order_by': re.compile(r'order\sby\s(.*)\s?'),
                'limit': re.compile(r'limit\s(\d+)\s?')
            }
        }

        @dataclasses.dataclass
        class StatementMap:
            columns: list = dataclasses.field(default_factory=list)
            table: str = None
            where: list = dataclasses.field(default_factory=list)
            group_by: list = dataclasses.field(default_factory=list)
            order_by: list = dataclasses.field(default_factory=list)
            limit: int = None

            def __setitem__(self, key, value):
                setattr(self, key, value)

        sql_bits = defaultdict(list)
        for name, values in regexes.items():
            if not name in sql:
                continue

            bits = sql_bits[name]
            for key, regex in values.items():
                result = regex.match(sql)

                if not result:
                    result = regex.search(sql)

                if not result:
                    continue

                statement_map = StatementMap()
                tokens = result.group(1).split(',')
                tokens = list(map(lambda x: x.strip(), tokens))

                statement_map[key] = tokens
                bits.append((key, tokens))
        return sql_bits


class SQLiteBackend(SQL):
    """Class that initiates and encapsulates a
    new connection to an sqlite database. The connection
    can be in memory or to a physical database

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

    def __init__(self, database_or_name: Optional[TypeDatabase | TypeStrOrPathLibPath] = None, log_queries: bool = False):
        self.database_name: Optional[str] = None
        self.database_path: Optional[pathlib.Path] = None
        self.database_instance: Optional[TypeDatabase] = None
        self.connection_timestamp = datetime.datetime.now().timestamp()
        self.in_memory_connection: bool = False
        self.mask_values = mask_values

        # sqlite3.register_adapter(datetime.datetime.now, str)
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
        SHA256Hash.create_function(connection)
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
