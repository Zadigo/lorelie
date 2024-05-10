import dataclasses
import itertools
import re
import sqlite3
from dataclasses import field

from lorelie.expressions import Case
from lorelie.aggregation import Count, Avg
from lorelie.functions import ExtractDay, ExtractMonth, ExtractYear, Length, Lower, Upper
from lorelie.queries import Query


class Connections:
    """A class that remembers the different 
    connections that were created to the
    SQLite database"""

    connections_map = {}
    created_connections = set()

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
        connection map"""
        return list(self.created_connections)[-1]

    def register(self, connection, name=None):
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

    def __init__(self, cursor, fields, data):
        # Indicate that this specific row
        # values have been changed and could
        # eligible for saving
        self.mark_for_update = False
        self._cursor = cursor
        self._fields = fields
        self._cached_data = data
        self._backend = connections.get_last_connection()
        self.linked_to_table = self._backend.current_table.name
        self.updated_fields = {}

        for key, value in self._cached_data.items():
            setattr(self, key, value)

    def __repr__(self):
        # By default, show the rowid or id in the representation
        # of the value a given column e.g. <id: 1> which can
        # be changed for example to <id: Kendall Jenner> if the
        # user chooses to use that column to represent the column
        str_field = self._backend.current_table.str_field or 'id'
        # The rowid is not necessarily implemented by default in the
        # sqlite database. Hence why we test for the id field
        name_to_show = getattr(self, 'rowid', getattr(self, str_field, None))
        return f'<{self._backend.current_table.verbose_name}: {name_to_show}>'

    def __setitem__(self, name, value):
        # Before saving the item to the database,
        # call the field responsible for setting
        # the value to a database usable object
        # table_field = self._backend.current_table.get_field(name)
        # value = table_field.to_database(value)

        self.mark_for_update = True
        # We don't really care if the user
        # sets a field that does not actually
        # exist on the database. We'll simply
        # invalidate the field in the final SQL
        # setattr(self, name, value)
        self.updated_fields[name] = [name, value]
        self.__dict__[name] = value

    def __getitem__(self, name):
        # It seems like when an ID field
        # is specified as primary key, the
        # RowOD
        if name == 'rowid':
            if 'rowid' not in self._cached_data:
                return None

        value = getattr(self, name)
        # Before returning the value,
        # get the field responsible for
        # converting said value to a Python
        # usable object
        return value

    # FIXME: When trying to set this up
    # we get a recursion error for
    # whatever reasons
    def __setattr__(self, name, value):
        # if name in self._fields:
        #     self.mark_for_update = True
        #     self.updated_fields[name] = [name, value]
        #     self.__dict__[name] = value
        super().__setattr__(name, value)

    def __hash__(self):
        return hash((self.rowid))

    def __contains__(self, value):
        # Check that a value exists in
        # in all the values of the row
        truth_array = []
        for item in self._cached_data.values():
            if item is None:
                truth_array.append(False)
                continue

            if isinstance(item, int):
                item = str(item)

            truth_array.append(value in item)
        return any(truth_array)

    def __eq__(self, value):
        return any((self[key] == value for key in self._fields))

    def save(self):
        """Changes the data on the actual row
        by calling `save_row_object`

        >>> row = database.objects.last('my_table')
        ... row.name = 'Kendall'
        ... row.save()
        ... row['name'] = 'Kylie'
        ... row.save()
        """
        self._backend.save_row_object(self)
        self.updated_fields.clear()
        return self

    def delete(self):
        """Deletes the row from the database

        >>> row = database.objects.last('my_table')
        ... row.delete()
        """
        self._backend.delete_row_object(self)
        return self


def row_factory(backend):
    """Base function for generation custom SQLite Row
    that implements additional functionnalities on the 
    results of the database. This function overrides the 
    default class used for the data in the database."""
    def inner_factory(cursor, row):
        fields = [column[0] for column in cursor.description]
        data = {key: value for key, value in zip(fields, row)}
        return BaseRow(cursor, fields, data)
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


class SQL:
    """Base sql compiler"""

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
    DESCENDNIG = '{field} desc'

    ORDER_BY = 'order by {conditions}'
    GROUP_BY = 'group by {conditions}'
    # UNIQUE_INDEX = 'create unique index {name} ON {table}({fields})'

    LOWER = 'lower({field})'
    UPPER = 'upper({field})'
    LENGTH = 'length({field})'
    MAX = 'max({field})'
    MIN = 'min({field})'
    AVERAGE = 'avg({field})'
    COUNT = 'count({field})'

    STRFTIME = 'strftime({format}, {value})'

    CHECK_CONSTRAINT = 'check ({conditions})'

    CASE = 'case {field} {conditions} end {alias}'
    WHEN = 'when {condition} then {value}'

    LIMIT = 'limit {value}'

    SQL_REGEXES = [
        re.compile(r'^select\s(.*)\sfrom\s(.*)\s(where)?\s(.*);?$')
    ]

    base_filters = {
        'eq': '=',
        'lt': '<',
        'gt': '>',
        'lte': '<=',
        'gte': '>=',
        'contains': 'like',
        'startswith': 'startswith',
        'endswith': 'endswith',
        'range': 'between',
        'ne': '!=',
        'in': 'in',
        'isnull': 'isnull'
    }

    @staticmethod
    def quote_value(value):
        if isinstance(value, int):
            return value

        if value.startswith("'"):
            return value
        return f"'{value}'"

    @staticmethod
    def comma_join(values):
        def check_integers(value):
            if isinstance(value, (int, float)):
                return str(value)
            return value
        values = map(check_integers, values)
        return ', '.join(values)

    @staticmethod
    def operator_join(values, operator='and'):
        """Joins a set of values using a valid
        operator: and, or

        >>> self.operator_join(["name='Kendall'", "surname='Jenner'"])
        ... "name='Kendall' and surname='Jenner'"
        """
        return f' {operator} '.join(values)

    @staticmethod
    def simple_join(values, space_characters=True):
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
    def finalize_sql(sql):
        """Ensures that a statement ends
        with `;` to be considered valid"""
        if sql.endswith(';'):
            return sql
        return f'{sql};'

    @staticmethod
    def de_sqlize_statement(sql):
        """Returns an sql statement without 
        `;` at the end"""
        if sql.endswith(';'):
            return sql.removesuffix(';')
        return sql

    @staticmethod
    def wrap_parenthentis(value):
        return f"({value})"

    @staticmethod
    def build_alias(condition, alias):
        """Returns the alias statement for a given sql
        statement like in `count(name) as top_names`"""
        return f'{condition} as {alias}'

    def quote_startswith(self, value):
        """Creates a startswith wildcard and returns
        the quoted condition

        >>> self.quote_startswith(self, 'kendall')
        ... "'kendall%'"
        """
        value = value + '%'
        return self.quote_value(value)

    def quote_endswith(self, value):
        """Creates a endswith wildcard and returns
        the quoted condition

        >>> self.quote_endswith(self, 'kendall')
        ... "'%kendall'"
        """
        value = '%' + value
        return self.quote_value(value)

    def quote_like(self, value):
        """Creates a like wildcard and returns
        the quoted condition

        >>> self.quote_like(self, 'kendall')
        ... "'%kendall%'"
        """
        value = f'%{value}%'
        return self.quote_value(value)

    def dict_to_sql(self, data, quote_values=True):
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
                map(lambda x: self.quote_value(x), data.values()))
            return fields, quoted_value
        else:
            return fields, data.values()

    def build_script(self, *sqls):
        return '\n'.join(map(lambda x: self.finalize_sql(x), sqls))

    def decompose_filters_columns(self, value):
        """Return only the column parts of the filters
        that were passed

        >>> self.decompose_filters_from_string('rowid__eq')
        ... ['rowid']
        """
        if isinstance(value, str):
            result = self.decompose_filters_from_string(value)
        elif isinstance(value, dict):
            result = self.decompose_filters(**value)
        return list(map(lambda x: x[0], result))

    def decompose_filters_from_string(self, value):
        """Decompose a set of filters to a list of
        key, operator and value list from a string

        >>> self.decompose_filters_from_string('rowid__eq=1')
        ... [('rowid', '=', '1')]
        """
        identified_operator = False
        operators = self.base_filters.keys()
        for operator in operators:
            operator_to_identify = f'__{operator}='
            if operator_to_identify in value:
                identified_operator = True
                break

        if not identified_operator:
            # Use regex to identify the unique
            # possible existing pattern if the
            # user does not use a "__" filter
            result = re.match(r'\w+\=', value)
            if not result:
                raise ValueError('Could not identify the condition')

        tokens = value.split('=')
        return self.decompose_filters(**{tokens[0]: tokens[1]})

    def decompose_filters(self, **kwargs):
        """Decompose a set of filters to a list of
        key, operator and value list from a dictionnary

        >>> self.decompose_filters(rowid__eq=1)
        ... [('rowid', '=', '1')]
        """
        filters_map = []
        for key, value in kwargs.items():
            if '__' not in key:
                key = f'{key}__eq'

            tokens = key.split('__', maxsplit=1)
            if len(tokens) > 2:
                raise ValueError(f'Filter is not valid. Got: {key}')

            lhv, rhv = tokens
            operator = self.base_filters.get(rhv)
            if operator is None:
                raise ValueError(
                    f'Operator is not recognized. Got: {key}'
                )
            filters_map.append((lhv, operator, value))
        return filters_map

    def build_filters(self, items, space_characters=True):
        """Tranform a list of decomposed filters to
        usable string conditions for an sql statement

        >>> self.build_filters([('rowid', '=', '1')])
        ... ["rowid = '1'"]

        >>> self.build_filters([('rowid', 'startswith', '1')])
        ... ["rowid like '1%'"]


        >>> self.build_filters([('url', '=', Lower('url')])
        ... ["lower(url)"]
        """
        built_filters = []
        for item in items:
            field, operator, value = item

            # TODO: Implement a check tha raises an
            # error if the operator is not a valid
            # existing one aka: =,!=, <,>,<=,>=,in,
            # endswith, startswith, between, isnull, like

            if operator == 'in':
                # TODO: Allow the user to check that a
                # value would also exist in a queryset or
                # an iterable in general
                if not isinstance(value, (tuple, list)):
                    raise ValueError(
                        'The value when using "in" '
                        'should be a tuple or a list'
                    )

                quoted_list_values = (self.quote_value(item) for item in value)
                operator_and_value = self.IN.format_map({
                    'field': field,
                    'values': self.comma_join(quoted_list_values)
                })
                built_filters.append(operator_and_value)
                continue

            if operator == 'like':
                operator_and_value = self.LIKE.format_map({
                    'field': field,
                    'conditions': self.quote_like(value)
                })
                built_filters.append(operator_and_value)
                continue

            if operator == 'startswith':
                operator_and_value = self.LIKE.format_map({
                    'field': field,
                    'conditions': self.quote_startswith(value)
                })
                built_filters.append(operator_and_value)
                continue

            if operator == 'endswith':
                operator_and_value = self.LIKE.format_map({
                    'field': field,
                    'conditions': self.quote_endswith(value)
                })
                built_filters.append(operator_and_value)
                continue

            if operator == 'between':
                lhv, rhv = value
                operator_and_value = self.BETWEEN.format_map({
                    'field': field,
                    'lhv': lhv,
                    'rhv': rhv
                })
                built_filters.append(operator_and_value)
                continue

            if operator == 'isnull':
                if value:
                    operator_and_value = f'{field} is null'
                else:
                    operator_and_value = f'{field} is not null'
                built_filters.append(operator_and_value)
                continue

            value = self.quote_value(value)
            built_filters.append(
                self.simple_join(
                    (field, operator, value),
                    space_characters=space_characters
                )
            )
        return built_filters

    def build_annotation(self, **conditions):
        """For each database function, creates a special
        statement as in `count(column_name) as my_special_name`.
        If we have Count function in our conditions, we have to
        identify it since it requires a special """
        annotation_map = AnnotationMap()

        for alias_name, function in conditions.items():
            annotation_map.alias_fields.append(alias_name)
            annotation_map.annotation_type_map[alias_name] = function.__class__.__name__
            if isinstance(function, Case):
                function.alias_name = alias_name
                case_sql = function.as_sql(self.current_table.backend)
                annotation_map.sql_statements_dict[alias_name] = case_sql
            
            if isinstance(function, (Count, Avg, Length, Lower, Upper, ExtractYear, ExtractDay, ExtractMonth)):
                annotation_map.field_names.append(
                    function.field_name
                )
                annotation_map.sql_statements_dict[alias_name] = function.as_sql(
                    self
                )

        return annotation_map


class SQLiteBackend(SQL):
    """Class that initiates and encapsulates a
    new connection to the database"""

    def __init__(self, database_name=None):
        from lorelie.functions import Hash

        if database_name is None:
            database_name = ':memory:'
        else:
            database_name = f'{database_name}.sqlite'
        self.database_name = database_name

        connection = sqlite3.connect(database_name)
        connection.create_function('hash', 1, Hash.create_function())
        connection.row_factory = row_factory(self)

        self.connection = connection
        self.current_table = None

        connections.register(self, name=database_name)

    def set_current_table(self, table):
        """Track the current table that is being updated
        or queried at the connection level for other parts
        of the project that require this knowledge"""
        self.current_table = table

    def list_table_columns_sql(self, table):
        sql = f'pragma table_info({table.name})'
        query = Query(self, [sql], table=table)
        query.run()
        return query.result_cache

    def drop_indexes_sql(self, row):
        sql = self.DROP_INDEX.format_map({
            'value': row['name']
        })
        return sql

    def create_table_fields(self, table, columns_to_create):
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
            query = Query(self, [alter_sql], table=table)
            query.run(commit=True)

    def list_tables_sql(self):
        sql = self.SELECT.format(
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
        query = Query([sql, where_clause], backend=self)
        query.run()
        return query.result_cache

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
        query = Query([select_sql, where_clause], backend=self)
        query.run()
        return query.result_cache

    def list_table_indexes(self, table):
        # sql = f'PRAGMA index_list({self.quote_value(table.name)})'
        sql = f'PRAGMA index_list({table.name})'
        query = Query(self, [sql], table=table)
        query.run()
        return query.result_cache

    def save_row_object(self, row):
        """Creates the SQL statement required for
        saving a row in the database
        """
        if self.current_table.auto_update_fields:
            value = str(datetime.datetime.now(tz=pytz.UTC))
            for field in self._backend.current_table.auto_update_fields:
                row[field] = value

        fields_to_set = []
        for _, values in row.updated_fields.items():
            lhv, rhv = values
            equality_statement = self.EQUALITY.format_map({
                'field': lhv,
                'value': self.quote_value(rhv)
            })
            fields_to_set.append(equality_statement)

        conditions = self.comma_join(fields_to_set)
        update_set = self.UPDATE_SET.format_map({
            'params': conditions
        })

        fields_to_set = self.comma_join(fields_to_set)
        update_sql = self.UPDATE.format_map({
            'table': self.current_table.name,
            'params': fields_to_set
        })
        where_sql = self.WHERE_CLAUSE.format_map({
            'params': self.EQUALITY.format_map({
                'field': 'id',
                'value': row.id
            })
        })

        query = Query([update_sql, update_set, where_sql], backend=self)
        query.run(commit=True)
        return query

    def delete_row_object(self, row):
        """Creates the SQL statement required for
        deleting a row in the database
        """
        delete_sql = self.DELETE.format_map({
            'table': self.current_table.name
        })
        where_sql = self._backend.WHERE_CLAUSE.format_map({
            'params': self.EQUALITY.format_map({
                'field': 'id',
                'value': row.id
            })
        })

        query = Query([delete_sql, where_sql], backend=self)
        query.run(commit=True)
        return query
