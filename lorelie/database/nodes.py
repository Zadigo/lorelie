"""
The logic behind this test is instead of using string
values for all the SQL values as we do, we use Python
classes (called nodes) to construct the sql bits.

Nodes can be constructed as:
    complexnode = node1 + node2

Or:

    RawSQL(backend, node1, node2)

RawSQL is node aggregator that can be used to concatenate
multiple nodes together
"""


import dataclasses
import itertools
from dataclasses import field
import re
from abc import ABC, abstractmethod
from typing import Any, ClassVar, Generic, Optional, override

from lorelie.expressions import CombinedExpression, Q
from lorelie.lorelie_typings import (NodeEnums, TypeJoinTypes, TypeNode,
                                     TypeQuerySet, TypeSQLiteBackend,
                                     TypeTable)


@dataclasses.dataclass
class SelectMap:
    """A map used in Query that resolves the correct
    positions for the different parameters
    within a select sql statement:

        1. "SELECT" + "FROM"
        2. "WHERE"
        3. "GROUP BY" + "HAVING"
        4. "ORDER BY"
        5. "LIMIT" + "OFFSET"
    """

    # The node used to complete the dataclass fields
    select: Optional['SelectNode'] = None
    where: Optional['WhereNode'] = None
    order_by: Optional['OrderByNode'] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    groupby: Optional[str] = None
    having: Optional[str] = None

    def __setitem__(self, name: str,  value: str):
        setattr(self, name, value)

    @property
    def should_resolve_map(self):
        return all([
            self.select is not None,
            self.select == 'select'
        ])

    def resolve(self, backend: TypeSQLiteBackend):
        nodes: list[str] = []
        nodes.extend(self.select.as_sql(backend))

        if self.where is not None:
            nodes.extend(self.where.as_sql(backend))

        if self.groupby is not None:
            nodes.extend([self.groupby])

            if self.having is not None:
                nodes.extend([self.having])

        if self.order_by is not None:
            nodes.extend(self.order_by.as_sql(backend))

        if self.limit is not None:
            limit = self.limit or getattr(self, 'limit', self.select.limit)
            nodes.extend([backend.LIMIT.format_map({'value': limit})])

            if self.select.offset is not None:
                offset = (
                    self.offset or
                    getattr(self, 'offset', self.select.offset)
                )
                nodes.extend([
                    backend.OFFSET.format_map({
                        'value': offset
                    })
                ])

        return nodes

    def add_where(self, other: 'WhereNode'):
        """Add a where node to the select map. If a where node
        already exists, we combine them using the `+` operator
        which represents an AND operation.
        """
        if not isinstance(other, WhereNode):
            raise ValueError()

        if self.where is None:
            self.where = other
        else:
            self.where = self.where + other

    def add_ordering(self, other: 'OrderByNode'):
        """Add an order by node to the select map. If an order by node
        already exists, we combine them using the `&` operator
        which represents a merging of the ordering fields."""
        if not isinstance(other, OrderByNode):
            raise ValueError()

        if self.order_by is None:
            self.order_by = other
        else:
            self.order_by = self.order_by & other


@dataclasses.dataclass
class AnnotationMap:
    """Annotation map that tracks the different
    sql statements and their alias fields just like
    SelectMap does for select statements"""

    sql_statements_dict: dict = field(default_factory=dict)
    alias_fields: list = field(default_factory=list)
    field_names: list = field(default_factory=list)
    annotation_type_map: dict = field(default_factory=dict)

    def __and__(self, other: 'AnnotationMap'):
        if not isinstance(other, AnnotationMap):
            return NotImplemented

        combined = AnnotationMap()

        combined.sql_statements_dict = {
            **self.sql_statements_dict,
            **other.sql_statements_dict
        }

        combined.alias_fields = list(
            itertools.chain(
                self.alias_fields, 
                other.alias_fields
            )
        )

        combined.field_names = list(
            itertools.chain(
                self.field_names, 
                other.field_names
            )
        )

        combined.annotation_type_map = {
            **self.annotation_type_map,
            **other.annotation_type_map
        }

        return combined

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


class RawSQL:
    def __init__(self, backend: TypeSQLiteBackend, *nodes: TypeNode):
        for node in nodes:
            if not isinstance(node, (BaseNode, str)):
                raise ValueError('RawSQL only accepts BaseNode or str types')

        self.nodes = list(nodes)
        self.backend = backend

        self.resolve_select = 'select' in self.nodes[0]
        self.select_map = SelectMap()

        if self.resolve_select:
            for node in nodes:
                if self.select_map.order_by is not None:
                    self.select_map.order_by = self.select_map.order_by & node
                    continue
                self.select_map[node.node_name] = node

    def __repr__(self):
        return str(self.as_sql())

    def __str__(self):
        return self.backend.simple_join(self.as_sql())

    def __iter__(self):
        return iter(self.as_sql())

    def __eq__(self, value):
        if isinstance(value, RawSQL):
            value = str(value)
        return value == self.__str__()

    @property
    def can_resolve(self):
        if self.nodes:
            # The first node of an SQL statement
            # is always one of the expected nodes
            first_node = self.nodes[0]
            expected_nodes = ['select', 'update', 'delete', 'create']
            is_valid = map(lambda x: first_node == x, expected_nodes)
            return any(is_valid)
        return False

    def as_sql(self):
        if self.resolve_select:
            return self.select_map.resolve(self.backend)

        sql: list[str] = []
        for node in self.nodes:
            sql.extend(node.as_sql(self.backend))
        return sql


class ComplexNode(Generic[TypeNode]):
    """A node that aggregates multiple nodes together

    Args:
        *nodes (TypeNode): The nodes to aggregate

    Example:
    >>> node1 = SelectNode(table, 'name')
    >>> node2 = WhereNode(name='Kendall')
    >>> complex_node = ComplexNode(node1, node2)
    ... complex_node.as_sql(connection)
    ... ["select name from table", "where name='Kendall'"]
    """

    def __init__(self, *nodes: TypeNode):
        self.nodes = list(nodes)

    def __repr__(self):
        return f'<ComplexNode: {self.nodes}>'

    def __add__(self, node):
        if not isinstance(node, BaseNode):
            return NotImplemented

        if isinstance(node, OrderByNode):
            self.nodes.append(node)
        return self

    def __contains__(self, node):
        return node in self.nodes

    def as_sql(self, backend: TypeSQLiteBackend):
        clean_nodes: list[TypeNode] = []

        # Merge all where nodes into a single one
        _where_nodes = filter(
            lambda node: node.node_name != NodeEnums.WHERE.value,
            self.nodes
        )
        base_node: TypeNode = None
        for i, node in enumerate(_where_nodes):
            if i == 0:
                base_node = node
                continue
            _, _, _, args, kwargs = node.deconstruct()
            base_node(*args, **kwargs)

        clean_nodes.append(base_node)
        return RawSQL(backend, *clean_nodes)


class BaseNode(ABC):
    template_sql: Optional[str] = None

    def __init__(self, table: Optional[TypeTable] = None, fields: list[str] = []):
        self.table = table
        self.fields = fields or ['*']

    def __repr__(self):
        return f'<{self.__class__.__name__}>'

    def __add__(self, node):
        if not isinstance(node, BaseNode):
            return NotImplemented
        return ComplexNode(self, node)

    def __eq__(self, node):
        name = node
        if isinstance(node, BaseNode):
            name = node.node_name

        if isinstance(name, NodeEnums):
            name = name.value

        return name == self.node_name

    def __contains__(self, value):
        name = self.node_name
        if isinstance(name, NodeEnums):
            name = name.value

        return value in name

    def __and__(self, node):
        return NotImplemented

    def __call__(self, *fields: str):
        return NotImplemented

    @property
    def node_name(self) -> str:
        return ''

    @abstractmethod
    def as_sql(self, backend: TypeSQLiteBackend) -> list[str]:
        raise NotImplemented

    def deconstruct(self) -> list[str]:
        return [
            self.__class__.__name__,
            None if self.table is None else self.table.name,
            self.fields
        ]


class SelectNode(BaseNode):
    template_sql: ClassVar[str] = 'select {fields} from {table}'

    def __init__(self, table: TypeTable, *fields: str, distinct: bool = False, limit: Optional[int] = None, offset: Optional[int] = None, view_name: Optional[str] = None):
        super().__init__(table=table, fields=fields)
        self.distinct = distinct
        # This parameter is implemented
        # afterwards on the SelectMap
        self.limit = limit
        self.offset = offset
        self.view_name = view_name

    def __call__(self, *fields: str, **kwargs):
        new_fields = self.fields.extend(fields)
        return self.__class__(self.table, *new_fields, **kwargs)

    @property
    def node_name(self):
        return NodeEnums.SELECT.value

    @override
    def as_sql(self, backend: TypeSQLiteBackend):
        select_sql = self.template_sql.format_map({
            'fields': backend.comma_join(self.fields),
            # We can query a table or view that was previously
            # created in the current database using View
            'table': self.view_name or self.table.name
        })

        if self.distinct:
            return [select_sql.replace('select', 'select distinct')]

        # Returns a regular select sql. The rest of the
        # parameters (where, order by, limit, offset)
        # are handled by the SelectMap in the Query class
        # before final sql generation
        return [select_sql]

    @override
    def deconstruct(self) -> tuple[str, str, tuple[str, ...], dict[str, Any]]:
        values = super().deconstruct()
        other_params = ({
            'distinct': self.distinct,
            'limit': self.limit,
            'offset': self.offset,
            'view_name': self.view_name
        })
        return values + [other_params]


class WhereNode(BaseNode):
    """
    >>> node = WhereNode(name='Kendall')
    ... node.as_sql(connection)
    ... "where name='Kendall'"

    `args` accepts a `Q` function as arguments:

    >>> node = WhereNode(Q(name='Kendall'))
    ... node.as_sql(connection)
    ... "where name='Kendall'"
    """

    template_sql: ClassVar[str] = 'where {params}'

    def __init__(self, *args: Q | CombinedExpression, **expressions: Any):
        self.expressions = expressions
        self.func_expressions = list(args)
        self.invert: bool = False
        super().__init__()

    def __call__(self, *args, **expressions: Any):
        self.expressions.update(expressions)
        self.func_expressions.extend(args)
        return self

    def __invert__(self):
        self.invert = True
        return self

    @property
    def node_name(self):
        return NodeEnums.WHERE.value

    @override
    def as_sql(self, backend: TypeSQLiteBackend):
        # First, resolve Q, CombinedExpression to
        # their SQL representation. They are more
        # complex SQL expressions
        resolved = []
        for func in self.func_expressions:
            if isinstance(func, (Q, CombinedExpression)):
                resolved.extend(func.as_sql(backend))

        # WhereNode(firstname=Q(firstname='Kendall')) is a useless
        # expression since Q already wraps firstname. There's no
        # sense then to accept Q expressions in the node
        for _, value in self.expressions.items():
            if isinstance(value, (Q, CombinedExpression)):
                raise ValueError(
                    f'{value} cannot be a Q or CombinedExpression value')

        # Resolve base expressions e.g. firstname__eq which
        # are "and" operations which go after the more complexe ones
        filters = backend.decompose_filters(**self.expressions)
        joined_filters = backend.build_filters(filters, space_characters=False)
        resolved.extend(joined_filters)

        joined_resolved = backend.operator_join(resolved)
        if self.invert:
            joined_resolved = f'not {joined_resolved}'

        where_clause = self.template_sql.format(params=joined_resolved)
        return [where_clause]

    def deconstruct(self) -> tuple[str, str, list[str], tuple[Q | CombinedExpression], dict[str, Any]]:
        values = super().deconstruct()
        return tuple(values + [self.func_expressions, self.expressions])


class OrderByNode(BaseNode):
    template_sql: ClassVar[str] = 'order by {fields}'

    def __init__(self, table: TypeTable, *fields: str):
        self.ascending: set[str] = set()
        self.descending: set[str] = set()
        super().__init__(table=table, fields=fields)

        for field in self.fields:
            if not isinstance(field, str):
                raise ValueError(
                    "Field should be of type <str>"
                )

            result = re.match(r'^(\-)?(\w+)$', field)
            if result:
                sign, name = result.groups()

                if (name in self.ascending or
                        name in self.descending):
                    raise ValueError(
                        "The field has been registered twice in "
                        "ascending and descending fields"
                    )

                if sign:
                    self.descending.add(name)
                else:
                    self.ascending.add(name)

        self.cached_fields = list(self.ascending.union(self.descending))

    def __hash__(self):
        return hash((self.node_name, *self.fields))

    def __and__(self, node):
        if not isinstance(node, OrderByNode):
            return NotImplemented

        other_fields = set(node.fields)
        other_fields.update(self.fields)
        return node.__class__(self.table, *list(other_fields))

    @property
    def node_name(self):
        return NodeEnums.ORDER_BY.value

    @staticmethod
    def construct_sql(backend: TypeSQLiteBackend, field: str, ascending: bool = True):
        if field == '*':
            return None

        if ascending:
            return backend.ASCENDING.format_map({'field': field})
        else:
            return backend.DESCENDING.format_map({'field': field})

    def as_sql(self, backend: TypeSQLiteBackend):
        ascending_fields = map(
            lambda x: self.construct_sql(backend, x),
            self.ascending
        )
        descending_fields = map(
            lambda x: self.construct_sql(backend, x, ascending=False),
            self.descending
        )
        conditions = list(ascending_fields) + list(descending_fields)
        fields = backend.comma_join(conditions)
        ordering_sql = backend.ORDER_BY.format_map({'conditions': fields})
        return [ordering_sql]

    def deconstruct(self) -> tuple[str, str, tuple[str, ...]]:
        return tuple(super().deconstruct())


class UpdateNode(BaseNode):
    """To update existing data in a table, you use SQLite 
    UPDATE statement. The following illustrates the syntax 
    of the UPDATE statement:

    >>> node = UpdateNode(table, {'name': 'Kendall'}, name='Kylie')
    ... node.as_sql(connection)
    ... ["update celebrities set name='Kendall'", "where name='Kylie'"]

    `where_expressions` can also be provided a key-value pair:

    >>> node = UpdateNode(table, {'name': 'Kendall'}, name='Kylie')

    `where_args` accepts `Q` functions as arguments:

    >>> node = UpdateNode(table, {'name': 'Kendall'}, Q(name='Kylie'))
    ... node.as_sql(connection)
    ... ["update celebrities set name='Kendall'", "where name='Kylie'"]

    If both `where_args` and `where_expressions` are provided:

    >>> ["update celebrities set name='Kendall'", "where name='Kylie' and name='Julie'"]

    Note: https://www.sqlitetutorial.net/sqlite-update/
    """

    template_sql = 'update {table} set {fields}'

    def __init__(self, table: TypeTable, update_defaults: dict, *where_args: Q, **where_expressions: dict[str, Any]):
        super().__init__(table=table)
        self.where_args = where_args
        self.where_expressions = where_expressions
        self.update_defaults = update_defaults

    @property
    def node_name(self):
        return NodeEnums.UPDATE.value

    def as_sql(self, backend: TypeSQLiteBackend):
        where_node = WhereNode(*self.where_args, **self.where_expressions)
        fields_to_set = backend.parameter_join(self.update_defaults)

        update_sql = self.template_sql.format_map({
            'table': self.table.name,
            'fields': fields_to_set
        })
        sql = [
            update_sql,
            *where_node.as_sql(backend)
        ]
        return sql


class DeleteNode(BaseNode):
    def __init__(self, table: TypeTable, *where_args: Q, order_by: list[str] = [], limit: Optional[int] = None, **where_expressions: dict[str, Any]):
        super().__init__(table=table)
        self.where_args = where_args
        self.where_expressions = where_expressions
        self.order_by = order_by
        self.limit = limit

    @property
    def node_name(self):
        return NodeEnums.DELETE.value

    def as_sql(self, backend: TypeSQLiteBackend):
        delete_sql = backend.DELETE.format_map({
            'table': self.table.name
        })
        where_node = WhereNode(*self.where_args, **self.where_expressions)
        sql = [
            delete_sql,
            *where_node.as_sql(backend)
        ]

        if self.order_by:
            order_by_node = OrderByNode(self.table, *self.order_by)
            sql.extend(order_by_node.as_sql(backend))

        if self.limit is not None:
            if not isinstance(self.limit, int):
                raise ValueError(f'{self.limit} should be an integer')
            sql.extend([f'limit {self.limit}'])
        return sql


class InsertNode(BaseNode):
    """This node allows the creation of the sql
    for insert one or multiple values in the database.

    Values can be inserted in batch using `batch_values` which
    is a list of dictionnaries or inserted as a single element
    using `insert_values`

    Note: https://www.sqlitetutorial.net/sqlite-insert/
    """

    template_sql: ClassVar[str] = 'insert into {table} ({columns}) values({values})'
    batch_insert_sql: ClassVar[str] = 'insert into {table} ({columns}) values {values}'

    def __init__(self, table: TypeTable, batch_values: list[dict] = [], insert_values: dict = {}, returning: list[str] = []):
        super().__init__(table=table)
        self.insert_values = insert_values
        self.returning = returning

        for item in batch_values:
            if not isinstance(item, dict):
                raise ValueError(
                    f"{item} should be a dictionnary"
                )
        self.batch_values = batch_values

    @property
    def node_name(self):
        return NodeEnums.INSERT.value

    def as_sql(self, backend: TypeSQLiteBackend):
        template = self.template_sql

        if self.batch_values:
            columns = self.batch_values[0].keys()

            values = []
            for item in self.batch_values:
                quoted_values = backend.quote_values(item.values())
                joined = backend.comma_join(quoted_values)
                values.append(f"({joined})")

            joined_values = backend.comma_join(values)
            template = self.batch_insert_sql
        else:
            columns, values = backend.dict_to_sql(self.insert_values)
            joined_values = backend.comma_join(backend.quote_values(values))

        insert_sql = template.format_map({
            'table': self.table.name,
            'columns': backend.comma_join(columns),
            'values': joined_values
        })
        sql = [insert_sql]

        if self.returning:
            sql.append(f'returning {backend.comma_join(self.returning)}')
        else:
            sql.append('returning id')
        return sql


class JoinNode(BaseNode):
    """Node used to create the SQL statement
    that allows foreign key joins"""

    template_sql: ClassVar[str] = '{join_type} join {table} on {condition}'

    cross_join: ClassVar[str] = 'cross join {field}'
    full_outer_join: ClassVar[str] = 'full outer join {table} using({field})'

    def __init__(self, table: TypeTable, relationship_map, join_type: TypeJoinTypes = 'inner'):
        super().__init__()

        accepted_joins: list[TypeJoinTypes] = [
            'inner', 'left', 'right', 'cross']
        if join_type not in accepted_joins:
            raise ValueError('Join type is not valid')

        self.join_type = join_type
        self.table = table
        self.relationship_map = relationship_map

    @property
    def node_name(self):
        return NodeEnums.JOIN.value

    def as_sql(self, backend: TypeSQLiteBackend):
        # if self.join_type == 'cross':
        #     return []

        # if self.join_type == 'full':
        #     return []

        condition = self.relationship_map.get_relationship_condition(
            self.table
        )
        condition = ' = '.join(condition)

        join_sql = self.template_sql.format_map({
            'join_type': self.join_type,
            'table': self.table,
            'condition': condition
        })
        return [join_sql]


class IntersectNode(BaseNode):
    template_sql: ClassVar[str] = '{0} intersect {1}'

    def __init__(self, left_select: 'SelectNode', right_select: 'SelectNode'):
        self.left_select = left_select
        self.right_select = right_select

    @property
    def node_name(self):
        return NodeEnums.INTERSECT.value

    def as_sql(self, backend: TypeSQLiteBackend):
        lhv = self.left_select.as_sql(backend)
        rhv = self.right_select.as_sql(backend)
        sql = self.template_sql.format(lhv[0], rhv[0])
        return [sql]


class ViewNode(Generic[TypeQuerySet], BaseNode):
    template_sql: ClassVar[str] = 'create view if not exists {name} as {select_node}'

    def __init__(self, name: str, queryset: TypeQuerySet, temporary: bool = False):
        self.name = name
        self.temporary = temporary
        self.queryset = queryset

    @property
    def node_name(self):
        return NodeEnums.VIEW.value

    def as_sql(self, backend: TypeSQLiteBackend):
        if not hasattr(self.queryset, 'load_cache'):
            raise ValueError(
                "ViewNode expects a "
                f"Queryset. Got: {self.queryset}"
            )

        template_sql = self.template_sql
        if self.temporary:
            template_sql = self.template_sql.replace('view', 'temp view')

        # We need to evaluate the queryset first
        # in order to get underlying sql query
        # that will be used to create the view
        self.queryset.load_cache()
        sql = template_sql.format_map({
            'name': self.name,
            'select_node': self.queryset.query.sql
        })
        return [sql]



class WhenNode:
    def __init__(self, **condition: Any):
        self.condition = condition

    def as_sql(self, backend: TypeSQLiteBackend):
        filters = backend.decompose_filters(**self.condition)
        joined_filters = backend.build_filters(filters, space_characters=False)
        when_sql = backend.WHEN_TEMPLATE.format_map({
            'condition': joined_filters
        })
        return when_sql
