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
import re
from collections import defaultdict

from expressions import Value
from lorelie.expressions import CombinedExpression, Q


@dataclasses.dataclass
class SelectMap:
    """A node map that resolves the correct
    positions for the different parameters
    for the select sql statemment"""

    select: type = None
    where: type = None
    order_by: type = None
    limit: int = None
    groupby: list = None
    having: list = None

    def __setitem__(self, name,  value):
        setattr(self, name, value)

    @property
    def should_resolve_map(self):
        return self.select is not None

    def resolve(self, backend):
        nodes = []
        nodes.extend(self.select.as_sql(backend))

        if self.where is not None:
            nodes.extend(self.where.as_sql(backend))

        if self.order_by is not None:
            nodes.extend(self.order_by.as_sql(backend))

        if self.limit is not None:
            nodes.extend([f'limit {self.limit}'])

        if self.groupby is not None:
            nodes.extend([self.groupby])

        if self.having is not None:
            nodes.extend([self.having])

        return nodes

    def add_ordering(self, other):
        if not isinstance(other, OrderByNode):
            raise ValueError()

        if self.order_by is not None:
            self.order_by = self.order_by & other


class RawSQL:
    def __init__(self, backend, *nodes):
        for node in nodes:
            if not isinstance(node, (BaseNode, str)):
                raise ValueError()

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
        return list(self.as_sql())

    def __eq__(self, value):
        return value == self.__str__()

    def as_sql(self):
        if self.resolve_select:
            return self.select_map.resolve(self.backend)

        sql = []
        for node in self.nodes:
            sql.extend(node.as_sql(self.backend))
        return sql


class ComplexNode:
    def __init__(self, *nodes):
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

    def as_sql(self, backend):
        # return [node.as_sql(backend) for node in self.nodes]
        return RawSQL(backend, *self.nodes)


class BaseNode:
    template_sql = None

    def __init__(self, table=None, fields=[]):
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
        return name == self.node_name

    def __contains__(self, value):
        return value in self.node_name

    def __and__(self, node):
        return NotImplemented

    def __call__(self, *fields):
        return NotImplemented

    @property
    def node_name(self):
        return NotImplemented

    def as_sql(self, backend):
        return NotImplemented


class SelectNode(BaseNode):
    template_sql = 'select {fields} from {table}'

    def __init__(self, table, *fields, distinct=False, limit=None):
        super().__init__(table=table, fields=fields)
        self.distinct = distinct
        self.limit = limit

    @property
    def node_name(self):
        return 'select'

    def __call__(self, *fields):
        new_fields = self.fields.extend(fields)
        return self.__class__(self.table, *new_fields, distinct=self.distinct)

    def as_sql(self, backend):
        select_sql = self.template_sql.format_map({
            'fields': backend.comma_join(self.fields),
            'table': self.table.name
        })

        if self.distinct:
            return [select_sql.replace('select', 'select distinct')]
        return [select_sql]


class WhereNode(BaseNode):
    template_sql = 'where {params}'

    def __init__(self, *args, **expressions):
        self.expressions = expressions
        self.func_expressions = list(args)
        self.invert = False
        super().__init__()

    def __call__(self, *args, **expressions):
        self.expressions.update(expressions)
        self.func_expressions.extend(args)
        return self

    def __invert__(self):
        self.invert = True
        return self

    @property
    def node_name(self):
        return 'where'

    def as_sql(self, backend):
        # First, resolve Q, CombinedExpression to
        # their SQL representation. They are more
        # complex SQL expressions
        resolved = []
        for func in self.func_expressions:
            if isinstance(func, (Q, CombinedExpression)):
                resolved.extend(func.as_sql(backend))

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


class OrderByNode(BaseNode):
    template_sql = 'order by {fields}'

    def __init__(self, table, *fields):
        self.ascending = set()
        self.descending = set()
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
                        "ascending or descending fields"
                    )

                if sign:
                    self.descending.add(name)
                else:
                    self.ascending.add(name)

        self.cached_fields = list(self.ascending.union(self.descending))

    @property
    def node_name(self):
        return 'order_by'

    def __hash__(self):
        return hash((self.node_name, *self.fields))

    def __and__(self, node):
        if not isinstance(node, OrderByNode):
            return NotImplemented

        other_fields = set(node.fields)
        other_fields.update(self.fields)
        return node.__class__(self.table, *list(other_fields))

    @staticmethod
    def construct_sql(backend, field, ascending=True):
        if field == '*':
            return None

        if ascending:
            return backend.ASCENDING.format_map({'field': field})
        else:
            return backend.DESCENDING.format_map({'field': field})

    def as_sql(self, backend):
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


class UpdateNode(BaseNode):
    """To update existing data in a table, 
    you use SQLite UPDATE statement. The following illustrates 
    the syntax of the UPDATE statement:
    """

    # https://www.sqlitetutorial.net/sqlite-update/
    template_sql = 'update {table} set {fields}'

    def __init__(self, table, update_defaults, *where_args, **where_expressions):
        super().__init__(table=table)
        self.where_args = where_args
        self.where_expressions = where_expressions
        self.update_defaults = update_defaults

    @property
    def node_name(self):
        return 'update'

    def as_sql(self, backend):
        where_node = WhereNode(*self.where_args, **self.where_expressions)
        fields_to_set = backend.parameter_join(self.update_defaults)

        update_sql = self.template_sql.format_map({
            'table': self.table.name,
            'fields': fields_to_set
        })
        return [update_sql, *where_node.as_sql(backend)]


class InsertNode(BaseNode):
    """This node allows the creation of the sql
    for insert one or multiple values in the database.

    Values can be inserted in batch using `batch_values` which
    is a list of dictionnaries or inserted as a single element
    using `insert_values`

    Note: https://www.sqlitetutorial.net/sqlite-insert/
    """

    template_sql = 'insert into {table} ({columns}) values({values})'
    bactch_insert_sql = 'insert into {table} ({columns}) values {values}'

    def __init__(self, table, batch_values=[], insert_values={}, returning=False):
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
        return 'insert'

    def as_sql(self, backend):
        template = self.template_sql

        if self.batch_values:
            columns = self.batch_values[0].keys()
        
            values = []
            for item in self.batch_values:
                quoted_values = backend.quote_values(item.values())
                joined = backend.comma_join(quoted_values)
                values.append(f"({joined})")
        
            joined_values = backend.comma_join(values)
            template = self.bactch_insert_sql
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
