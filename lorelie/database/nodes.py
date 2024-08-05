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

from lorelie.expressions import CombinedExpression, Q


@dataclasses.dataclass
class SelectMap:
    """A map that resolves the correct
    positions for the different parameters
    for the select sql statement"""

    select: type = None
    join: type = None
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
    
    def valid_select_node_statement(self, node):
        """Checks if a nodes name is a valid select
        element for the select statement"""
        valid_names = list(map(lambda x: x.name, dataclasses.fields(self)))
        return node.node_name in valid_names

    def resolve(self, backend):
        nodes = []
        nodes.extend(self.select.as_sql(backend))

        if self.join is not None:
            nodes.extend(self.join.as_sql(backend))

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

    def __init__(self, table, *fields, distinct=False, limit=None, view_name=None):
        super().__init__(table=table, fields=fields)
        self.distinct = distinct
        self.limit = limit
        self.view_name = view_name

    def __call__(self, *fields, **kwargs):
        new_fields = self.fields.extend(fields)
        return self.__class__(self.table, *new_fields, **kwargs)

    @property
    def node_name(self):
        return 'select'

    def as_sql(self, backend):
        select_sql = self.template_sql.format_map({
            'fields': backend.comma_join(self.fields),
            # We can query a table or view that was previously
            # created in the current database using View
            'table': self.view_name or self.table.name
        })

        if self.distinct:
            return [select_sql.replace('select', 'select distinct')]
        return [select_sql]


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
        sql = [
            update_sql,
            *where_node.as_sql(backend)
        ]
        return sql


class DeleteNode(BaseNode):
    def __init__(self, table, *where_args, order_by=[], limit=None, **where_expressions):
        super().__init__(table=table)
        self.where_args = where_args
        self.where_expressions = where_expressions
        self.order_by = order_by
        self.limit = limit

    @property
    def node_name(self):
        return 'delete'

    def as_sql(self, backend):
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

    template_sql = 'insert into {table} ({columns}) values({values})'
    bactch_insert_sql = 'insert into {table} ({columns}) values {values}'

    def __init__(self, table, batch_values=[], insert_values={}, returning=[]):
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


class JoinNode(BaseNode):
    """Node used to create the SQL statement
    that allows foreign key joins"""

    template_sql = '{join_type} join {table} on {condition}'

    cross_join = 'cross join {field}'
    full_outer_join = 'full outer join {table} using({field})'

    def __init__(self, table, relationship_map, join_type='inner'):
        super().__init__()

        accepted_joins = ['inner', 'left', 'right', 'cross']
        if join_type not in accepted_joins:
            raise ValueError('Join type is not valid')

        self.join_type = join_type
        self.table = table
        self.relationship_map = relationship_map

    @property
    def node_name(self):
        return 'join'

    def as_sql(self, backend):
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
            'table': self.table.name,
            'condition': condition
        })
        return [join_sql]


class IntersectNode(BaseNode):
    template_sql = '{0} intersect {1}'

    def __init__(self, left_select, right_select):
        self.left_select = left_select
        self.right_select = right_select

    @property
    def node_name(self):
        return 'intersect'

    def as_sql(self, backend):
        # lhv = self.left_select.as_sql(backend)
        # rhv = self.right_select.as_sql(backend)
        # sql = self.template_sql.format(lhv[0], rhv[0])
        # return [sql]
        lhv = backend.de_sqlize_statement(self.left_select)
        rhv = backend.de_sqlize_statement(self.right_select)
        return [self.template_sql.format(lhv, rhv)]


class ViewNode(BaseNode):
    template_sql = 'create view if not exists {name} as {select_node}'

    def __init__(self, name, queryset, temporary=False):
        self.name = name
        self.temporary = temporary
        self.queryset = queryset

    @property
    def node_name(self):
        return 'view'

    def as_sql(self, backend):
        if not hasattr(self.queryset, 'load_cache'):
            raise ValueError(
                f"ViewNode expects a Queryset. Got: {self.queryset}")

        template_sql = self.template_sql
        if self.temporary:
            template_sql = self.template_sql.replace('view', 'temp view')

        # We need to evaluate the queryset
        # in order to get underlying sql query
        # that will be used to create the view
        self.queryset.load_cache()
        sql = template_sql.format_map({
            'name': self.name,
            'select_node': self.queryset.query.sql
        })
        return [sql]
