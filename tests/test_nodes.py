from lorelie.backends import SQLiteBackend
from lorelie.expressions import CombinedExpression, Q
from lorelie.tables import Table

# backend = SQLiteBackend()

# table = Table('celebrities')


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


class RawSQL:
    def __init__(self, backend, *nodes):
        self.nodes = list(nodes)
        self.backend = backend

    def __repr__(self):
        return str(self.as_sql())

    def __str__(self):
        return self.backend.comma_join(self.as_sql())

    def __iter__(self):
        return list(self.as_sql())

    def __eq__(self, value):
        return value == self.__str__()

    def as_sql(self):
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
    """Class that represents a base SQL node.
    A node can be defined as an SQL bit that can
    be concatenated to other bits in order to
    create the full SQL statement text"""

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

    @property
    def node_name(self):
        return NotImplemented

    def as_sql(self, backend):
        return NotImplemented


class SelectNode(BaseNode):
    template_sql = 'select {fields} from {table}'

    def __init__(self, table, *fields, distinct=False, limit=None):
        self.distinct = distinct
        self.limit = limit
        super().__init__(table=table, fields=fields)

    @property
    def node_name(self):
        return 'select'

    def as_sql(self, backend):
        sql = self.template_sql.format_map({
            'fields': backend.comma_join(self.fields),
            'table': self.table.name
        })

        if self.distinct:
            return [sql.replace('select', 'select distinct')]
        return [sql]


class WhereNode(BaseNode):
    template_sql = 'where {params}'

    def __init__(self, *args, **expressions):
        self.expressions = expressions
        self.func_expressions = list(args)
        super().__init__()

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
        # are "and" operations go after the more complexe ones
        filters = backend.decompose_filters(**self.expressions)
        joined_filters = backend.build_filters(filters, space_characters=False)
        resolved.extend(joined_filters)

        joined_resolved = backend.operator_join(resolved)
        return [self.template_sql.format(params=joined_resolved)]


class OrderByNode(BaseNode):
    template_sql = 'order by {fields}'

    def __init__(self, table, *fields):
        super().__init__(table=table, fields=fields)

    @property
    def node_name(self):
        return 'order by'

    def as_sql(self, backend):
        joined_fields = backend.comma_join(self.fields)
        return [self.template_sql.format(fields=joined_fields)]


# class SubQueryNode(BaseNode):
#     template_sql = '{field}=({statement})'

#     def __init__(self, field, statement):
#         self.field = field
#         self.statement = statement
#         super().__init__()

#     def as_sql(self, backend):
#         statement = self.statement
#         if isinstance(self.statement, SelectNode):
#             statement = statement.as_sql(backend)
#         return self.template_sql.format_map({
#             'field': self.field,
#             'statement': self.statement
#         })


# select = SelectNode(table, 'firstname', 'lastname')
# where = WhereNode(firstname__eq='Kendall', lastname__startswith='Kendall')
# # where = WhereNode(
# #     (
# #         Q(firstame__eq='Kendall') |
# #         Q(lastname__eq='Jenner') &
# #         Q(lastname__ne='Margot')
# #     ),
# #     age__gt=15
# # )
# order_by = OrderByNode(table, 'firstname', 'lastname')
# node = select + where + order_by
# # print(select.as_sql(backend))
# # print(node.as_sql(backend))
# # print(node)
# # print(select == 'select')
# # print(order_by in node)

# select = SelectNode(table, 'firstname')
# where = WhereNode(firstname__eq=1)
# sub_query = SubQueryNode('firstname', select + where)
# print(sub_query)
# sql = RawSQL(backend, select, where, order_by)
# print(sql)
