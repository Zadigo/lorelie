import dataclasses
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


@dataclasses.dataclass
class SelectMap:
    """A node map that resolves the correct
    positions for the different parameters
    of the select node"""

    select: type = None
    where: type = None
    order_by: type = None
    limit: int = None
    groupby: list = None
    having: list = None

    def __setitem__(self, name,  value):
        setattr(self, name, value)

    def resolve(self, backend):
        nodes = []
        nodes.extend(self.select.as_sql(backend))

        if self.where is not None:
            nodes.extend(self.where.as_sql(backend))

        if self.order_by is not None:
            nodes.extend(self.order_by.as_sql(backend))

        if self.limit is not None:
            nodes.extend(self.limit.as_sql(backend))

        if self.groupby is not None:
            nodes.extend(self.groupby.as_sql(backend))

        if self.having is not None:
            nodes.extend(self.having.as_sql(backend))

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

    def __contains__(self, value):
        return value in self.node_name

    def __and__(self, node):
        return NotImplemented

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
        return 'order_by'

    def __hash__(self):
        return hash((self.node_name, *self.fields))

    def __and__(self, node):
        if not isinstance(node, OrderByNode):
            return NotImplemented
        other_fields = set(node.fields)
        other_fields.update(self.fields)
        return node.__class__(self.table, *list(other_fields))

    def as_sql(self, backend):
        joined_fields = backend.comma_join(self.fields)
        return [self.template_sql.format(fields=joined_fields)]


"""This section tests chaining. In other words,
as long as the functions are chained from the
queryset, the query is not evaluated e.g.
db.objects.all().filter(). In this example,
the queryset will be evaluated on the last
filter method filter()
"""


class Query:
    def __init__(self, table=None, database=None):
        self.statements = []
        self.table = table
        self.database = database
        self.cache = []
        self.is_evaluated = False
        self.node_map = NodeMap()
        self.sql = []

    def add_sql(self, statement):
        if not isinstance(statement, (BaseNode, str)):
            raise ValueError()
        self.statements.append(statement)

    def pre_sql_setup(self):
        for statement in self.statements:
            if isinstance(statement, BaseNode):
                self.sql.extend(statement.as_sql(backend))
            elif isinstance(statement, str):
                self.sql.extend([statement])

    def run(self):
        self.pre_sql_setup()
        self.cache = ['value']
        self.is_evaluated = True


class QuerySet:
    def __init__(self, query):
        self.query = query
        self.cache = []

    def __str__(self):
        self.load_cache()
        return str(self.cache)

    def load_cache(self):
        if not self.cache:
            self.query.run()
            self.cache.append('Test')

    @property
    def sql_statemnent(self):
        return self.query.sql

    def all(self):
        return self

    def order_by(self, *fields):
        if not self.query.is_evaluated:
            order_by = OrderByNode(table, *fields)
            self.query.add_sql(order_by)
        return self


class Database:
    query_class = Query

    def all(self):
        query = self.query_class(database=self)
        select = SelectNode(table)
        query.add_sql(select)
        return QuerySet(query)


# # chaining = Database()
# # qs = chaining.all().order_by('firstname').order_by('lastname')
# # print(qs)
# # print(qs.sql_statemnent)


# select = SelectNode(table, 'firstname', 'lastname')
# # where = WhereNode(firstname__eq='Kendall', lastname__startswith='Kendall')
# # # where = WhereNode(
# # #     (
# # #         Q(firstame__eq='Kendall') |
# # #         Q(lastname__eq='Jenner') &
# # #         Q(lastname__ne='Margot')
# # #     ),
# # #     age__gt=15
# # # )

# order_by = OrderByNode(table, 'firstname', 'lastname')
# # order_by_2 = OrderByNode(table, 'age')
# # # new_order_by = order_by & order_by_2

# # a = [order_by, order_by_2]
# # # print(a.index('order by'))
# # print(set(a))

# raw = RawSQL(backend, select, order_by)
# print(raw)

# # node = select + where + order_by
# # # print(select.as_sql(backend))
# # # print(node.as_sql(backend))
# # # print(node)
# # # print(select == 'select')
# # # print(order_by in node)

# # select = SelectNode(table, 'firstname')
# # where = WhereNode(firstname__eq=1)
# # sub_query = SubQueryNode('firstname', select + where)
# # print(sub_query)
# # sql = RawSQL(backend, select, where, order_by)
# # print(sql)
