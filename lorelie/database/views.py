from lorelie.database.nodes import ViewNode
from lorelie.lorelie_typings import TypeQuerySet, TypeTable
from lorelie.queries import Query


class View:
    """A view is a result set of a stored query by packing a query into a named 
    object stored in the database.

    You can access the data of the underlying tables through a view. The tables that the query 
    in the view definition refers to are called base tables.

    A view is useful in some cases:

    * First, views provide an abstraction layer over tables. You can add and remove the columns in the view without touching the schema of the underlying tables.
    * Second, you can use views to encapsulate complex queries with joins to simplify the data access

    Args:
        name (str): The name of the view.
        queryset (TypeQuerySet): The queryset that defines the view.
        temporary (bool, optional): Whether the view is temporary. Defaults to False.
        fields (list[str], optional): The fields of the view. Defaults to [].

    Example:

        >>> table = Table('products', fields=[CharField('name')])
        ... db = Database(table, name='products', log_queries=True)
        ... db.migrate()

        ... view = View('my_view', db.objects.all('products'))
        ... qs = view(table)

        Every following query on `qs` will be using `my_view`:

        >>> qs.all()
    """

    def __init__(self, name: str, queryset: TypeQuerySet, temporary: bool = False, fields: list[str] = []):
        self.name = name
        self.queryset = queryset
        self.temporary = temporary
        self.fields = fields

    def __call__(self, table: TypeTable):
        # Create a node and then run the underlying query
        # of the node in order to create the view. Since
        # the queryset was evaluated, return it. The latter
        # will be using the new view name to evaluate all
        # the following queries
        node = ViewNode(self.name, self.queryset, temporary=self.temporary)
        sql = node.as_sql(table.backend)

        query = Query(table=table)
        query.add_sql_nodes(sql)
        query.run(commit=True)

        setattr(node.queryset, 'alias_view_name', self.name)
        return node.queryset
