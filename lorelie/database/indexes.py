import secrets

from lorelie.database.nodes import WhereNode


class Index:
    """ Used to create an index in the database, enhancing the 
    performance of queries on specified fields.

    An index is a database object that helps speed up the retrieval 
    of rows by using pointers. Indexes are created on database tables 
    and can significantly enhance the performance of data retrieval 
    operations by allowing the database to find rows more quickly and 
    efficiently. By indexing specific columns, the database can perform 
    searches, sorts, and filtering operations more efficiently, especially 
    in large datasets.

    This class allows for the creation of such indexes, 
    ensuring that the specified fields are indexed properly. It also handles 
    the naming and uniqueness constraints associated with index creation.

    >>> table = Table('celebrities', index=[Index('index_name', 'firstname')])
    """
    template_sql = 'create index {name} on {table} ({fields})'
    prefix = 'idx'
    max_name_length = 30

    def __init__(self, name, fields=[], condition=None):
        if len(name) > self.max_name_length:
            raise ValueError('Name should be maximum 30 carachters long')

        if not fields:
            raise ValueError(
                "At least one field must be provided "
                "in order to use an index on a database"
            )
        
        self.name = name
        self.fields = list(fields)
        self.condition = condition
        index_id = secrets.token_hex(nbytes=5)
        self.index_name = f'{self.prefix}_{name}_{index_id}'

    def __repr__(self):
        return f'<Index: fields={self.fields} condition={self.condition}>'

    def as_sql(self, table):
        for field in self.fields:
            table.has_field(field, raise_exception=True)

        fields_sql = self.template_sql.format_map({
            'name': self.index_name,
            'table': table.name,
            'fields': table.backend.comma_join(self.fields)
        })

        sql = [fields_sql]
    
        if self.condition is not None:
            where_node = WhereNode(self.condition)
            sql.extend(where_node.as_sql(table.backend))
        return table.backend.simple_join(sql)
