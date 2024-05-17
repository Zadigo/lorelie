import secrets


class Index:
    """Used to create an index in the database, enhancing 
    the performance of queries on specified fields.

    An index is a database object that helps speed up the retrieval 
    of rows by using pointers. Indexes are created on database tables 
    and can significantly enhance the performance of data retrieval 
    operations by allowing the database to find rows more quickly 
    and efficiently.

    >>> table = Table('celebrities', index=[Index('index_name', 'firstname')])
    """
    prefix = 'idx'
    max_name_length = 30

    def __init__(self, name, *fields):
        if len(name) > self.max_name_length:
            raise ValueError('Name should be maximum 30 carachters long')
        
        self.name = name
        self.fields = list(fields)
        index_id = secrets.token_hex(nbytes=5)
        self.index_name = f'{self.prefix}_{name}_{index_id}'

    def __repr__(self):
        return f'<Index: {self.index_name}>'

    def as_sql(self, table):
        for field in self.fields:
            table.has_field(field, raise_exception=True)

        return table.backend.CREATE_INDEX.format_map({
            'name': self.index_name,
            'table': table.name,
            'fields': table.backend.comma_join(self.fields)
        })
