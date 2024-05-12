import secrets


class Index:
    """Used for creating and index on the database
    on certain fields

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
