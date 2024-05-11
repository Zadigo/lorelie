class Index:
    """Used for creating and index on the database
    on certain fields
    
    >>> table = Table('celebrities', indexes=[Index(fields=['id'])])
    """
    prefix = 'idx'

    def __init__(self, name, *fields):
        self.index_name = f'{self.prefix}_{name}'
        self.fields = list(fields)

    def __repr__(self):
        return f'<Index: {self.index_name}>'

    def as_sql(self, table):
        return table.backend.CREATE_INDEX.format_map({
            'name': self.index_name,
            'table': table.name,
            'fields': self._backend.comma_join(self._fields)
        })
