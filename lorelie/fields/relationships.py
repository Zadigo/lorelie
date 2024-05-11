from lorelie.fields.base import Field


class BaseRelationshipField(Field):
    template = None

    def __init__(self, relationship_map=None, related_name=None, **kwargs):
        self.name = None
        self.relationship_map = relationship_map
        name = relationship_map.relationship_field_name
        super().__init__(name, **kwargs)

        # self.base_field_parameters['deferrable'] = True
        # self.base_field_parameters['initially deferred'] = True

    # def __getattr__(self, name):
    #     pass

    def prepare(self, database):
        self.database = database

    def as_sql(self, backend):
        pass


class ForeignKeyField(BaseRelationshipField):
    template = 'foreign key ({field}) references {table}({related_field})'

    def as_sql(self, backend):
        foreign_key_sql = self.template.format_map({
            'field': self.relationship_map.backward_related_field,
            'table': self.relationship_map.left_table.name,
            'related_field': 'id'
        })
        return [foreign_key_sql, 'deferrable', 'initially deferred']
