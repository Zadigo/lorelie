from warnings import deprecated
from lorelie.fields.base import Field


@deprecated("ForeignKeyAction not supported yet")
class ForeignKeyAction:
    str_choice = None

    def __init__(self, choice):
        self.str_choice = choice

    def __repr__(self):
        return f"<ForeignKeyActionsSQL: {self.str_choice}>"

    @classmethod
    def as_sql(cls, on_delete=True, **kwargs):
        if on_delete:
            return f"on delete {cls.str_choice}"


@deprecated("ForeignKeyActions not supported yet")
class ForeignKeyActions:
    """Default actions to be run on the
    foreign key when the parent is updated
    """

    CASCADE = ForeignKeyAction('cascade')
    SET_DEFAULT = ForeignKeyAction('set default')
    SET_NULL = ForeignKeyAction('set null')
    DO_NOTHING = ForeignKeyAction('no action')


@deprecated("BaseRelationshipField not supported yet")
class BaseRelationshipField(Field):
    """A special field used to access the
    relationship between two given tables"""

    def __init__(self, relationship_map=None, related_name=None, reverse=False, **kwargs):
        self.database = None
        self.related_name = related_name
        self.relationship_map = relationship_map
        self.reverse = reverse
        # By default, the id on the parent table is linked
        # to a child field on the child table: id -> tablename_id
        if reverse:
            name = relationship_map.foreign_backward_related_field_name
        else:
            name = relationship_map.foreign_forward_related_field_name

        # Instead of using the default tablename_id
        # related name, the user can provide his own
        # name. There might be cases where a names clash
        # and therefore we need to force the user to provide
        # a custom name for the relationship
        if related_name is not None:
            pass

        super().__init__(name, **kwargs)
        self.null = True
        self.is_relationship_field = True
        self.relationship_field_params = []

    @property
    def field_type(self):
        return 'integer'

    def prepare(self, database):
        self.database = database


@deprecated("ForeignKeyField not supported yet")
class ForeignKeyField(BaseRelationshipField):
    def __init__(self, on_delete=None, **kwargs):
        self.on_delete = on_delete
        super().__init__(**kwargs)

    def field_parameters(self):
        initial_field_parameters = super().field_parameters()
        template = 'foreign key ({field}) references {table}({parent_field_name})'
        relationship_sql = template.format_map({
            'field': self.relationship_map.foreign_forward_related_field_name,
            'table': self.relationship_map.left_table.name,
            'parent_field_name': 'id'
        })
        self.relationship_field_params = [
            relationship_sql,
            'deferrable',
            'initially deferred'
        ]

        # if self.on_delete is None:
        #     self.on_delete = ForeignKeyActions.SET_NULL

        # self.relationship_field_params.append(
        #     self.on_delete.as_sql()
        # )

        return initial_field_parameters
