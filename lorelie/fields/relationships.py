from lorelie.fields.base import Field


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


class ForeignKeyActions:
    """Default actions to be run on the
    foreign key when the parent is updated
    """

    CASCADE = ForeignKeyAction('cascade')
    SET_DEFAULT = ForeignKeyAction('set default')
    SET_NULL = ForeignKeyAction('set null')
    DO_NOTHING = ForeignKeyAction('no action')


class BaseRelationshipField(Field):
    """A special field used to access the
    relationship between two given tables"""

    def __init__(self, name, related_name=None, reverse=False, **kwargs):
        self.table = None
        self.database = None
        self.related_name = related_name
        self.relationship_map = None
        self.reverse = reverse
        # By default, the id on the parent table is linked
        # to a child field on the child table: id -> tablename_id
        # if reverse:
        #     name = relationship_map.foreign_backward_related_field_name
        # else:
        #     name = relationship_map.foreign_forward_related_field_name

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

    def prepare(self, table):
        self.table = table
        if self.reverse:
            self.related_name = self.relationship_map.foreign_backward_related_field_name
        else:
            self.related_name = self.relationship_map.foreign_forward_related_field_name


class ForeignKeyField(BaseRelationshipField):
    """Adds a foreign key between two tables by using the
    default primary ID field. The orientation for the foreign
    key goes from `left_table.id` to `right_table.field_id`

    >>> table1 = Table('celebrities', fields=[CharField('firstname', max_length=200)])
    ... table2 = Table('social_media', fields=[CharField('name', max_length=200)])

    >>> db = Database(table1, table2)
    ... db.foreign_key('followers', table1, table2, on_delete='cascade', related_name='f_my_table')
    ... db.migrate()
    ... db.social_media_tbl.all()
    ... db.celebrity_tbl_set.all()
    ... db.objects.foreign_key('social_media').all()
    ... db.objects.foreign_key('social_media', reverse=True).all()
    """

    def __init__(self, table, name, on_delete=None, **kwargs):
        self.on_delete = on_delete
        self.foreign_table = table
        super().__init__(name, **kwargs)

    def field_parameters(self):
        initial_field_parameters = super().field_parameters()
        # Do not use the user provided name which is just a
        # name to facilitate the querying on the database.
        # We want to use a "field_id" type (ideally) if the
        # user does not provide a related name, as the column
        # name on the database
        initial_field_parameters[0] = self.related_name

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

        if self.on_delete is None:
            self.on_delete = ForeignKeyActions.SET_NULL

        # self.relationship_field_params.append(
        #     self.on_delete.as_sql()
        # )

        return initial_field_parameters
