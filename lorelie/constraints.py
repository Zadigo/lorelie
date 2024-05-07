import secrets

from lorelie.backends import SQLiteBackend


class CheckConstraint:
    def __init__(self, name, *, fields=[]):
        self.name = name
        self.fields = fields

    def __call__(self, value):
        pass


# class MaxLengthConstraint(CheckConstraint):
#     def __init__(self, fields=[]):
#         super().__init__(
#             name=f'cst_{secrets.token_bytes(nbytes=5)}',
#             fields=fields
#         )
#         self.max_length = None

#     def __call__(self, value):
#         if value is None:
#             return True
#         return len(value) > self.max_length

#     # TODO: Pass backend as an argument?
#     def as_sql(self, backend):
#         if not isinstance(backend, SQLiteBackend):
#             raise ValueError()
        
#         # params = {'field': field, 'operator': '>', 'value': self.max_length}
#         # values = [
#         #     backend.CONDITION.format_map(params)
#         #     for field in self.fields
#         # ]

#         values = []
#         for field in self.fields:
#             values.append(backend.CONDITION.format_map({
#                 'field': field, 
#                 'operator': '>', 
#                 'value': self.max_length
#             }))

#         sql = backend.CHECK_CONSTRAINT.format_map({
#             'constraints': backend.operator_join(values)
#         })
#         return sql


class MaxLengthConstraint(CheckConstraint):
    CHECK = 'check({condition})'

    def __init__(self, limit, field):
        self.limit = limit
        self.field = field

    def __call__(self, value):
        if value is None:
            return True
        return len(value) > self.limit

    def as_sql(self, backend):
        if not isinstance(backend, SQLiteBackend):
            raise ValueError()
        
        condition = backend.CONDITION.format_map({
            'field': self.field.name, 
            'operator': '>', 
            'value': self.limit
        })
        check_sql = self.CHECK.format_map({
            'condition': condition
        })
        return check_sql
