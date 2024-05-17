import secrets

class CheckConstraint:
    template_sql = 'check({condition})'

    def __init__(self, name, *, fields=[], expressions={}):
        self.name = name
        self.fields = fields
        self.expressions= expressions


class UniqueConstraint:
    UNIQUE = 'unique({fields})'

    def __init__(self, name, *, fields=[]):
        self.name = name
        self.fields = fields

    def as_sql(self, backend):
        fields = backend.comma_join(self.fields)
        return self.UNIQUE.format(fields=fields)


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
        condition = backend.CONDITION.format_map({
            'field': self.field.name,
            'operator': '>',
            'value': self.limit
        })
        check_sql = self.CHECK.format_map({
            'condition': condition
        })
        return check_sql
