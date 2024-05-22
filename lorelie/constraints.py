import secrets

from lorelie.expressions import Q, CombinedExpression


class BaseConstraint:
    template_sql = None

    def __repr__(self):
        return f'<{self.__class__.__name__}>'

    def as_sql(self, backend):
        return NotImplemented
    

class CheckConstraint(BaseConstraint):
    template_sql = 'check({condition})'

    def __init__(self, name, condition):
        self.name = name
        if not isinstance(condition, (Q, CombinedExpression)):
            raise ValueError('Condition should be an instance of Q')
        self.condition = condition

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.condition}>'

    def as_sql(self, backend):
        condition_sql = backend.simple_join(self.condition.as_sql(backend))
        return self.template_sql.format(condition=condition_sql)


class UniqueConstraint(BaseConstraint):
    template_sql = 'unique({fields})'

    def __init__(self, name, *, fields=[]):
        self.name = name
        self.fields = fields

    def as_sql(self, backend):
        fields = backend.comma_join(self.fields)
        return self.template_sql.format(fields=fields)


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
