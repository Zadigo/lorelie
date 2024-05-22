import secrets

from lorelie.expressions import CombinedExpression, Q


class BaseConstraint:
    template_sql = None
    prefix = None

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.generated_name}>'

    @property
    def generated_name(self):
        random_string = secrets.token_hex(nbytes=5)
        return '_'.join([self.prefix, self.name, random_string])

    def as_sql(self, backend):
        return NotImplemented
    

class CheckConstraint(BaseConstraint):
    """Represents a SQL CHECK constraint that is used 
    to enforce a specific condition on data in a database table. 
    This constraint ensures that all values in a column or combination 
    of columns meet a predefined condition

    >>> name_constraint = CheckConstraint('my_name', Q(name__ne='Kendall'))
    ... table = Table('celebrities', constraints=[name_constraint])
    """
    template_sql = 'check({condition})'
    prefix = 'chk'

    def __init__(self, name, condition):
        super().__init__(name)
        if not isinstance(condition, (Q, CombinedExpression)):
            raise ValueError('Condition should be an instance of Q')
        self.condition = condition

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.condition}>'

    def as_sql(self, backend):
        condition_sql = backend.simple_join(self.condition.as_sql(backend))
        return self.template_sql.format(condition=condition_sql)


class UniqueConstraint(BaseConstraint):
    """Represents a SQL UNIQUE constraint that ensures all 
    values in a specified column or combination of columns 
    are unique across the database table.  This function is very 
    useful for adding multiple constraints on multiple columns 
    in the database, providing an efficient way to enforce 
    data integrity and avoid duplicate entries

    >>> unique_name = UniqueConstraint(fields=['name'])
    ... table = Table('celebrities', constraints=[unique_name])
    """
    template_sql = 'unique({fields})'
    prefix = 'unq'

    def __init__(self, name, *, fields=[]):
        super().__init__(name)
        self.fields = fields

    def __repr__(self):
        return f'<{self.__class__.__name__}: ({self.fields})>'

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
