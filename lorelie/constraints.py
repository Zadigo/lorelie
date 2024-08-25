import secrets

from lorelie.expressions import CombinedExpression, Q


class BaseConstraint:
    template_sql = None
    prefix = None
    base_errors = {
        'integer': (
            "Limit for {klass} should be "
            "an integer field"
        )
    }

    def __init__(self, name):
        self.name = name

        random_string = secrets.token_hex(nbytes=5)
        self.constraint_name = '_'.join([
            self.prefix,
            name,
            random_string
        ])

    def __repr__(self):
        klass_name = self.__class__.__name__
        return f'<{klass_name}: {self.constraint_name}>'

    def __hash__(self):
        return hash((self.name))

    def __eq__(self, instance):
        if not isinstance(instance, BaseConstraint):
            return NotImplemented
        return instance.name == self.name

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

    prefix = 'unq'
    template_sql = 'unique({fields})'

    def __init__(self, name, *, fields=[]):
        super().__init__(name)
        self.fields = fields

    def __repr__(self):
        return f'<{self.__class__.__name__}: ({self.fields})>'

    def as_sql(self, backend):
        fields = backend.comma_join(self.fields)
        return self.template_sql.format(fields=fields)


class MinMaxMixin:
    def __init__(self, limit, field):
        if not isinstance(limit, int):
            error = self.base_errors['integer']
            raise ValueError(error.format(klass=self.__class__.__name__))

        self.name = 'min_max'
        self.constraint_name = f'{self.name}_{secrets.token_hex(nbytes=5)}'
        self.limit = limit
        self.field = field

    def __hash__(self):
        return hash((self.limit, self.field))


class MaxLengthConstraint(MinMaxMixin, BaseConstraint):
    """The `MaxLengthConstraint` class is a custom database constraint 
    used to enforce a maximum length on a specified field within a table. 
    This constraint ensures that the length of the field's value does not 
    exceed a defined limit. If the value's length surpasses this limit, 
    the constraint will be violated, thus maintaining data integrity by 
    restricting the length of the input data"""

    template_sql = 'check({condition})'
    length_sql = 'length({column})'

    def as_sql(self, backend):
        condition = backend.CONDITION.format_map({
            'field': self.length_sql.format(self.field.name),
            'operator': '>',
            'value': self.limit
        })
        return self.template_sql.format(condition=condition)


class MinValueConstraint(MinMaxMixin, BaseConstraint):
    template_sql = 'check({condition})'
    operator = '>'

    def as_sql(self, backend):
        condition = backend.CONDITION.format_map({
            'field': self.field.name,
            'operator': self.operator,
            'value': self.limit
        })
        return self.template_sql.format(condition=condition)


class MaxValueConstraint(MinValueConstraint):
    operator = '<'
