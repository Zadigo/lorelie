import secrets
from abc import ABC, abstractmethod
from typing import ClassVar, Optional, override

from lorelie.expressions import CombinedExpression, Q
from lorelie.lorelie_typings import TypeField, TypeSQLiteBackend


class BaseConstraint(ABC):
    template_sql: Optional[str] = None
    prefix: Optional[str] = None
    base_errors = {
        'integer': (
            "Limit for {klass} should be "
            "an integer field"
        )
    }

    def __init__(self, name: str):
        self.name = name

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.generated_name}>'

    def __hash__(self) -> int:
        return hash((self.name))

    @property
    def generated_name(self) -> str:
        random_string = secrets.token_hex(nbytes=5)
        return '_'.join([self.prefix, self.name, random_string])

    @abstractmethod
    def as_sql(self, backend: TypeSQLiteBackend) -> str:
        raise NotImplemented

    def deconstruct(self) -> tuple[str, list[str]]:
        params = [self.name]
        return self.__class__.__name__, params


class CheckConstraint(BaseConstraint):
    """Represents a SQL CHECK constraint that is used 
    to enforce a specific condition on data in a database table. 
    This constraint ensures that all values in a column or combination 
    of columns meet a predefined condition

    >>> name_constraint = CheckConstraint('my_name', Q(name__ne='Kendall'))
    ... table = Table('celebrities', constraints=[name_constraint])

    This is equivalent to the SQL statement:

    ```sql
    CHECK(name!='Kendall')
    ```

    Args:
        name (str): The name of the constraint.
        condition (Q | CombinedExpression): The condition to be enforced by the constraint.

    Raises:
        ValueError: If the condition is not an instance of Q or CombinedExpression.
    """

    template_sql: Optional[str] = 'check({condition})'
    prefix: Optional[str] = 'chk'

    def __init__(self, name: str, condition: Q | CombinedExpression):
        super().__init__(name)
        if not isinstance(condition, (Q, CombinedExpression)):
            raise ValueError('Condition should be an instance of Q')
        self.condition = condition

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.condition}>'

    @override
    def as_sql(self, backend: TypeSQLiteBackend):
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

    Thi is equivalent to the SQL statement:

    ```sql
    UNIQUE(name)
    ```

    Args:
        name (str): The name of the constraint.
        fields (list[str]): The list of fields to be included in the unique constraint.
    """

    template_sql: Optional[str] = 'unique({fields})'
    prefix: Optional[str] = 'unq'

    def __init__(self, name: str, *, fields: list[str] = []):
        super().__init__(name)
        self.fields = fields

    def __repr__(self):
        return f'<{self.__class__.__name__}: ({self.fields})>'

    @override
    def as_sql(self, backend: TypeSQLiteBackend):
        fields = backend.comma_join(self.fields)
        return self.template_sql.format(fields=fields)


class MinMaxMixin:
    def __init__(self, limit: int, field: TypeField):
        if not isinstance(limit, int):
            error = self.base_errors['integer']
            raise ValueError(error.format(klass=self.__class__.__name__))

        self.limit = limit
        self.field = field


class MaxLengthConstraint(MinMaxMixin, BaseConstraint):
    """The `MaxLengthConstraint` class is a custom database constraint 
    used to enforce a maximum length on a specified field within a table. 
    This constraint ensures that the length of the field's value does not 
    exceed a defined limit. If the value's length surpasses this limit, 
    the constraint will be violated, thus maintaining data integrity by 
    restricting the length of the input data

    This is equivalent to the SQL statement:

    ```sql
    CHECK(length(column_name) <= limit)
    ```

    Args:
        limit (int): The maximum allowed length for the field.
        field (TypeField): The field to which the constraint is applied.
    """

    template_sql: Optional[str] = 'check({condition})'
    length_sql: ClassVar[str] = 'length({column})'

    @override
    def as_sql(self, backend: TypeSQLiteBackend):
        condition = backend.CONDITION.format_map({
            'field': self.length_sql.format(column=self.field.name),
            'operator': '>',
            'value': self.limit
        })
        return self.template_sql.format(condition=condition)


class MinValueConstraint(MinMaxMixin, BaseConstraint):
    """The `MinValueConstraint` class is a custom database constraint 
    used to enforce a minimum value on a specified field within a table. 
    This constraint ensures that the value of the field is not less than 
    a defined limit. If the value falls below this limit, the constraint
    will be violated, thus maintaining data integrity by restricting
    the range of acceptable input data.

    This is equivalent to the SQL statement:

    ```sql
    CHECK(column_name > limit)
    ```

    Args:
        limit (int): The minimum allowed value for the field.
        field (TypeField): The field to which the constraint is applied.
    """

    template_sql: Optional[str] = 'check({condition})'
    operator: Optional[str] = '>'

    @override
    def as_sql(self, backend: TypeSQLiteBackend):
        condition = backend.CONDITION.format_map({
            'field': self.field.name,
            'operator': self.operator,
            'value': self.limit
        })
        return self.template_sql.format(condition=condition)


class MaxValueConstraint(MinValueConstraint):
    """The `MaxValueConstraint` class is a custom database constraint
    used to enforce a maximum value on a specified field within a table.
    This constraint ensures that the value of the field does not exceed
    a defined limit. If the value surpasses this limit, the constraint
    will be violated, thus maintaining data integrity by restricting
    the range of acceptable input data.

    This is equivalent to the SQL statement:

    ```sql
    CHECK(column_name < limit)
    ```

    Args:
        limit (int): The maximum allowed value for the field.
        field (TypeField): The field to which the constraint is applied.
    """

    operator: Optional[str] = '<'
