class BaseExpression:
    template_sql = None

    @property
    def internal_type(self):
        return 'expression'

    @property
    def representation(self):
        """Property used to create the visual
        representation of expressions for 
        the shell"""
        return NotImplemented

    def as_sql(self, backend):
        return NotImplemented


class Value(BaseExpression):
    def __init__(self, value, output_field=None):
        self.value = value
        self.internal_name = 'value'
        self.output_field = output_field
        self.get_output_field()

    def __repr__(self):
        return f'{self.__class__.__name__}({self.to_database()})'

    def get_output_field(self):
        from lorelie.fields.base import CharField, FloatField, IntegerField, JSONField
        if self.output_field is None:
            if isinstance(self.value, int):
                self.output_field = IntegerField(self.internal_name)
            elif isinstance(self.value, float):
                self.output_field = FloatField(self.internal_name)
            elif isinstance(self.value, str):
                if self.value.isdigit() or self.value.isnumeric():
                    self.output_field = IntegerField(self.internal_name)
                else:
                    self.output_field = CharField(self.internal_name)
            elif isinstance(self.value, dict):
                self.output_field = JSONField(self.internal_name)

    def to_python(self, value):
        pass

    def to_database(self):
        return self.output_field.to_database(self.value)

    def as_sql(self, backend):
        if callable(self.value):
            self.value = str(self.value)

        if hasattr(self.value, 'internal_type'):
            self.value = str(self.value)

        return [backend.quote_value(self.value)]


class NegatedExpression(BaseExpression):
    template_sql = 'not {expression}'

    def __init__(self, expression):
        self.children = [expression]
        self.seen_expressions = []

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.expression}>'

    def __and__(self, other):
        self.children.append('and')

        if isinstance(other, NegatedExpression):
            self.children.extend(other.children)
            return self

        if isinstance(other, (F, Q)):
            self.seen_expressions.append(other.__class__.__name__)

        # We should not be able to combine certain
        # types of expressions ex. F & Q, F | Q
        # self.seen_expressions = [
        #     child.__class__.__name__
        #         for child in self.children
        #             if not isinstance(child, str)
        # ]

        # unique_children = set(self.seen_expressions)
        # if len(unique_children) > 1:
        #     pass

        self.children.append(other)
        return self

    def as_sql(self, backend):
        def map_children(node):
            if isinstance(node, str):
                return node
            return backend.simple_join(node.as_sql(backend))

        sql = map(map_children, self.children)

        return self.template_sql.format_map({
            'expression': backend.simple_join(sql)
        })


class When(BaseExpression):
    """Represents a conditional expression in an SQL query. 
    It defines a condition and the corresponding value when 
    the condition is met
    """

    def __init__(self, condition, then_case, **expressions):
        self.condition = condition
        self.then_case = then_case
        self.expressions = expressions
        self.children = []

    def __repr__(self):
        return f'When({self.representation})'

    @property
    def representation(self):
        representations = []
        for child in self.children:
            representations.append(repr(child))
        result = ' '.join(representations)
        return f'AND: {result} THEN: {self.then_case}'

    def as_sql(self, backend):
        if not isinstance(self.condition, (Q, CombinedExpression)):
            raise ValueError(
                'Condition should be a Q filter or a CombinedExpression')
        self.children.append(self.condition)

        if self.expressions:
            instance = Q(**self.expressions)
            self.children.append(instance)

        combined_expression = None
        for expression in self.children:
            if combined_expression is None:
                combined_expression = expression
                continue
            combined_expression = combined_expression & expression

        condition = backend.simple_join(combined_expression.as_sql(backend))
        sql = backend.WHEN.format_map({
            'condition': condition,
            'value': backend.quote_value(self.then_case)
        })
        return sql


class Case(BaseExpression):
    """Represents a conditional expression in an SQL query. 
    It evaluates multiple conditions and returns a value 
    based on the first condition that is met

    >>> logic = When('firstname=Kendall', 'Kylie')
    ... case = Case(condition1)
    ... db.objects.annotate('celebrities', case)
    """

    CASE = 'case {conditions}'
    CASE_ELSE = 'else {value}'
    CASE_END = 'end {alias}'

    def __init__(self, *cases, default=None):
        self.field_name = None
        self.alias_field_name = None
        self.default = default

        for case in cases:
            if not isinstance(case, When):
                raise ValueError('Value should be an instance of When')
        self.cases = list(cases)

    def __repr__(self):
        cases_repr = ', '.join(repr(case) for case in self.cases)
        return f'{self.__class__.__name__}({cases_repr})'

    def as_sql(self, backend):
        fields = set()
        statements_to_join = []

        for case in self.cases:
            fields.add(case.field_name)
            statements_to_join.append(case.as_sql(backend))

        case_sql = self.CASE.format_map({
            'conditions': backend.simple_join(statements_to_join)
        })

        if self.default is None:
            self.default = ''

        case_else_sql = self.CASE_ELSE.format(
            value=backend.quote_value(self.default)
        )

        if self.alias_field_name is None:
            raise ValueError(
                "Case annotation does not have an "
                f"alias name. Got: {self.alias_field_name}"
            )

        # TODO: We should check that the alias name does not
        # conflict with a field in the original table

        case_end_sql = self.CASE_END.format(alias=self.alias_field_name)
        return backend.simple_join([case_sql, case_else_sql, case_end_sql])


class CombinedExpression:
    """A combined expression is a combination
    of multiple expressions in order to create
    an combined expression sql statement

    >>> CombinedExpression(Q(age=21), Q(age=34))
    ... CombinedExpression(F('age'))
    """

    template_sql = '({inner}) {outer}'

    def __init__(self, *funcs):
        self.others = list(funcs)
        self.children = []
        self.other_combined_children = []
        self.should_resolve_combined = False
        
        for other in self.others:
            # We need to check for CombinedExpressions
            # here for example in Q & Q -> CombinedExpression
            # which will be passed inside another
            # CombinedExpression
            if isinstance(other, CombinedExpression):
                self.should_resolve_combined = True
                self.other_combined_children.extend(other.children)
                continue

            if self.should_resolve_combined:
                # We need to treat every function included
                # alongside combined expression as part
                # of the same resolution "sytem"
                self.other_combined_children.append(other)

        # Indicates that the expression
        # will be a mathematical one
        # e.g. F('age') + 1
        self.is_math = False
        # These expressions require an alias
        # field name in order to be rendered
        # correctly in sqlite
        self.alias_field_name = None
        # NOTE: Technically we will never be
        # using the combined expression as a
        # standalone class but we might need
        # to build the existing children on
        # init to avoid arrays like
        # a = CombinedExpression(Q(firstname='Kendall'))
        # b = Q(age__gt=26)
        # c = a & b -> ['(and age>26)']
        # self.build_children()

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.children}>'

    def __or__(self, other):
        self.children.append('or')
        self.children.append(other)
        return self

    def __and__(self, other):
        self.children.append('and')
        self.children.append(other)
        return self

    def __add__(self, other):
        self.children.append('+')
        self.children.append(Value(other))
        return self

    def __sub__(self, other):
        self.children.append('-')
        self.children.append(Value(other))
        return self

    def __div__(self, other):
        self.children.append('/')
        self.children.append(Value(other))
        return self

    def __mul__(self, other):
        self.children.append('*')
        self.children.append(Value(other))
        return self

    @property
    def internal_type(self):
        return 'expression'

    def build_children(self, operator='and'):
        for i in range(len(self.others)):
            other = self.others[i]
            self.children.append(other)

            if i + 1 != len(self.others):
                self.children.append(operator)

    def as_sql(self, backend):
        sql_statement = []
        for child in self.children:
            if isinstance(child, (str, int, float)):
                sql_statement.append(child)
                continue
            child_sql = child.as_sql(backend)
            sql_statement.append(child_sql)

        inner_items = []
        outer_items = []
        seen_operator = None
        is_inner = True

        for item in sql_statement:
            if item == 'and' or item == 'or' or item in ['+', '-', '/', '*']:
                if seen_operator is None:
                    is_inner = True
                elif seen_operator != item:
                    is_inner = False
                seen_operator = item

            if is_inner:
                if isinstance(item, (str, int, float)):
                    item = [item]
                inner_items.extend(item)
                continue
            else:
                if isinstance(item, (str, int, float)):
                    item = [item]
                outer_items.extend(item)
                continue

        inner = backend.simple_join(inner_items)
        outer = backend.simple_join(outer_items)
        sql = self.template_sql.format(inner=inner, outer=outer).strip()
        # TODO: Ensure that the SQL returned from these types
        # of expressions all return a list of strings
        return [sql]


class Q(BaseExpression):
    """Represents a filter expression within SQL queries, 
    employed within the `WHERE` clause for example
    to define complex filter conditions.

    >>> Q(firstname="Kendall") & Q(lastname="Jenner")

    The expression above expression results in a logical AND operation
    between two Q instances, where each instance represents a filter condition.

    >>> Q(firstname="Kendall") | Q(lastname="Jenner")

    The above expression results in a logical OR operation.
    """

    def __init__(self, **expressions):
        self.expressions = expressions

    def __repr__(self):
        klass_name = self.__class__.__name__
        return f'<{klass_name}: {self.representation()}>'

    def __and__(self, other):
        instance = CombinedExpression(self, other)
        instance.build_children()
        return instance

    def __or__(self, other):
        instance = CombinedExpression(self, other)
        instance.build_children(operator='or')
        return instance

    def __invert__(self):
        return NegatedExpression(self)

    def representation(self):
        result = []
        items = list(self.expressions.items())

        for i, item in enumerate(items):
            column, value = item

            if i > 0:
                result.append(f'AND: {column}:{value}')
                continue
            result.append(f'{column}:{value}')
        return ', '.join(result)

    def as_sql(self, backend):
        filters = backend.decompose_filters(**self.expressions)
        built_filters = backend.build_filters(filters, space_characters=False)
        return [backend.operator_join(built_filters)]


class F(BaseExpression):
    """The F function allows us to make operations
    directly on the value of the database

    >>> F('age') + 1
    ... F('price') * 1.2
    ... F('price') / 1.2
    ... F('price') - 1.2
    ... F('price') + F('price')

    These operations will translate directly to database
    operations in such as `price * 1.2` where the value
    in the price column will be multiplied by `1.2`
    """

    ADD = '+'
    SUBSRACT = '-'
    MULTIPLY = 'x'
    DIVIDE = '/'

    def __init__(self, field):
        self.field = field

    def __repr__(self):
        return f'{self.__class__.__name__}({self.field})'

    def __add__(self, other):
        if not isinstance(other, (F, str, int)):
            return NotImplemented

        if isinstance(other, (int, str, float)):
            other = Value(other)

        combined = CombinedExpression(self, other)
        combined.build_children(operator=self.ADD)
        return combined

    def __mul__(self, other):
        if not isinstance(other, (F, str, int)):
            return NotImplemented

        if isinstance(other, (int, str, float)):
            other = Value(other)

        combined = CombinedExpression(self, other)
        combined.build_children(operator=self.MULTIPLY)
        return combined

    def __div__(self, other):
        if not isinstance(other, (F, str, int)):
            return NotImplemented

        if isinstance(other, (int, str, float)):
            other = Value(other)

        combined = CombinedExpression(self, other)
        combined.build_children(operator=self.DIVIDE)
        return combined

    def __sub__(self, other):
        if not isinstance(other, (F, str, int)):
            return NotImplemented

        if isinstance(other, (int, str, float)):
            other = Value(other)

        combined = CombinedExpression(self, other)
        combined.build_children(operator=self.SUBSRACT)
        return combined

    def __neg__(self):
        return NegatedExpression(self)

    def as_sql(self, backend):
        return [self.field]
