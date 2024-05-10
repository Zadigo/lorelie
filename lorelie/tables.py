from collections import OrderedDict

from lorelie.backends import SQLiteBackend
from lorelie.exceptions import FieldExistsError, ImproperlyConfiguredError
from lorelie.expressions import OrderBy
from lorelie.fields import AutoField, Field
from lorelie.queries import Query


class BaseTable(type):
    def __new__(cls, name, bases, attrs):
        super_new = super().__new__
        if 'prepare' in attrs:
            new_class = super_new(cls, name, bases, attrs)
            cls.prepare(new_class)
            return new_class
        return super_new(cls, name, bases, attrs)

    @classmethod
    def prepare(cls, table):
        pass


class AbstractTable(metaclass=BaseTable):
    query_class = Query
    backend_class = SQLiteBackend

    def __init__(self):
        self.backend = None
        self.is_prepared = False

    def __hash__(self):
        return hash((self.name))

    def __eq__(self, value):
        return self.name == value

    def validate_values(self, fields, values):
        """Validate an incoming value in regards
        to the related field the user is trying
        to set on the column. The returned values
        are quoted by default"""
        validates_values = []
        for i, field in enumerate(fields):
            # TODO: Allow creation with id field
            if field == 'rowid' or field == 'id':
                continue
            
            try:
                field = self.fields_map[field]
            except KeyError:
                raise FieldExistsError(field, self)
            
            validated_value = self.backend.quote_value(
                field.to_database(list(values)[i])
            )
            validates_values.append(validated_value)
        return validates_values

    def load_current_connection(self):
        from lorelie.backends import connections
        self.backend = connections.get_last_connection()


class Table(AbstractTable):
    """Represents a table in the database. This class
    can be used independently but would require creating
    and managing table creation

    To create a table without using `Database`:

    >>> table = Table('my_table', 'my_database', fields=[Field('url')])
    ... table.prepare()
    ... table.create(url='http://example.come')

    However, if you wish to manage a migration file and other table related
    tasks, wrapping tables in `Database` is the best option:

    >>> table = Table('my_table', 'my_database', fields=[Field('url')])
    ... database = Database('my_database', table)
    ... database.make_migrations()
    ... database.migrate()
    ... database.objects.create('url', url='http://example.com')
    ... database.objects.all('url')
    """

    def __init__(self, name, *, fields=[], index=[], constraints=[], ordering=[], str_field='id'):
        self.name = name
        self.verbose_name = name.lower().title()
        self.indexes = index
        self.table_constraints = constraints
        self.field_constraints = {}
        # The str_field is the name of the
        # field to be used for representing
        # the column in the BaseRow
        self.str_field = str_field

        self.ordering = OrderBy(ordering)

        super().__init__()
        self.fields_map = OrderedDict()

        for field in fields:
            if not isinstance(field, Field):
                raise ValueError(f'{field} should be an instance of Field')

            field.prepare(self)
            self.fields_map[field.name] = field

        # Automatically create an ID field and set
        # it up with the table and backend
        id_field = AutoField()
        id_field.prepare(self)
        self.fields_map['id'] = id_field

        field_names = list(self.fields_map.keys())
        field_names.append('rowid')
        self.field_names = field_names

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.name}>'

    def __getattribute__(self, name):
        if name == 'backend':
            backend = self.__dict__['backend']
            if backend is None:
                raise ImproperlyConfiguredError(
                    self,
                    "You are trying to use a table outside of a Database "
                    "and therefore calling it without a backend being set"
                )
        return super().__getattribute__(name)

    def has_field(self, name, raise_exception=False):
        result = name in self.fields_map
        if not result and raise_exception:
            raise FieldExistsError(name, self)
        return result

    def get_field(self, name):
        return self.fields_map[name]

    def create_table_sql(self, fields):
        sql = self.backend.CREATE_TABLE.format_map({
            'table': self.name,
            'fields': fields
        })
        return [sql]

    def drop_table_sql(self, name):
        sql = self.backend.DROP_TABLE.format_map({
            'table': self.name
        })
        return [sql]

    def build_field_parameters(self):
        """Returns the paramaters for all
        the fields present on the current table"""
        return [
            field.field_parameters()
            for field in self.fields_map.values()
        ]

    def prepare(self):
        """Prepares the table and then creates it
        in the database using the parameters of the
        different fields and parameters"""
        field_params = self.build_field_parameters()
        field_params = [
            self.backend.simple_join(params)
            for params in field_params
        ]
        sql = self.create_table_sql(self.backend.comma_join(field_params))
        query = self.query_class(self.backend, sql, table=self)
        query.run(commit=True)
        self.is_prepared = True
