class ImproperlyConfiguredError(Exception):
    def __init__(self, table, message):
        super().__init__(message)


class TableExistsError(Exception):
    message = (
        'Table with name "{name}" '
        'does not exist in the database'
    )

    def __init__(self, name):
        message = self.message.format(name=name)
        super().__init__(message)


class NoDatabaseError(Exception):
    message = (
        "You are trying to load an sqlite connection from "
        "from a table ({table}) outside of a Database class"
    )

    def __init__(self, table):
        message = self.message.format(table=table)
        super().__init__(message)


class FieldExistsError(Exception):
    def __init__(self, name, table):
        available_fields = ', '.join(table.field_names)
        message = (
            f'Field with name "{name}" '
            f'does not exist on {table}: {available_fields}'
        )
        super().__init__(message)


class ValidationError(Exception):
    def __init__(self, message, **kwargs):
        super().__init__(message.format(**kwargs))


class MigrationsExistsError(Exception):
    """Error used in the absence of any
    migration files on the database"""

    def __init__(self):
        message = (
            "You are trying to call a function on the "
            "the database while there was no existing "
            "migrations for the database tables. Call "
            "db.migrate() before trying to use any query "
            "functions on the table instance"
        )
        super().__init__(message)


class ConnectionExistsError(Exception):
    message = (
        "No existing connection was found "
        "in the connections pool"
    )

    def __init__(self):
        super().__init__(self.message)
