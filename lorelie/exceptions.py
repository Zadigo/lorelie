class ImproperlyConfiguredError(Exception):
    def __init__(self, table, message):
        super().__init__(message)


class TableExistsError(Exception):
    def __init__(self, name):
        message = (
            f'Table with name "{name}" '
            'does not exist in the database'
        )
        super().__init__(message)


class FieldExistsError(Exception):
    def __init__(self, name, table):
        available_fields = ', '.join(table.field_names)
        message = (
            f'Field with name "{name}" '
            f'does not exist on the table: {available_fields}'
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
            "db.migrate() before using any query functions"
        )
        super().__init__(message)


class ConnectionExistsError(Exception):
    def __init__(self):
        message = (
            "No existing connections were found "
            "in the connections pool"
        )
        super().__init__(message)
