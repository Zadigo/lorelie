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
