import dataclasses
from dataclasses import dataclass
from lorelie.fields.base import Field


@dataclass
class Column:
    """Represents a column for the SQLite database
    which also maps the relationship between the `Field`
    instance on the local table """

    field: Field
    index: int = 0
    name: str = None
    full_column_name: str = None
    double_relation: bool = False

    def __post_init__(self):
        if self.name is None:
            name = self.field.name
            if self.field.is_relationship_field:
                name = self.field.related_name
            self.name = name
        else:
            if self.name != self.field.name:
                raise ValueError(
                    "Ambiguous names were used for the "
                    f"field: {self.name} <> {self.field.name}"
                )

        self.index = self.field.index

    def __str__(self):
        return self.full_column_name

    def __eq__(self, item):
        if not isinstance(item, (str, Column, Field)):
            return NotImplemented

        bits = self.full_column_name.split('.')

        if isinstance(item, str):
            return any([
                item == self.field.name,
                item == self.name,
                item == self.full_column_name,
                item == bits[-1]
            ])

        return any([
            item == self.field,
            item.name == self.name,
            item.name == self.full_column_name,
            item == bits[-1]
        ])

    def __str__(self):
        return self.full_column_name

    def __repr__(self):
        return f'<Column: {self.full_column_name}>'

    def __hash__(self):
        return hash((self.name, self.table.name, self.index))

    @property
    def is_foreign_column(self):
        return self.field.is_relationship_field

    @property
    def table(self):
        return self.field.table

    def prepare(self):
        template = '{table_name}.{field_name}'
        if self.field.is_relationship_field:
            self.full_column_name = template.format_map({
                'table_name': self.table.name,
                'field_name': self.field.related_name
            })
        else:
            self.full_column_name = template.format_map({
                'table_name': self.table.name,
                'field_name': self.field.name
            })
