from lorelie.database.tables.columns import Column
from lorelie.fields.base import CharField
from lorelie.test.testcases import LorelieTestCase


class TestColumns(LorelieTestCase):
    def test_structure(self):
        field = CharField('name')
        field.table = self.create_table()

        column = Column(field, index=1, name='name')
        column.prepare()

        self.assertEqual(column.name, 'name')
        self.assertEqual(column.full_column_name, 'celebrities.name')

    def test_equality(self):
        field = CharField('name')
        field.table = self.create_table()

        column = Column(field, index=1, name='name')
        column.prepare()

        self.assertTrue('celebrities.name' == column)
        self.assertTrue('name' == column)
