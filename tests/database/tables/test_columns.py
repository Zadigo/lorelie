from lorelie.database.tables.base import Column
from lorelie.test.testcases import LorelieTestCase


class TestColumn(LorelieTestCase):
    def test_stucture(self):
        table = self.create_table()
        field = table.get_field('name')

        column = Column(field)
        column.prepare()
        self.assertEqual('name', column)
