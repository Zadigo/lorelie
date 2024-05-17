import unittest

from lorelie.backends import BaseRow, SQLiteBackend

backend = SQLiteBackend()


class TestBaseRow(unittest.TestCase):
    def setUp(self):
        row = BaseRow(
            backend.connection.cursor(),
            ['id', 'name'],
            {'id': 1, 'name': 'Kendall'}
        )
        self.row = row

    def test_attributes(self):
        # self.assertEqual(self.row.pk, 1)
        self.assertEqual(self.row['name'], 'Kendall')

        self.row['name'] = 'Kylie'
        self.assertTrue(self.row.mark_for_update)

        for name in ['1', 'Kendall']:
            with self.subTest(name=name):
                self.assertIn(name, self.row)

        self.assertTrue(self.row == 'name')

    def test_save(self):
        self.row['name'] = 'Kylie'
        self.row.save()
        self.row.delete()


if __name__ == '__main__':
    unittest.main()
