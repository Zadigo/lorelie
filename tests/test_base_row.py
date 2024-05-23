# import unittest

# from lorelie.backends import BaseRow, SQLiteBackend

# backend = SQLiteBackend()


# class TestBaseRow(unittest.TestCase):
#     def setUp(self):
#         self.row = BaseRow(
#             ['id', 'name'],
#             {'id': 1, 'name': 'Kendall'}
#         )

#     def test_attributes(self):
#         self.assertEqual(self.row.pk, 1)
#         self.assertEqual(self.row['name'], 'Kendall')
#         self.assertIn('Kendall', self.row)
#         self.assertFalse(self.row.mark_for_update)

#     def test_set_attributes(self):
#         # self.row.name = 'Kylie'
#         # self.assertTrue(self.row.mark_for_update)
#         self.row['name'] = 'Kylie'
#         self.assertTrue(self.row.mark_for_update)

#     def test_save(self):
#         self.row['name'] = 'Kylie'
#         self.row.save()
#         self.row.delete()


# if __name__ == '__main__':
#     unittest.main()
