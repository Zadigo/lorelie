from lorelie.database.views import View
from lorelie.queries import QuerySet
from lorelie.test.testcases import LorelieTestCase


class TestView(LorelieTestCase):
    def test_structure(self):
        db = self.create_database()
        table = db.get_table('celebrities')

        view = View('my_view', table.objects.all())
        qs = view(db.get_table('celebrities'))
        self.assertIsInstance(qs, QuerySet)

        list(qs.all())

        self.assertEqual(qs.alias_view_name, 'my_view')
        self.assertEqual(qs.query.sql, 'select * from my_view;')

    def test_view_with_fields(self):
        db = self.create_database()
        table = db.get_table('celebrities')

        view = View('my_view', table.objects.all(), fields=['name', 'age'])
        qs = view(db.get_table('celebrities'))
        self.assertIsInstance(qs, QuerySet)

        list(qs.all())

        self.assertEqual(qs.alias_view_name, 'my_view')
        self.assertEqual(qs.query.sql, 'select * from my_view;')
