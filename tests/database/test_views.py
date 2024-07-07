from lorelie.database.views import View
from lorelie.queries import QuerySet
from lorelie.test.testcases import LorelieTestCase


class TestView(LorelieTestCase):
    def test_structure(self):
        db = self.create_database()
        view = View('my_view', db.objects.all('celebrities'))
        qs = view(db.get_table('celebrities'))
        self.assertIsInstance(qs, QuerySet)
        # self.assertTrue()
