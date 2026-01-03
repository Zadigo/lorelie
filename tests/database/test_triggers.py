from lorelie.database.triggers import trigger
from lorelie.constants import PythonEvent
from lorelie.test.testcases import LorelieTestCase


class TestTrigger(LorelieTestCase):
    def test_structure(self):
        @trigger.register_python(PythonEvent.PRE_INIT, 'celebrities')
        def test_function():
            print("Trigger executed")

        table = self.create_table()
        self.assertTrue(len(trigger.python_events) > 0)
