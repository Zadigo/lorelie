from lorelie.backends import connections
from lorelie.test.testcases import LorelieTestCase


class TestSQLiteConnection(LorelieTestCase):
    def setUp(self):
        self.connection = self.create_connection()

    def test_structure(self):
        pass

    def test_connections(self):
        conn = connections.get_last_connection()
        self.assertEqual(conn, self.connection)
        # If a connection is not created within a database,
        # then there should be 'default' connection
        self.assertIn('default', connections.connections_map)
