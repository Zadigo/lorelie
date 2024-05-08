import unittest

from lorelie.backends import SQLiteBackend, connections


class TestConnections(unittest.TestCase):
    def setUp(self):
        backend = SQLiteBackend()
        connections.register(backend)

    def test_get_last_connection(self):
        backend = connections.get_last_connection()
        self.assertIsInstance(backend, SQLiteBackend)

    def test_connection_keys(self):
        values = connections.connections_map.keys()
        # TODO: Should have one connection which is
        # the initial created one
        self.assertListEqual(values, ['default'])


if __name__ == '__main__':
    unittest.main()
