
from lorelie.database.nodes import (
    BaseNode,
    ComplexNode,
)
from lorelie.lorelie_typings import NodeEnums
from lorelie.test.testcases import LorelieTestCase


class CustomNode(BaseNode):
    def as_sql(self, backend):
        return ['custom sql']


class OtherNode(CustomNode):
    @property
    def node_name(self):
        return NodeEnums.CREATE.value


class TestBaseNode(LorelieTestCase):
    def setUp(self):
        self.table = self.create_table()

    def test_structure(self):
        node = CustomNode(self.table)

        self.assertListEqual(node.fields, ['*'])
        self.assertEqual(node.node_name, '')
        self.assertIsInstance(node + node, ComplexNode)
        self.assertListEqual(
            node.as_sql(self.create_connection()),
            ['custom sql']
        )

    def test_add_valid(self):
        node = CustomNode(self.table) + CustomNode(self.table)
        self.assertIsInstance(node, ComplexNode)

    def test_add_invalid(self):
        with self.assertRaises(TypeError):
            node = CustomNode(self.table) + "invalid"
            self.assertEqual(node, NotImplemented)

    def test_equality(self):
        cases = [
            (CustomNode(self.table), CustomNode(self.table)),
            (CustomNode(self.table), 'Invalid Value'),
            (CustomNode(self.table), NodeEnums.CREATE),
        ]

        for case in cases:
            with self.subTest(case=f'Running case: {case}'):
                node1 = CustomNode(self.table)
                node2 = CustomNode(self.table)
                self.assertEqual(node1, node2)

    def test_contains_invalid(self):
        node = CustomNode(self.table)
        self.assertNotIn('invalid', node)

    def test_contains_valid(self):
        node = OtherNode(self.table)
        self.assertIn(NodeEnums.CREATE.value, node)
        self.assertIn(NodeEnums.CREATE, node)
