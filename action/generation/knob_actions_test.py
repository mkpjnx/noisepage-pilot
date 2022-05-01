import unittest
from knob_actions_categorical import CategoricalKnobGenerator
from connector import Connector


# TODO(YING): test altersystem
class TestCategoricalKnobGenerator(unittest.TestCase):
    def test_illegal_name(self):
        enable = CategoricalKnobGenerator(Connector(), 'enable_seq', ['off', True])  # local
        actions = list(enable)
        self.assertEqual(actions, [])
        self.assertTrue(enable.illegal_knob)

    def test_illegal_type(self):
        with self.assertRaises(TypeError):
            _ = CategoricalKnobGenerator(Connector(), 'work_mem', [3, 4])  # local

    def test_boolean_legal_options(self):
        set_values = ['off', True, 'on', False]
        enable = CategoricalKnobGenerator(Connector(), 'enable_seqscan', set_values)  # local
        actions = list(enable)

        self.assertFalse(enable.illegal_knob)
        self.assertEqual(len(actions), len(set_values))
        self.assertEqual(enable.illegal_options, [])
        for i, a in enumerate(actions):
            # print(a)
            self.assertTrue("SET enable_seqscan TO '{}';".format(str(set_values[i])) == str(a))

    def test_boolean_illegal_options(self):
        set_values = ['off', 'or', '??', True]
        enable = CategoricalKnobGenerator(Connector(), 'enable_seqscan', set_values)  # local
        actions = list(enable)

        self.assertEqual(len(actions), 2)
        self.assertEqual(enable.illegal_options, ['or', '??'])
        self.assertFalse(enable.illegal_knob)

    def test_enum_legal_options(self):
        set_values = ['minimal', 'replica']
        wal_alter = CategoricalKnobGenerator(Connector(), 'wal_level', set_values)
        actions = list(wal_alter)

        self.assertFalse(wal_alter.illegal_knob)
        self.assertEqual(len(actions), len(set_values))
        self.assertEqual(wal_alter.illegal_options, [])
        for i, a in enumerate(actions):
            self.assertTrue("SET wal_level TO '{}';".format(str(set_values[i])) == str(a))

    def test_enum_illegal_options(self):
        set_values = ['off', 'or', 'minimal', True]
        wal_alter = CategoricalKnobGenerator(Connector(), 'wal_level', set_values)
        actions = list(wal_alter)

        self.assertEqual(len(actions), 1)
        self.assertEqual(wal_alter.illegal_options, ['off', 'or', True])
        self.assertFalse(wal_alter.illegal_knob)


if __name__ == '__main__':
    unittest.main()
