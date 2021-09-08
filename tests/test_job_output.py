import unittest

from multiflow import JobOutput


class TestJobOutput(unittest.TestCase):
    def test_successful(self):
        output = JobOutput(True, 1)
        self.assertTrue(output)

    def test_not_successful(self):
        output = JobOutput(False, 1)
        self.assertFalse(output)

    def test_result_repr(self):
        result = 'This is the result'
        output = JobOutput(True, 1, result=result)
        self.assertEqual(repr(result), repr(output))

    def test_result_str(self):
        result = 'This is the result'
        output = JobOutput(True, 1, result=result)
        self.assertEqual(result, str(output))

    def test_get_arg(self):
        output = JobOutput(True, 1, args=('a', 'b'), arg_to_index={'x': 0, 'y': 1})
        self.assertEqual('a', output[0])
        self.assertEqual('b', output[1])
        self.assertEqual('a', output.get('x'))
        self.assertEqual('b', output.get('y'))

    def test_get_kwarg(self):
        output = JobOutput(True, 1, kwargs={'a': 0, 'b': 1})
        self.assertEqual(0, output.get('a'))
        self.assertEqual(1, output.get('b'))

    def test_get_arg_none(self):
        output = JobOutput(True, 1)
        value = output.get('x')
        self.assertIsNone(value)
