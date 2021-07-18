import unittest

from multiflow import JobOutput


class TestJobOutput(unittest.TestCase):
    def test_successful(self):
        output = JobOutput(True, 1)
        self.assertTrue(output)

    def test_not_successful(self):
        output = JobOutput(False, 1)
        self.assertFalse(output)

    def test_result(self):
        result = 'This is the result'
        output = JobOutput(True, 1, result=result)
        self.assertEqual(str(output), result)
