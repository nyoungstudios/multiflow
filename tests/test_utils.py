import unittest

from multiflow.utils import *

class TestUtils(unittest.TestCase):
    def test_pluralize_zero(self):
        result = pluralize(0)
        expected_result = 's'
        self.assertEqual(result, expected_result)

    def test_pluralize_one(self):
        result = pluralize(1)
        expected_result = ''
        self.assertEqual(result, expected_result)

    def test_calc_args_one(self):
        def test(one):
            pass

        result = calc_args(test)
        expected_result = 1

        self.assertEqual(result, expected_result)

    def test_calc_args_one_with_kwargs(self):
        def test(one, value=None, other=None):
            pass

        result = calc_args(test)
        expected_result = 1

        self.assertEqual(result, expected_result)

    def test_calc_args_zero(self):
        def test():
            pass

        result = calc_args(test)
        expected_result = 0

        self.assertEqual(result, expected_result)

    def test_calc_args_zero_with_kwargs(self):
        def test(value=None, other=None):
            pass

        result = calc_args(test)
        expected_result = 0

        self.assertEqual(result, expected_result)
