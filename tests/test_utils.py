import unittest

from multiflow.utils import *


class TestUtils(unittest.TestCase):
    def test_pluralize_zero(self):
        result = pluralize(0)
        expected_result = 's'
        self.assertEqual(expected_result, result)

    def test_pluralize_one(self):
        result = pluralize(1)
        expected_result = ''
        self.assertEqual(expected_result, result)

    def test_count_args_one(self):
        def test(one):
            pass

        result = count_args(test)
        expected_result = 1

        self.assertEqual(expected_result, result)

    def test_count_args_one_with_kwargs(self):
        def test(one, value=None, other=None):
            pass

        result = count_args(test)
        expected_result = 1

        self.assertEqual(expected_result, result)

    def test_count_args_zero(self):
        def test():
            pass

        result = count_args(test)
        expected_result = 0

        self.assertEqual(expected_result, result)

    def test_count_args_zero_with_kwargs(self):
        def test(value=None, other=None):
            pass

        result = count_args(test)
        expected_result = 0

        self.assertEqual(expected_result, result)

    def test_c_type_string_format(self):
        str_fmt = 'this is awesome: %(success_count)s'
        items = {'success_count': 2}

        self.assertTrue(use_c_string(str_fmt, items))

    def test_string_format_map(self):
        str_fmt = 'this is awesome: {success_count}'
        items = {'success_count': 2}

        self.assertFalse(use_c_string(str_fmt, items))
