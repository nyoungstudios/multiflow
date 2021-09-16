import unittest

from multiflow.utils import find_arg_names, pluralize, use_c_string


class TestUtils(unittest.TestCase):
    def test_pluralize_zero(self):
        result = pluralize(0)
        expected_result = 's'
        self.assertEqual(expected_result, result)

    def test_pluralize_one(self):
        result = pluralize(1)
        expected_result = ''
        self.assertEqual(expected_result, result)

    def test_c_type_string_format(self):
        str_fmt = 'this is awesome: %(success_count)s'
        items = {'success_count': 2}

        self.assertTrue(use_c_string(str_fmt, items))

    def test_string_format_map(self):
        str_fmt = 'this is awesome: {success_count}'
        items = {'success_count': 2}

        self.assertFalse(use_c_string(str_fmt, items))

    def test_find_args_with_args_and_kwargs(self):
        def fn(a, b, c=3):
            pass

        arg_to_index, kwargs_to_default = find_arg_names(fn)

        expected_arg_to_index = {'a': 0, 'b': 1, 'c': 2}
        expected_kwarg_to_default = {'c': 3}

        self.assertDictEqual(expected_arg_to_index, arg_to_index)
        self.assertDictEqual(expected_kwarg_to_default, kwargs_to_default)

    def test_find_args_with_args(self):
        def fn(a, b, c):
            pass

        arg_to_index, kwargs_to_default = find_arg_names(fn)

        expected_arg_to_index = {'a': 0, 'b': 1, 'c': 2}
        expected_kwarg_to_default = {}

        self.assertDictEqual(expected_arg_to_index, arg_to_index)
        self.assertDictEqual(expected_kwarg_to_default, kwargs_to_default)

    def test_find_args_with_kwargs(self):
        def fn(a=1, b=2, c=3):
            pass

        arg_to_index, kwargs_to_default = find_arg_names(fn)

        expected_arg_to_index = {'a': 0, 'b': 1, 'c': 2}
        expected_kwarg_to_default = {'a': 1, 'b': 2, 'c': 3}

        self.assertDictEqual(expected_arg_to_index, arg_to_index)
        self.assertDictEqual(expected_kwarg_to_default, kwargs_to_default)
