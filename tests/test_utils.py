import unittest

from multiflow.utils import pluralize, use_c_string


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
