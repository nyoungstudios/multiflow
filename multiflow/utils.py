"""
Util functions
"""
import inspect


def pluralize(count: int) -> str:
    """
    Returns string '' if count is equal to one otherwise 's'. Useful for making print/logging statements grammatically
    correct.

    :param count: The numerical count
    :return: A string if the noun needs to be plural or not
    """
    return '' if count == 1 else 's'


def use_c_string(str_format: str, items: dict) -> bool:
    """
    Tests if we should use the C type string formatting. Otherwise, defaults to False. Meaning we should use
    format_map() instead

    :param str_format: The string format
    :param items: A dictionary of items to pass to the string formatter
    :return: A boolean value if we should use the C type string formatting (%)
    """
    value = str_format % items
    if value == str_format:
        # raises KeyError if the string format has values not present in the dictionary
        str_format.format_map(items)
        return False
    else:
        return True


def find_arg_names(fn) -> tuple:
    """
    Creates a dictionary mapping of the argument name to its index of a function

    :param fn: a callable function
    :return: a tuple with the first item being a dictionary of the argument name to index of the argument, the second
        argument being a dictionary mapping of the kwarg name to default value
    """
    arg_to_index = {}
    kwargs_to_default = {}
    for i, (name, value) in enumerate(inspect.signature(fn).parameters.items()):
        arg_to_index[name] = i
        if value.default is not value.empty:
            kwargs_to_default[name] = value.default

    return arg_to_index, kwargs_to_default
