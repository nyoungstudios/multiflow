"""
Util functions
"""


def calc_args(fn):
    """
    Calculates the number of arguments a function accepts from its signature

    :param fn: A function
    :return: The number of arguments of a function
    """
    all_args_count = fn.__code__.co_argcount

    if fn.__defaults__ is not None:  # in case there are no kwargs
        num_of_kwargs = len(fn.__defaults__)
    else:
        num_of_kwargs = 0

    return all_args_count - num_of_kwargs


def pluralize(count: int) -> str:
    """
    Returns string '' if count is equal to one otherwise 's'. Useful for making print/logging statements grammatically
    correct.

    :param count: The numerical count
    :return: A string if the noun needs to be plural or not
    """
    return '' if count == 1 else 's'
