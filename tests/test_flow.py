import unittest

from multiflow import MultithreadedGeneratorBase, MultithreadedGenerator, MultithreadedFlow, FlowException


def iterator(num):
    for i in range(num):
        yield i


def returns_item(item):
    return item


class TestFlow(unittest.TestCase):
    def test_no_consumer(self):
        try:
            with MultithreadedFlow(iterator, 500) as flow:
                for output in flow:
                    print(output)
        except Exception as e:
            self.assertIsInstance(e, FlowException)

    def test_zero_items(self):
        expected_count = 0
        with MultithreadedFlow(iterator, expected_count) as flow:
            flow.add_function('returns item', returns_item)

            for output in flow:
                print(output)

            count = flow.get_successful_job_count() + flow.get_failed_job_count()

        self.assertEqual(count, expected_count)


