import hashlib
import io
import logging
import math
import os
import re
import threading
import unittest

from multiflow import MultithreadedGeneratorBase, MultithreadedGenerator, MultithreadedFlow, FlowException
from tests.setup_logger import get_logger


def iterator(num):
    for i in range(num):
        yield i


def returns_item(item):
    return item


class TestFlow(unittest.TestCase):
    def setUp(self):
        self.thread_count = threading.active_count()
        self.assertEqual(self.thread_count, 1)

    def tearDown(self):
        final_thread_count = threading.active_count()
        self.assertEqual(self.thread_count, final_thread_count)
        self.assertEqual(self.thread_count, 1)
        self.thread_count = None

    def test_no_consumer(self):
        try:
            with MultithreadedFlow(iterator, 500) as flow:
                for output in flow:
                    pass
        except Exception as e:
            self.assertIsInstance(e, FlowException)

    def test_zero_items(self):
        expected_count = 0
        with MultithreadedFlow(iterator, expected_count) as flow:
            flow.add_function('returns item', returns_item)

            for output in flow:
                pass

            count = flow.get_successful_job_count() + flow.get_failed_job_count()

        self.assertEqual(count, expected_count)

    def test_flow_two_functions(self):
        def add_one(value):
            return value + 1

        def add_two(value):
            return value + 2

        expected_count = 5
        items = []
        with MultithreadedFlow(iterator, expected_count) as flow:
            flow.add_function('add one', add_one)
            flow.add_function('add two', add_two)

            for output in flow:
                items.append(output.get_result())

        for i in range(3, expected_count + 3):
            self.assertIn(i, items)

    def test_exception_catcher(self):
        def even_throw_exception(value):
            if value % 2 == 0:
                raise Exception('Failed because {} is an even number'.format(value))
            else:
                return value

        expected_count = 6
        class TestException(MultithreadedGenerator):
            def consumer(self):
                for i in iterator(expected_count):
                    self.submit_job(even_throw_exception, i)

        try:
            with TestException(catch_exception=True) as test_exception:
                for output in test_exception:
                    if not output:
                        self.assertIn('Failed because ', str(output.get_exception()))
                        self.assertIn(' is an even number', str(output.get_exception()))

                success_count = test_exception.get_successful_job_count()
                failed_count = test_exception.get_failed_job_count()

            self.assertEqual(success_count, 3)
            self.assertEqual(failed_count, 3)

        except Exception as e:
            # doesn't actually properly catch exception and cause the test case to fail since it is a threaded error
            self.fail(e)

    def test_log_errors(self):
        log_name = 'test'
        logger = get_logger(log_name)
        exception_str = 'This is an exception'
        def throw_exception():
            raise Exception(exception_str)

        class TestException(MultithreadedGenerator):
            def consumer(self):
                self.submit_job(throw_exception)

        try:
            with self.assertLogs(logger, level=logging.INFO) as l:
                with TestException(
                    catch_exception=True,
                    logger=logger,
                    retry_count=2,
                    log_warning=True,
                    log_error=True
                ) as test_exception:
                    for output in test_exception:
                        self.assertEqual(output.get_num_of_attempts(), 3)

                    success_count = test_exception.get_successful_job_count()
                    failed_count = test_exception.get_failed_job_count()

                self.assertEqual(success_count, 0)
                self.assertEqual(failed_count, 1)

                expected_logs = [
                    'WARNING:{}:Retrying job after catching exception: {}'.format(log_name, exception_str),
                    'WARNING:{}:Retrying job after catching exception: {}'.format(log_name, exception_str),
                    'ERROR:{}:Job failed with exception: {}'.format(log_name, exception_str)
                ]

                self.assertEqual(l.output, expected_logs)

        except Exception as e:
            # doesn't actually properly catch exception and cause the test case to fail since it is a threaded error
            self.fail(e)

    def test_periodic_logger(self):
        size = int(math.pow(10, 9))  # 1 GB
        def create_random_data():
            random_data = io.BytesIO(os.urandom(size))
            md5 = hashlib.md5(random_data.read()).hexdigest()

            return md5

        log_name = 'test'
        logger = get_logger(log_name)

        log_regex = re.compile(r'^\d+ job[s]? completed successfully\. \d+ job[s]? failed\.$')

        expected_count = 25

        class TestLogger(MultithreadedGenerator):
            def consumer(self):
                for i in iterator(expected_count):
                    self.submit_job(create_random_data)

        with self.assertLogs(logger, level=logging.INFO) as l:
            with TestLogger(
                max_workers=100,
                logger=logger,
                log_interval=1,
                log_periodically=True
            ) as flow:
                for output in flow:
                    pass

                count = flow.get_successful_job_count()

            self.assertEqual(count, expected_count)

            if not l.output:
                self.fail('No periodic logs were recorded')

            for log_statement in l.output:
                log_parts = log_statement.split(':')
                self.assertEqual(log_parts[0], 'INFO')
                self.assertEqual(log_parts[1], log_name)
                self.assertIsNotNone(log_regex.match(log_parts[2]), log_parts[2])
