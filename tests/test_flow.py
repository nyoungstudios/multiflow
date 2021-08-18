import logging
import re
import threading
import time
import unittest

from multiflow import MultithreadedGeneratorBase, MultithreadedGenerator, MultithreadedFlow, FlowException
from tests.setup_logger import get_logger


def iterator(num):
    for i in range(num):
        yield i


def returns_item(item):
    return item


def add_one(value):
    return value + 1


def add_two(value):
    return value + 2


def add_n(value, n):
    return value + n


class CustomException(Exception):
    pass


def even_throw_exception(value):
    if value % 2 == 0:
        raise CustomException('Failed because it is an even number|{}'.format(value))
    else:
        return value


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
            self.fail('Did not throw an exception')
        except Exception as e:
            self.assertIsInstance(e, FlowException)

    def test_no_consumer_base(self):
        try:
            with MultithreadedGeneratorBase() as flow:
                for output in flow:
                    pass
            self.fail('Did not throw an exception')
        except Exception as e:
            self.assertIsInstance(e, FlowException)

    def test_not_iterable(self):
        try:
            with MultithreadedFlow(None) as flow:
                flow.add_function(returns_item)

                for output in flow:
                    pass

                self.fail('Did not throw an exception')
        except Exception as e:
            self.assertIsInstance(e, FlowException)

    def test_zero_items(self):
        logger = get_logger('test')

        expected_before_count = 0
        expected_count = 0
        with MultithreadedFlow(iterator, expected_count) as flow:
            before_count = flow.get_successful_job_count() + flow.get_failed_job_count()
            flow.set_params(
                logger=logger,
                log_interval=1,
                log_periodically=True
            )
            flow.add_function(returns_item)
            flow.add_function(returns_item)

            for output in flow:
                pass

            count = flow.get_successful_job_count() + flow.get_failed_job_count()

        self.assertEqual(before_count, expected_before_count)
        self.assertEqual(count, expected_count)

    def test_no_function_arguments(self):
        try:
            with MultithreadedFlow(iterator, 1) as flow:
                flow.add_function()
                for output in flow:
                    pass
            self.fail('Did not throw an exception')
        except Exception as e:
            self.assertIsInstance(e, FlowException)

    def test_no_function_arguments_with_name(self):
        try:
            with MultithreadedFlow(iterator, 1) as flow:
                flow.add_function('name')
                for output in flow:
                    pass
            self.fail('Did not throw an exception')
        except Exception as e:
            self.assertIsInstance(e, FlowException)

    def test_add_wrong_type_function(self):
        try:
            with MultithreadedFlow(iterator, 1) as flow:
                flow.add_function(1)
                for output in flow:
                    pass
            self.fail('Did not throw an exception')
        except Exception as e:
            self.assertIsInstance(e, FlowException)

    def test_add_wrong_type_function_with_name(self):
        try:
            with MultithreadedFlow(iterator, 1) as flow:
                flow.add_function('name', 1)
                for output in flow:
                    pass
            self.fail('Did not throw an exception')
        except Exception as e:
            self.assertIsInstance(e, FlowException)

    def test_flow_two_functions_callable(self):
        expected_count = 5
        items = []
        with MultithreadedFlow(iterator, expected_count) as flow:
            flow.add_function('add one', add_one)
            flow.add_function('add two', add_two)

            for output in flow:
                items.append(output.get_result())

        for i in range(3, expected_count + 3):
            self.assertIn(i, items)

    def test_flow_two_functions_iterable(self):
        expected_count = 5
        items = []
        with MultithreadedFlow([0, 1, 2, 3, 4]) as flow:
            flow.add_function('add one', add_one)
            flow.add_function('add two', add_two)

            for output in flow:
                items.append(output.get_result())

        for i in range(3, expected_count + 3):
            self.assertIn(i, items)

    def test_flow_with_args_and_kwargs(self):
        expected_count = 5
        items = []
        with MultithreadedFlow([0, 1, 2, 3, 4]) as flow:
            flow.add_function(add_n, 4)
            flow.add_function(add_n, n=6)

            for output in flow:
                items.append(output.get_result())

        for i in range(10, expected_count + 10):
            self.assertIn(i, items)

    def test_exception_catcher(self):
        expected_count = 6
        error_indices = {0, 2, 4}

        class TestException(MultithreadedGenerator):
            def consumer(self):
                for i in iterator(expected_count):
                    self.submit_job(even_throw_exception, i)

        try:
            with TestException(catch_exception=True) as test_exception:
                for output in test_exception:
                    if not output:
                        error_parts = str(output.get_exception()).split('|')

                        self.assertEqual('Failed because it is an even number', error_parts[0])
                        self.assertIn(int(error_parts[1]), error_indices)

                success_count = test_exception.get_successful_job_count()
                failed_count = test_exception.get_failed_job_count()

            self.assertEqual(success_count, 3)
            self.assertEqual(failed_count, 3)

        except Exception as e:
            # doesn't actually properly catch exception and cause the test case to fail since it is a threaded error
            self.fail(e)

    def test_flow_handle_and_throw_exception(self):
        def exception_handler(exception, value):
            raise exception

        expected_success = 5
        expected_failed = 5
        expected_count = expected_success + expected_failed

        with MultithreadedFlow(iterator, expected_count) as flow:
            flow.set_params(catch_exception=True)
            flow.add_function(even_throw_exception).error_handler(exception_handler)

            for output in flow:
                if not output:
                    self.assertIsInstance(output.get_exception(), CustomException)

            success_count = flow.get_successful_job_count()
            failed_count = flow.get_failed_job_count()

        self.assertEqual(success_count, expected_success)
        self.assertEqual(failed_count, expected_failed)

    def test_flow_handle_and_catch_exception(self):
        def exception_handler(exception, value):
            if value == 2:
                raise exception
            elif value == 8:
                return exception
            else:
                return value

        expected_success = 8
        expected_failed = 2
        expected_count = expected_success + expected_failed

        with MultithreadedFlow(iterator, expected_count) as flow:
            flow.set_params(catch_exception=True)
            flow.add_function(even_throw_exception).error_handler(exception_handler)

            for output in flow:
                if not output:
                    self.assertIsInstance(output.get_exception(), CustomException)

            success_count = flow.get_successful_job_count()
            failed_count = flow.get_failed_job_count()

        self.assertEqual(success_count, expected_success)
        self.assertEqual(failed_count, expected_failed)

    def test_flow_upstream_error(self):
        def exception_handler(exception, value):
            if value == 2:
                return exception
            else:
                return value

        valid_results = {4, 5, 7, 8, 9, 10, 11}

        with MultithreadedFlow(iterator, 8) as flow:
            flow.set_params(catch_exception=True)
            flow.add_function(returns_item)
            flow.add_function(even_throw_exception).error_handler(exception_handler)
            flow.add_function(add_n, n=4)

            for output in flow:
                if output:
                    self.assertIn(output.get_result(), valid_results)
                else:
                    self.assertIsInstance(output.get_exception(), CustomException)

                    error_parts = str(output.get_exception()).split('|')
                    self.assertEqual('Failed because it is an even number', error_parts[0])
                    self.assertEqual(int(error_parts[1]), 2)

            success_count = flow.get_successful_job_count()
            failed_count = flow.get_failed_job_count()

        self.assertEqual(success_count, 7)
        self.assertEqual(failed_count, 1)

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
        def sleep_mod(value):
            sleep_time = value % 5
            time.sleep(sleep_time)

            return sleep_time

        log_name = 'test'
        logger = get_logger(log_name)

        log_regex = re.compile(r'^ \d+ job[s]? completed successfully\. \d+ job[s]? failed\.$')

        expected_count = 25

        with self.assertLogs(logger, level=logging.INFO) as l:
            with MultithreadedFlow(iterator, expected_count) as flow:
                flow.set_params(
                    max_workers=100,
                    logger=logger,
                    log_interval=1,
                    log_periodically=True
                )
                flow.add_function('fn1', sleep_mod)
                flow.add_function('fn2', sleep_mod)

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
                if log_parts[2].startswith('fn1'):
                    self.assertEqual(log_parts[2], 'fn1 (0)')
                else:
                    self.assertEqual(log_parts[2], 'fn2 (1)')
                self.assertIsNotNone(log_regex.match(log_parts[3]), log_parts[2])
