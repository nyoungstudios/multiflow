import logging
import io
import re
import sys
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


def throw_exception(msg):
    raise CustomException(msg)


def even_throw_exception(value):
    if value % 2 == 0:
        raise CustomException('Failed because it is an even number|{}'.format(value))
    else:
        return value


class TestFlow(unittest.TestCase):
    def setUp(self):
        self.thread_count = threading.active_count()
        self.assertEqual(1, self.thread_count)

    def tearDown(self):
        final_thread_count = threading.active_count()
        self.assertEqual(self.thread_count, final_thread_count)
        self.assertEqual(1, self.thread_count)
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

        self.assertEqual(expected_before_count, before_count)
        self.assertEqual(expected_count, count)

    def test_base_a_lot_of_items(self):
        expected_count = 5000
        with MultithreadedGeneratorBase() as flow:
            def consumer():
                for i in iterator(expected_count):
                    flow.submit_job(add_n, i, 1)

            flow.set_consumer(consumer)

            for output in flow:
                if output:
                    self.assertGreaterEqual(output.get_result(), 1)
                    self.assertLess(output.get_result(), expected_count + 1)

            success_count = flow.get_successful_job_count()

        self.assertEqual(expected_count, success_count)

    def test_flow_a_lot_of_items(self):
        expected_count = 5000
        expected_min = 4
        expected_max = expected_count + expected_min
        with MultithreadedFlow(iterator, expected_count) as flow:
            for _ in range(expected_min):
                flow.add_function(add_one)

            for output in flow:
                if output:
                    self.assertGreaterEqual(output.get_result(), expected_min)
                    self.assertLess(output.get_result(), expected_max)

            success_count = flow.get_successful_job_count()

        self.assertEqual(expected_count, success_count)

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

        with TestException(quiet_traceback=True) as test_exception:
            for output in test_exception:
                if not output:
                    error_parts = str(output.get_exception()).split('|')

                    self.assertEqual('Failed because it is an even number', error_parts[0])
                    self.assertIn(int(error_parts[1]), error_indices)

            success_count = test_exception.get_successful_job_count()
            failed_count = test_exception.get_failed_job_count()

        self.assertEqual(3, success_count)
        self.assertEqual(3, failed_count)

    def test_flow_handle_and_throw_exception(self):
        def exception_handler(exception, value):
            raise exception

        expected_success = 5
        expected_failed = 5
        expected_count = expected_success + expected_failed

        with MultithreadedFlow(iterator, expected_count) as flow:
            flow.set_params(quiet_traceback=True)
            flow.add_function(even_throw_exception).error_handler(exception_handler)

            for output in flow:
                if not output:
                    self.assertIsInstance(output.get_exception(), CustomException)

            success_count = flow.get_successful_job_count()
            failed_count = flow.get_failed_job_count()

        self.assertEqual(expected_success, success_count)
        self.assertEqual(expected_failed, failed_count)

    def test_flow_handle_and_catch_exception(self):
        def exception_handler(exception, value):
            if value == 2 or value == 4:
                raise exception
            else:
                return value

        expected_success = 8
        expected_failed = 2
        expected_count = expected_success + expected_failed

        with MultithreadedFlow(iterator, expected_count) as flow:
            flow.set_params(quiet_traceback=True)
            flow.add_function(even_throw_exception).error_handler(exception_handler)

            for output in flow:
                if not output:
                    self.assertIsInstance(output.get_exception(), CustomException)

            success_count = flow.get_successful_job_count()
            failed_count = flow.get_failed_job_count()

        self.assertEqual(expected_success, success_count)
        self.assertEqual(expected_failed, failed_count)

    def test_flow_upstream_error(self):
        def exception_handler(exception, value):
            if value == 2:
                raise exception
            else:
                return value

        valid_results = {4, 5, 7, 8, 9, 10, 11}

        with MultithreadedFlow(iterator, 8) as flow:
            flow.set_params(quiet_traceback=True)
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
                    self.assertEqual(2, int(error_parts[1]))

            success_count = flow.get_successful_job_count()
            failed_count = flow.get_failed_job_count()

        self.assertEqual(7, success_count)
        self.assertEqual(1, failed_count)

    def test_flow_upstream_branch(self):
        def exit_on_even(value):
            if value % 2 != 0:
                return value

        options = {3, 5, 7, 9}

        with MultithreadedFlow(iterator, 8) as flow:
            flow.add_function(add_one)
            flow.add_function(exit_on_even)
            flow.add_function(add_two)

            for output in flow:
                if output.get_result() is not None:
                    self.assertIn(output.get_result(), options)
                    self.assertEqual(output.get_job_id(), 2)
                else:
                    self.assertEqual(output.get_job_id(), 1)

            success_count = flow.get_successful_job_count()
            failed_count = flow.get_failed_job_count()

        self.assertEqual(8, success_count)
        self.assertEqual(0, failed_count)

    def test_log_errors(self):
        log_name = 'test'
        logger = get_logger(log_name)
        exception_str = 'This is an exception'

        class TestException(MultithreadedGenerator):
            def consumer(self):
                self.submit_job(throw_exception, exception_str)

        with self.assertLogs(logger, level=logging.INFO) as l:
            with TestException(
                logger=logger,
                retry_count=2,
                quiet_traceback=True,
                log_warning=True,
                log_error=True
            ) as test_exception:
                for output in test_exception:
                    self.assertEqual(3, output.get_num_of_attempts())

                success_count = test_exception.get_successful_job_count()
                failed_count = test_exception.get_failed_job_count()

            self.assertEqual(0, success_count)
            self.assertEqual(1, failed_count)

            expected_logs = [
                'WARNING:{}:Retrying job after catching exception: {}'.format(log_name, exception_str),
                'WARNING:{}:Retrying job after catching exception: {}'.format(log_name, exception_str),
                'ERROR:{}:Job failed with exception: {}'.format(log_name, exception_str)
            ]

            self.assertEqual(expected_logs, l.output)

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

            self.assertEqual(expected_count, count)

            if not l.output:
                self.fail('No periodic logs were recorded')

            for log_statement in l.output:
                log_parts = log_statement.split(':')
                self.assertEqual('INFO', log_parts[0])
                self.assertEqual(log_name, log_parts[1])
                if log_parts[2].startswith('fn1'):
                    self.assertEqual('fn1 (0)', log_parts[2])
                else:
                    self.assertEqual('fn2 (1)', log_parts[2])
                self.assertIsNotNone(log_regex.match(log_parts[3]), log_parts[2])

    def test_log_traceback(self):
        log_name = 'test'
        logger = get_logger(log_name)
        exception_str = 'This is an exception'

        class TestException(MultithreadedGenerator):
            def consumer(self):
                self.submit_job(throw_exception, exception_str)

        with self.assertLogs(logger, level=logging.INFO) as l:
            with TestException(
                logger=logger,
                log_error=True
            ) as test_exception:
                for output in test_exception:
                    self.assertEqual(1, output.get_num_of_attempts())

                success_count = test_exception.get_successful_job_count()
                failed_count = test_exception.get_failed_job_count()

            self.assertEqual(0, success_count)
            self.assertEqual(1, failed_count)

            expected_log_error = 'ERROR:{}:Job failed with exception: {}'.format(log_name, exception_str)
            traceback_first_line = 'Traceback (most recent call last):'

            lines = l.output[0].split('\n')

            self.assertEqual(expected_log_error, lines[0])
            self.assertEqual(traceback_first_line, lines[1])
            self.assertIn(exception_str, lines[-1])

    def test_print_traceback(self):
        exception_str = 'testing 1, 2, 3'

        old_stderr = sys.stderr
        redirected_error = sys.stderr = io.StringIO()

        with MultithreadedFlow([exception_str]) as flow:
            flow.add_function(throw_exception)

            for output in flow:
                self.assertFalse(output)
                self.assertIsInstance(output.get_exception(), CustomException)

            err = redirected_error.getvalue()

            lines = err.strip().split('\n')

            traceback_first_line = 'Traceback (most recent call last):'

            self.assertEqual(traceback_first_line, lines[0])
            self.assertIn(exception_str, lines[-1])

            sys.stderr = old_stderr

    def test_flow_thread_name_prefix(self):
        log_name = 'test'
        log_format = '%(name)s-%(levelname)s-%(threadName)s-%(message)s'
        unittest.case._AssertLogsContext.LOGGING_FORMAT = log_format
        logger = get_logger(log_name, log_format=log_format)
        thread_prefix = 'qwerty'
        error_msg = 'this is an error message'

        with self.assertLogs(logger, level=logging.INFO) as l:
            with MultithreadedFlow([error_msg]) as flow:
                flow.set_params(
                    logger=logger,
                    quiet_traceback=True,
                    log_error=True,
                    thread_prefix=thread_prefix
                )
                flow.add_function(throw_exception)

                for output in flow:
                    pass

                count = flow.get_failed_job_count()

            self.assertEqual(1, count)

            for log_statement in l.output:
                log_parts = log_statement.split('-')
                self.assertEqual(log_name, log_parts[0])
                self.assertEqual('ERROR', log_parts[1])
                self.assertTrue(log_parts[2].startswith(thread_prefix))
                self.assertNotIn('Multiflow', log_parts[2])
                self.assertIn(error_msg, log_parts[3])
