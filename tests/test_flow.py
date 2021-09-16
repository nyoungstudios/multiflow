import logging
import io
import math
from parameterized import parameterized
import re
import sys
import threading
import time
import unittest


from multiflow import MultithreadedGeneratorBase, MultithreadedGenerator, MultithreadedFlow, FlowException
from tests.setup_logger import get_logger


PERIODIC_LOG_REGEX = r'^ \d+ job[s]? completed successfully\. \d+ job[s]? failed\.$'


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


def sleep_mod(value):
    sleep_time = value % 5
    time.sleep(sleep_time)

    return sleep_time


class TestFlowBase(unittest.TestCase):
    def setUp(self):
        self.thread_count = threading.active_count()
        self.assertEqual(1, self.thread_count)

    def tearDown(self):
        final_thread_count = threading.active_count()
        self.assertEqual(self.thread_count, final_thread_count)
        self.assertEqual(1, self.thread_count)
        self.thread_count = None


class TestFlowFlowBase(TestFlowBase):
    def test_flow_no_consumer_fn(self):
        try:
            with MultithreadedFlow() as flow:
                flow.consume(iterator, 500)
                for output in flow:
                    pass
            self.fail('Did not throw an exception')
        except Exception as e:
            self.assertIsInstance(e, FlowException)

    def test_flow_no_input_to_consume(self):
        try:
            with MultithreadedFlow() as flow:
                flow.consume()
            self.fail('Did not throw an exception')
        except Exception as e:
            self.assertIsInstance(e, FlowException)
            self.assertEqual('Must provide a function or iterable item to consume.', str(e))

    def test_flow_no_return_fn_to_consume(self):
        def nothing():
            pass

        try:
            with MultithreadedFlow() as flow:
                flow.consume(nothing)
                flow.add_function(add_one)

                for output in flow:
                    pass

            self.fail('Did not throw an exception')
        except Exception as e:
            self.assertIsInstance(e, FlowException)
            self.assertEqual('Function does not return or yield anything.', str(e))

    def test_flow_no_input_or_consuming_fn(self):
        try:
            with MultithreadedFlow() as flow:
                for output in flow:
                    pass
            self.fail('Did not throw an exception')
        except Exception as e:
            self.assertIsInstance(e, FlowException)
            self.assertEqual('Must add at least one consuming function.', str(e))

    def test_no_consumer_base(self):
        try:
            with MultithreadedGeneratorBase() as flow:
                for output in flow:
                    pass
            self.fail('Did not throw an exception')
        except Exception as e:
            self.assertIsInstance(e, FlowException)
            self.assertEqual('Must set the consumer function.', str(e))

    def test_not_iterable(self):
        try:
            with MultithreadedFlow() as flow:
                flow.consume(None)
                flow.add_function(returns_item)

                for output in flow:
                    pass

                self.fail('Did not throw an exception')
        except Exception as e:
            self.assertIsInstance(e, FlowException)
            self.assertEqual('First item must be an iterable item or function returning an iterator.', str(e))

    def test_flow_no_iterator_to_consume(self):
        try:
            with MultithreadedFlow() as flow:
                flow.add_function(returns_item)

                for output in flow:
                    pass

            self.fail('Did not throw an exception')
        except Exception as e:
            self.assertIsInstance(e, FlowException)
            self.assertEqual('Must call consume() to consume an iterable function or iterable item.', str(e))

    def test_zero_items(self):
        logger = get_logger('test')

        expected_before_count = 0
        expected_count = 0
        with MultithreadedFlow(
            logger=logger,
            log_interval=1,
            log_periodically=True
        ) as flow:
            flow.consume(iterator, expected_count)
            before_count = flow.get_successful_job_count() + flow.get_failed_job_count()
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
        with MultithreadedFlow() as flow:
            flow.consume(iterator, expected_count)
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
            with MultithreadedFlow() as flow:
                flow.consume(iterator, 1)
                flow.add_function()
                for output in flow:
                    pass
            self.fail('Did not throw an exception')
        except Exception as e:
            self.assertIsInstance(e, FlowException)
            self.assertEqual('Must provide a function to add to the process flow.', str(e))

    def test_no_function_arguments_with_name(self):
        try:
            with MultithreadedFlow() as flow:
                flow.consume(iterator, 1)
                flow.add_function('name')
                for output in flow:
                    pass
            self.fail('Did not throw an exception')
        except Exception as e:
            self.assertIsInstance(e, FlowException)
            self.assertEqual('If the first argument is of type string, must provide a second argument that is a callable function.', str(e))

    def test_add_wrong_type_function(self):
        try:
            with MultithreadedFlow() as flow:
                flow.consume(iterator, 1)
                flow.add_function(1)
                for output in flow:
                    pass
            self.fail('Did not throw an exception')
        except Exception as e:
            self.assertIsInstance(e, FlowException)
            self.assertTrue(str(e).startswith('The first argument must be a string or callable function, not of type '))

    def test_add_wrong_type_function_with_name(self):
        try:
            with MultithreadedFlow() as flow:
                flow.consume(iterator, 1)
                flow.add_function('name', 1)
                for output in flow:
                    pass
            self.fail('Did not throw an exception')
        except Exception as e:
            self.assertIsInstance(e, FlowException)
            self.assertEqual('If first argument is of type string, the second argument must be a callable function.', str(e))

    def test_flow_two_functions_callable(self):
        expected_count = 5
        items = []
        with MultithreadedFlow() as flow:
            flow.consume(iterator, expected_count)
            flow.add_function('add one', add_one)
            flow.add_function('add two', add_two)

            for output in flow:
                items.append(output.get_result())

        for i in range(3, expected_count + 3):
            self.assertIn(i, items)

    def test_flow_two_functions_iterable(self):
        expected_count = 5
        items = []
        with MultithreadedFlow() as flow:
            flow.consume([0, 1, 2, 3, 4])
            flow.add_function('add one', add_one)
            flow.add_function('add two', add_two)

            for output in flow:
                items.append(output.get_result())

        for i in range(3, expected_count + 3):
            self.assertIn(i, items)

    def test_flow_with_args_and_kwargs(self):
        expected_count = 5
        items = []
        with MultithreadedFlow() as flow:
            flow.consume([0, 1, 2, 3, 4])
            flow.add_function(add_n, 4)
            flow.add_function(add_n, n=6)

            for output in flow:
                items.append(output.get_result())

        for i in range(10, expected_count + 10):
            self.assertIn(i, items)

    def test_flow_expand_args(self):
        def return_pair(x):
            return x, x

        expected_count = 10
        expected_results = {2, 4, 6, 8, 10, 12, 14, 16, 18, 20}

        with MultithreadedFlow() as flow:
            flow.consume(iterator, expected_count)
            flow.add_function(add_one)
            flow.add_function(return_pair)
            flow.add_function(add_n).expand_params()

            for i, output in enumerate(flow):
                self.assertIn(output.get_result(), expected_results)

            count = flow.get_successful_job_count()

        self.assertEqual(expected_count, count)

    def test_flow_expand_kwargs(self):
        def return_kwargs(x):
            return {'value': x, 'less_than_five': x < 5}

        def conditional_multiply(value=0, less_than_five=True):
            if less_than_five:
                return value - 5
            else:
                return value + 5

        expected_count = 10

        with MultithreadedFlow() as flow:
            flow.consume(iterator, expected_count)
            flow.add_function(return_kwargs)
            flow.add_function(conditional_multiply).expand_params()

            for i, output in enumerate(flow):
                if i < 5:
                    self.assertEqual(i - 5, output.get_result())
                else:
                    self.assertEqual(i + 5, output.get_result())

            count = flow.get_successful_job_count()

        self.assertEqual(expected_count, count)

    def test_flow_expand_args_and_kwargs(self):
        def return_args_and_kwargs(x):
            return (x, x + 1, x + 2, {'item': x % 3})

        def pick_item(v1, v2, v3, item=0):
            if item == 0:
                return v1
            elif item == 1:
                return v2
            else:
                return v3

        expected_count = 10

        with MultithreadedFlow() as flow:
            flow.consume(iterator, expected_count)
            flow.add_function(return_args_and_kwargs)
            flow.add_function(pick_item).expand_params()

            for i, output in enumerate(flow):
                expected_value = i + (i % 3)
                self.assertEqual(expected_value, output.get_result())

            count = flow.get_successful_job_count()

        self.assertEqual(expected_count, count)

    def test_flow_expand_args_and_kwargs_with_error(self):
        error_msg = 'Special error'
        value1 = 'foo'
        value2 = 'bar'

        def return_args_and_kwargs(x):
            return (9, x, {'value': value1, 'another_value': value2})

        def divide_nums(numerator, denominator, value=None, another_value=None):
            v = numerator / denominator
            if v == 1:
                raise CustomException(error_msg)

            if v >= 2:
                return value
            else:
                return another_value

        def handle_error(error, numerator, denominator, value=None, another_value=None):
            if isinstance(error, ZeroDivisionError):
                return value + another_value
            else:
                raise error

        expected_success = 9
        expected_failed = 1
        expected_count = expected_success + expected_failed

        with MultithreadedFlow(quiet_traceback=True) as flow:
            flow.consume(iterator, expected_count)
            flow.add_function(return_args_and_kwargs)
            flow.add_function(divide_nums).expand_params().error_handler(handle_error)

            value1_count = 0
            value2_count = 0
            combined_value_count = 0

            for output in flow:
                if output:
                    if value1 == output.get_result():
                        value1_count += 1
                    elif value2 == output.get_result():
                        value2_count += 1
                    elif value1 + value2 == output.get_result():
                        combined_value_count += 1
                else:
                    self.assertEqual(error_msg, str(output.get_exception()))

            self.assertEqual(4, value1_count)
            self.assertEqual(4, value2_count)
            self.assertEqual(1, combined_value_count)

            success_count = flow.get_successful_job_count()
            failed_count = flow.get_failed_job_count()

        self.assertEqual(expected_success, success_count)
        self.assertEqual(expected_failed, failed_count)

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

        with MultithreadedFlow(quiet_traceback=True) as flow:
            flow.consume(iterator, expected_count)
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

        with MultithreadedFlow(quiet_traceback=True) as flow:
            flow.consume(iterator, expected_count)
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

        with MultithreadedFlow(quiet_traceback=True) as flow:
            flow.consume(iterator, 8)
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

        with MultithreadedFlow() as flow:
            flow.consume(iterator, 8)
            flow.add_function(add_one)
            flow.add_function(exit_on_even)
            flow.add_function(add_two)

            for output in flow:
                if output.get_result() is not None:
                    self.assertIn(output.get_result(), options)
                    self.assertEqual(output.get_fn_id(), 2)
                else:
                    self.assertEqual(output.get_fn_id(), 1)

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

        with self.assertLogs(logger, level=logging.INFO) as log:
            with TestException(
                logger=logger,
                retry_count=2,
                quiet_traceback=True,
                log_retry=True,
                log_error=True
            ) as test_exception:
                for output in test_exception:
                    self.assertEqual(3, output.get_num_of_attempts())

                success_count = test_exception.get_successful_job_count()
                failed_count = test_exception.get_failed_job_count()

            self.assertEqual(0, success_count)
            self.assertEqual(1, failed_count)

            expected_logs = [
                'WARNING:{}:{}(): Retrying job after catching exception: {}'
                    .format(log_name, throw_exception.__name__, exception_str),
                'WARNING:{}:{}(): Retrying job after catching exception: {}'
                    .format(log_name, throw_exception.__name__, exception_str),
                'ERROR:{}:{}(): Job failed with exception: {}'
                    .format(log_name, throw_exception.__name__, exception_str)
            ]

            self.assertEqual(expected_logs, log.output)

    def test_flow_log_error(self):
        log_name = 'test'
        logger = get_logger(log_name)
        exception_str = 'This is an exception'

        def output_error_msg(n):
            for _ in range(n):
                yield exception_str

        expected_failed = 10

        with self.assertLogs(logger, level=logging.INFO) as log:
            with MultithreadedFlow(logger=logger, log_error=True, quiet_traceback=True) as flow:
                flow.consume(output_error_msg, expected_failed)
                flow.add_function(throw_exception)

                for output in flow:
                    self.assertEqual(1, output.get_num_of_attempts())

                failed_count = flow.get_failed_job_count()


            self.assertEqual(expected_failed, failed_count)

            expected_log = 'ERROR:{}:throw_exception: Job failed with exception: {}'.format(log_name, exception_str)
            expected_logs = [expected_log] * expected_failed

            self.assertEqual(expected_logs, log.output)

    def test_log_traceback(self):
        log_name = 'test'
        logger = get_logger(log_name)
        exception_str = 'This is an exception'

        class TestException(MultithreadedGenerator):
            def consumer(self):
                self.submit_job(throw_exception, exception_str)

        with self.assertLogs(logger, level=logging.INFO) as log:
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

            expected_log_error = 'ERROR:{}:{}(): Job failed with exception: {}'.format(log_name, throw_exception.__name__, exception_str)
            traceback_first_line = 'Traceback (most recent call last):'

            lines = log.output[0].split('\n')

            self.assertEqual(expected_log_error, lines[0])
            self.assertEqual(traceback_first_line, lines[1])
            self.assertIn(exception_str, lines[-1])

    def test_log_summary(self):
        log_name = 'test'
        logger = get_logger(log_name)

        expected_count = 25

        with self.assertLogs(logger, level=logging.INFO) as log:
            with MultithreadedFlow(logger=logger, log_summary=True) as flow:
                flow.consume(iterator, expected_count)
                flow.add_function(returns_item)

                for output in flow:
                    pass

                count = flow.get_successful_job_count()

            self.assertEqual(expected_count, count)


            expected_logs = [
                'INFO:{}:{}: {} jobs completed successfully. 0 jobs failed.'
                    .format(log_name, returns_item.__name__, expected_count)
            ]

            self.assertEqual(expected_logs, log.output)

    def test_log_all(self):
        log_name = 'test'
        logger = get_logger(log_name)

        def sleeper(x):
            time.sleep(1)
            if x == 4:
                raise CustomException('failed')
            return x

        expected_success = 24
        expected_failed = 1
        expected_count = expected_success + expected_failed
        with self.assertLogs(logger, level=logging.INFO) as log:
            with MultithreadedFlow(
                max_workers=4,
                logger=logger,
                log_interval=1,
                retry_count=1,
                log_all=True,
                quiet_traceback=True
            ) as flow:
                flow.consume(iterator, expected_count)
                flow.add_function(sleeper)

                for output in flow:
                    pass

                success_count = flow.get_successful_job_count()
                failed_count = flow.get_failed_job_count()

            has_one_periodic = False
            has_one_warning = False
            has_one_error = False

            expected_warning_log = 'WARNING:{}:{}: Retrying job after catching exception: failed'.format(
                log_name, sleeper.__name__
            )

            expected_error_log = 'ERROR:{}:{}: Job failed with exception: failed'.format(
                log_name, sleeper.__name__
            )

            log_regex = re.compile(PERIODIC_LOG_REGEX)

            for log_msg in log.output[:-1]:
                if log_msg == expected_warning_log:
                    has_one_warning = True
                elif log_msg == expected_error_log:
                    has_one_error = True
                else:
                    log_parts = log_msg.split(':')
                    has_one_periodic = True
                    self.assertEqual('INFO', log_parts[0])
                    self.assertEqual(log_name, log_parts[1])
                    self.assertEqual(sleeper.__name__, log_parts[2])
                    self.assertIsNotNone(log_regex.match(log_parts[3]), log_parts[3])


            expected_summary_log = 'INFO:{}:{}: {} jobs completed successfully. {} job failed.'.format(
                log_name, sleeper.__name__, expected_success, expected_failed
            )

            self.assertEqual(expected_summary_log, log.output[-1])

            self.assertTrue(has_one_periodic)
            self.assertTrue(has_one_warning)
            self.assertTrue(has_one_error)


            self.assertEqual(expected_success, success_count)
            self.assertEqual(expected_failed, failed_count)

    def test_print_traceback(self):
        exception_str = 'testing 1, 2, 3'

        old_stderr = sys.stderr
        redirected_error = sys.stderr = io.StringIO()

        with MultithreadedFlow() as flow:
            flow.consume([exception_str])
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
        thread_prefix = 'qwerty'

        def get_thread_name(value):
            self.assertTrue(threading.currentThread().getName().startswith(thread_prefix))

            return value

        expected_count = 10

        with MultithreadedFlow(thread_prefix=thread_prefix) as flow:
            flow.consume(iterator, expected_count)
            flow.add_function(get_thread_name)

            for output in flow:
                pass

            count = flow.get_successful_job_count()

        self.assertEqual(expected_count, count)

    def test_flow_add_exception(self):
        try:
            flow = MultithreadedFlow()

            new_flow = flow + 1
            self.fail('Did not throw an exception')
        except Exception as e:
            self.assertIsInstance(e, FlowException)

    def test_flow_add(self):
        flow1 = MultithreadedFlow()
        flow1.add_function(add_one)
        flow1.add_function(add_two)

        flow2 = MultithreadedFlow()
        flow2.add_function(add_n, n=15)

        final_flow = flow1 + flow2

        expected_count = 10
        with final_flow:
            final_flow.consume(iterator, expected_count)

            for output in final_flow:
                self.assertGreaterEqual(output.get_result(), 18)
                self.assertLess(output.get_result(), expected_count + 18)

            count = final_flow.get_successful_job_count()

        self.assertEqual(expected_count, count)

    def test_flow_add_kwargs(self):
        flow1 = MultithreadedFlow(retry_count=1, quiet_traceback=True, sleep_seed=0)
        flow1.add_function(add_one)
        flow1.add_function(even_throw_exception)

        flow2 = MultithreadedFlow(retry_count=2, quiet_traceback=True, sleep_seed=0)
        flow2.add_function(throw_exception)

        final_flow = flow1 + flow2

        expected_count = 10
        with final_flow:
            final_flow.consume(iterator, expected_count)

            for output in final_flow:
                self.assertFalse(output.is_successful())
                if output.get_fn_id() == 1:
                    self.assertEqual(2, output.get_num_of_attempts())
                else:
                    self.assertEqual(3, output.get_num_of_attempts())

    def test_flow_job_output_args_and_kwargs(self):
        expected_count = 10
        with MultithreadedFlow() as flow:
            flow.consume(iterator, expected_count)
            flow.add_function(add_n, n=5)

            for output in flow:
                self.assertEqual(5, getattr(output, 'n'))
                self.assertEqual(output[0] + 5, output.get_result())
                self.assertEqual(output.get('value') + 5, output.get_result())

            count = flow.get_successful_job_count()

        self.assertEqual(expected_count, count)

    def test_flow_job_output_kwargs_as_args(self):
        def do_nothing(x=0):
            return x

        expected_count = 10
        with MultithreadedFlow() as flow:
            flow.consume(iterator, expected_count)
            flow.add_function(do_nothing)

            for output in flow:
                self.assertEqual(output[0], output.get_result())
                self.assertEqual(output.get('x'), output.get_result())

            count = flow.get_successful_job_count()

        self.assertEqual(expected_count, count)

    def test_flow_job_output_args_and_kwargs_failed(self):
        def even_throw_exception_and_add(value, n=0):
            if value % 2 == 0:
                raise CustomException('Failed because it is an even number|{}'.format(value))
            else:
                return value + n

        expected_success = 5
        expected_failed = 5
        expected_count = expected_success + expected_failed
        with MultithreadedFlow(quiet_traceback=True) as flow:
            flow.consume(iterator, expected_count)
            flow.add_function(even_throw_exception_and_add, n=5)

            for output in flow:
                self.assertEqual(5, output.get('n'))
                if output[0] % 2 == 0:
                    self.assertFalse(output)
                else:
                    self.assertTrue(output)
                    self.assertEqual(output[0] + 5, output.get_result())
                    self.assertEqual(output.get('value') + 5, output.get_result())

            success_count = flow.get_successful_job_count()
            failed_count = flow.get_failed_job_count()

        self.assertEqual(expected_success, success_count)
        self.assertEqual(expected_failed, failed_count)

    def test_flow_job_output_default_kwargs(self):
        def default_fn(x, y=4):
            return x

        expected_count = 10
        with MultithreadedFlow() as flow:
            flow.consume(iterator, expected_count)
            flow.add_function(default_fn)

            for output in flow:
                self.assertEqual(4, output.get('y'))
                self.assertEqual(output.get_result(), output.get('x'))

            success_count = flow.get_successful_job_count()

        self.assertEqual(expected_count, success_count)

    def test_flow_pass_parent(self):
        def fn1(x, y=5):
            return x

        def fn2(x, y=0):
            return x + y

        expected_count = 10
        with MultithreadedFlow() as flow:
            flow.consume(iterator, expected_count)
            flow.add_function(fn1)
            flow.add_function(fn2).pass_parent()

            for output in flow:
                self.assertEqual(5, output.get('y'))
                self.assertEqual(output.get('x') + output.get('y'), output.get_result())

            success_count = flow.get_successful_job_count()

        self.assertEqual(expected_count, success_count)

    def test_flow_not_pass_parent(self):
        def fn1(x, y=5):
            return x

        def fn2(x, y=0):
            return x + y

        expected_count = 10
        with MultithreadedFlow() as flow:
            flow.consume(iterator, expected_count)
            flow.add_function(fn1)
            flow.add_function(fn2)

            for output in flow:
                self.assertEqual(0, output.get('y'))
                self.assertEqual(output.get('x') + output.get('y'), output.get_result())

            success_count = flow.get_successful_job_count()

        self.assertEqual(expected_count, success_count)

    def test_flow_pass_parent_with_kwargs(self):
        def fn1(x, y=5, z=7):
            return {'x': x, 'z': 1}

        def fn2(x, y=0, z=0):
            return x * y + z

        expected_count = 10
        with MultithreadedFlow() as flow:
            flow.consume(iterator, expected_count)
            flow.add_function(fn1)
            flow.add_function(fn2).pass_parent().expand_params()

            for output in flow:
                self.assertEqual(1, output.get('z'))
                self.assertEqual(5, output.get('y'))
                expected_out = output.get('x') * output.get('y') + output.get('z')
                self.assertEqual(expected_out, output.get_result())

            success_count = flow.get_successful_job_count()

        self.assertEqual(expected_count, success_count)

    def test_flow_pass_parent_index_output(self):
        def fn1(x, y=5):
            return x

        def fn2(x, y):
            return x + y

        expected_count = 10
        with MultithreadedFlow() as flow:
            flow.consume(iterator, expected_count)
            flow.add_function(fn1)
            flow.add_function(fn2).pass_parent()

            for output in flow:
                self.assertEqual(5, output.get('y'))
                self.assertEqual(5, output[1])
                self.assertEqual(output.get('x') + output.get('y'), output.get_result())

            success_count = flow.get_successful_job_count()

        self.assertEqual(expected_count, success_count)

    def test_flow_index_output_expand_kwargs(self):
        def fn1(a, b=5):
            return {'x': a, 'y': b}

        def fn2(x, y):
            return x + y

        expected_count = 10
        with MultithreadedFlow() as flow:
            flow.consume(iterator, expected_count)
            flow.add_function(fn1)
            flow.add_function(fn2).expand_params()

            for output in flow:
                self.assertEqual(5, output.get('y'))
                self.assertEqual(5, output[1])
                self.assertEqual(output.get('x'), output[0])
                self.assertEqual(output.get('x') + output.get('y'), output.get_result())

            success_count = flow.get_successful_job_count()

        self.assertEqual(expected_count, success_count)


class TestFlowParameterizedFlowBase(TestFlowBase):
    @parameterized.expand([
        ('log_only_last', True),
        ('log_all', False)
    ])
    def test_periodic_logger(self, name, log_only_last):
        log_name = 'test'
        logger = get_logger(log_name)

        log_regex = re.compile(PERIODIC_LOG_REGEX)

        expected_count = 15

        with self.assertLogs(logger, level=logging.INFO) as log:
            with MultithreadedFlow(
                max_workers=100,
                logger=logger,
                log_interval=1,
                log_periodically=True,
                log_only_last=log_only_last
            ) as flow:
                flow.consume(iterator, expected_count)
                flow.add_function('fn1', sleep_mod)
                flow.add_function('fn2', sleep_mod)

                for output in flow:
                    pass

                count = flow.get_successful_job_count()

            self.assertEqual(expected_count, count)

            if not log.output:
                self.fail('No periodic logs were recorded')

            for log_statement in log.output:
                log_parts = log_statement.split(':')
                self.assertEqual('INFO', log_parts[0])
                self.assertEqual(log_name, log_parts[1])
                if not log_only_last and log_parts[2].startswith('fn1'):
                    self.assertEqual('fn1 (0)', log_parts[2])
                else:
                    self.assertEqual('fn2 (1)', log_parts[2])
                self.assertIsNotNone(log_regex.match(log_parts[3]), log_parts[3])

    @parameterized.expand([
        ('format_map', '{name}-{fid}. Abc {success} job{s_plural} so far. Xyz {failed} job{f_plural} so far.'),
        ('percent_str', '%(name)s-%(fid)s. Abc %(success)s job%(s_plural)s so far. Xyz %(failed)s job%(f_plural)s so far.')
    ])
    def test_periodic_logger_custom_format(self, name, log_format):
        log_name = 'test'
        logger = get_logger(log_name)

        log_regex = re.compile(r'^sleep_mod-[0-1]\. Abc \d+ job[s]? so far\. Xyz \d+ job[s]? so far\.$')

        expected_count = 10

        with self.assertLogs(logger, level=logging.INFO) as log:
            with MultithreadedFlow(
                max_workers=100,
                logger=logger,
                log_interval=1,
                log_periodically=True,
                log_format=log_format
            ) as flow:
                flow.consume(iterator, expected_count)
                flow.add_function(sleep_mod)
                flow.add_function(sleep_mod)

                for output in flow:
                    pass

                count = flow.get_successful_job_count()

            self.assertEqual(expected_count, count)

            if not log.output:
                self.fail('No periodic logs were recorded')

            for log_statement in log.output:
                log_parts = log_statement.split(':')
                self.assertEqual('INFO', log_parts[0])
                self.assertEqual(log_name, log_parts[1])
                self.assertIsNotNone(log_regex.match(log_parts[2]), log_parts[2])

    @parameterized.expand([
        ('format_map', '{fail}'),
        ('percent_str', '%(fail)s')
    ])
    def test_flow_log_format_key_error(self, name, log_format):
        try:
            with MultithreadedFlow(log_format=log_format) as flow:
                flow.add_function(sleep_mod)

                self.fail('Did not throw KeyError Exception')

        except Exception as e:
            self.assertIsInstance(e, KeyError)


class TestFlowExitParameterized(TestFlowBase):
    @parameterized.expand([
        ('one', 1),
        ('two', 2)
    ])
    def test_flow_exit(self, name, items):
        def sleep_one(x):
            time.sleep(1)
            return x

        log_name = 'test'
        logger = get_logger(log_name)

        expected_count = 2
        start = time.time()

        with self.assertLogs(logger, level=logging.INFO):
            with MultithreadedFlow(max_workers=4, logger=logger, log_periodically=True, log_interval=1) as flow:
                flow.consume(iterator, 10000)
                for _ in range(items):
                    flow.add_function(sleep_one)

                for i, output in enumerate(flow):
                    if i == expected_count - 1:
                        break

                count = flow.get_successful_job_count()

            elapsed_time = time.time() - start
            upper_time_bound = 2 + items
            self.assertLessEqual(math.floor(elapsed_time), upper_time_bound)

            self.assertEqual(expected_count, count)
