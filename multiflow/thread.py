"""
Multithreaded consumer/producer generator with periodic logging support and exception catching
"""
from abc import ABC, abstractmethod
from collections import defaultdict
import concurrent.futures
import logging
from queue import Queue
import sys
from threading import Event, Thread
import time
import traceback
from typing import Any, Callable, Generator, Iterable


from .utils import pluralize, use_c_string


_LOG_KWARGS = {
    'success': 0,
    'failed': 0,
    's_plural': 's',
    'f_plural': 's',
    'name': 'fn',
    'fid': 0
}


class _DummyItem:
    def __init__(self):
        """
        Dummy class to signify the last item in the thread pool
        """
        pass


class FlowException(Exception):
    """
    Exception to be raised within the multiflow package
    """
    pass


class FlowFunction:
    def __init__(self, name, fn, *args, **kwargs):
        """
        Holds information about the function to execute in the thread pool

        :param name: the name to be present in the periodic logs
        :param fn: function to call
        :param args: args for the function
        :param kwargs: kwargs for the function
        """
        self.name = name
        self._fn = fn
        self._args = args
        self._kwargs = kwargs

        self._handler = None
        self._expand = False

    def error_handler(self, fn: Callable):
        """
        Adds exception handler to job

        :param fn: The function to handle the exception. This function should accept the exception as the first
            positional argument, the previous task in the process flow's result, and all the args and kwargs passed to
            the job function.
        """
        self._handler = fn

        return self

    def expand_params(self):
        """
        Will expand the args and kwargs passed as an input from the previous function or iterator. So if the return
        value from the previous function or iterator is a tuple, then the items in the tuple will be interpreted as
        arguments in this function. If the last item in the tuple is a dictionary or the return value is a dictionary,
        then those values will be interpreted as kwargs in this function.
        """
        self._expand = True
        return self

    def _calc_args_and_kwargs(self, prev=None):
        """
        Calculates args and kwargs to pass to function. Extra parentheses in return tuple is for <= 3.7 Python support

        :param prev: return value from previous function in process flow
        :return: A tuple with the first item as the args and the second as the additional kwargs
        """
        if prev is None:
            # noinspection PyRedundantParentheses
            return (self._args, self._kwargs)
        else:
            if self._expand and isinstance(prev, tuple):
                if isinstance(prev[-1], dict):
                    # noinspection PyRedundantParentheses
                    return ((*prev[:-1], *self._args), {**prev[-1], **self._kwargs})
                else:
                    # noinspection PyRedundantParentheses
                    return ((*prev, *self._args), self._kwargs)
            elif self._expand and isinstance(prev, dict):
                # noinspection PyRedundantParentheses
                return ((), {**prev, **self._kwargs})
            else:
                # noinspection PyRedundantParentheses
                return ((prev, *self._args), self._kwargs)

    def _handle(self, exception: Exception, prev=None):
        """
        Handles exception thrown by running the function

        :param exception: the exception that was raised from _run
        :param prev: return value from previous function in process flow
        :return: a tuple with the first item as the new exception if it failed or the return value of the error
            handling function. And the second item is the exc info for capturing the traceback of the exception
        """
        if self._handler:
            try:
                args, kwargs = self._calc_args_and_kwargs(prev=prev)
                return self._handler(exception, *args, **kwargs), None
            except Exception as e:
                return e, sys.exc_info()
        else:
            return exception, sys.exc_info()

    def _run(self, *args, **kwargs):
        """
        Runs the function

        :param prev: return value from previous function in process flow
        :return: the return value of the function
        """
        return self._fn(*args, **kwargs)


class StoppableThread(Thread):
    def __init__(self, *args, **kwargs):
        """
        Thread class with a stop event
        """
        Thread.__init__(self, *args, **kwargs)
        self._stop_flag = Event()

    def stop(self):
        self._stop_flag.set()

    def wait(self, interval):
        return self._stop_flag.wait(interval)


class JobOutput:
    def __init__(
        self,
        success: bool,
        attempts: int,
        fn_id: int = 0,
        result: Any = None,
        exception: Exception = None,
        args: tuple = None,
        kwargs: dict = None
    ):
        """
        Data class to hold the output from the MultithreadedGenerator/Multithreadedflow

        :param success: If True, the job was successful; otherwise, it failed
        :param attempts: The number of attempts it ran the job
        :param fn_id: The function id
        :param result: If successful, the output of the job run
        :param exception: If not successful, the exception caught
        :param args: The args passed to the function that was run
        :param kwargs: The kwargs passed to the function that was run
        """
        self._success = success
        self._attempts = attempts
        self._fn_id = fn_id
        self._result = result
        self._exception = exception

        self._args = args if args else ()
        self._kwargs = kwargs if kwargs else {}

    def is_successful(self) -> bool:
        """
        Returns True if the job was successful; otherwise, false
        """
        return self._success

    def get_num_of_attempts(self) -> int:
        """
        Returns the number of attempts it tried to run the job
        """
        return self._attempts

    def get_fn_id(self) -> int:
        """
        Returns the function id
        """
        return self._fn_id

    def get_result(self) -> Any:
        """
        Returns the output of the job if successful; otherwise, None
        """
        return self._result

    def get_exception(self) -> Exception:
        """
        Returns the exception of the job if unsuccessful; otherwise, None
        """
        return self._exception

    def get(self, item):
        """
        Get value of kwargs

        :param item: kwarg key
        :return: value of the kwarg
        """
        return self._kwargs.get(item)

    def __bool__(self):
        return self.is_successful()

    def __getattr__(self, item):
        return self.get(item)

    def __getitem__(self, item):
        return self._args[item]

    def __repr__(self):
        return repr(self.get_result())

    def __str__(self):
        return str(self.get_result())


class MultithreadedGeneratorBase:
    def __init__(
        self,
        max_workers: int = None,
        retry_count: int = 0,
        sleep_seed: int = 1,
        quiet_traceback: bool = False,
        logger: logging.Logger = None,
        log_interval: int = 30,
        log_periodically: bool = False,
        log_retry: bool = False,
        log_error: bool = False,
        log_summary: bool = False,
        log_all: bool = False,
        log_format: str = None,
        thread_prefix: str = None
    ):
        """
        This class enables the ability to consume a generator function and do some work in a thread pool before
        returning an generator function to iterate over

        :param max_workers: The maximum number of workers to use in the thread pool
        :param retry_count: The number of additional tries to retry the job if it fails
        :param sleep_seed: If a job failed, it will retry after sleeping the job attempt number multiplied by this
            number. Any number less than or equal to zero will result in not sleeping between retries
        :param quiet_traceback: If True, will not print or log the traceback message on an exception
        :param logger: A logger to use for periodic logging and error messages
        :param log_interval: The time in seconds to log the periodic success and failure statuses
        :param log_periodically: If True, will log periodic success and failure status message
        :param log_retry: If True, will log retry warning messages
        :param log_error: If True, will log error messages
        :param log_summary: If True, will log the total job success and failure count after all the jobs have been
            complete
        :param log_all: If True, it is the equivalent of setting log_periodically, log_retry, log_error, and log_summary
            to true
        :param log_format: The periodic log string format
        :param thread_prefix: The prefix of the thread names. Defaults to "Multiflow"
        """
        if logger is not None:
            assert isinstance(logger, logging.Logger)

        self._input_queue = Queue()  # for storing the job to execute
        self._output_queue = Queue()  # for storing the job result

        # consumer function and it's arguments
        self._consumer_fn = None
        self._consumer_args = None
        self._consumer_kwargs = None

        self._thread_prefix = thread_prefix if thread_prefix is not None else 'Multiflow'

        # the thread pool executor
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix='{}ThreadPool'.format(self._thread_prefix)
        )

        # producer and consumer threads
        self._producer_thread = None
        self._consumer_thread = None

        # for automatically retrying the job
        self._sleep_seed = max(sleep_seed, 0)
        self._total_count = retry_count + 1

        # managing multiple types of jobs
        self._fid_to_name = {}

        # counts
        self._num_of_successful_jobs = defaultdict(int)
        self._num_of_failed_jobs = defaultdict(int)

        # error traceback
        self._quiet_traceback = quiet_traceback

        # logging
        self._logger_thread = None
        self._logger = logger
        self._log_interval = log_interval
        self._log_periodically = log_periodically or log_all
        self._log_retry = log_retry or log_all
        self._log_error = log_error or log_all
        self._log_summary = log_summary or log_all

        self._log_format = log_format
        if self._log_format:
            self._use_c_str_fmt = use_c_string(self._log_format, _LOG_KWARGS)
        else:
            self._use_c_str_fmt = True

        self._hide_fid = False

    def get_successful_job_count(self, fn_id: int = 0) -> int:
        return self._num_of_successful_jobs[fn_id]

    def get_failed_job_count(self, fn_id: int = 0) -> int:
        return self._num_of_failed_jobs[fn_id]

    def get_output(self) -> Generator[JobOutput, None, None]:
        """
        Returns a generator object

        :return: each item in the generator object is an instance of JobOutput
        """
        if not self._consumer_fn:
            raise FlowException('Must set the consumer function')

        self._producer_thread = Thread(target=self._producer, daemon=True,
                                       name='{}Producer'.format(self._thread_prefix))
        self._consumer_thread = Thread(target=self._wrap_consumer, daemon=True,
                                       name='{}Consumer'.format(self._thread_prefix))
        if self._log_periodically and self._logger:
            # noinspection PyTypeChecker
            self._logger_thread = StoppableThread(target=self._periodic_log_wrapper, daemon=True,
                                                  name='{}Logger'.format(self._thread_prefix))
            self._logger_thread.start()

        self._producer_thread.start()
        self._consumer_thread.start()

        # takes items from output queue and produces generator output
        while True:
            output = self._output_queue.get()
            self._output_queue.task_done()

            if not isinstance(output, _DummyItem):
                # updates successful and failed counts
                if output.is_successful():
                    self._num_of_successful_jobs[output.get_fn_id()] += 1
                else:
                    self._num_of_failed_jobs[output.get_fn_id()] += 1

                yield output
            else:
                break

        self._producer_thread.join()
        self._consumer_thread.join()

        if self._logger_thread:
            self._logger_thread.stop()
            self._logger_thread.join()

        # log final summary
        if self._log_summary:
            for fid in self._fid_to_name:
                self._log_status(fid)

    def _prepend_name_for_log(self, log_msg, fn_id, fn=None):
        name = self._fid_to_name[fn_id]

        # if there is a name, prepend the name and job id
        if name:
            if self._hide_fid:
                log_msg = '{}: '.format(name) + log_msg
            else:
                log_msg = '{} ({}): '.format(name, fn_id) + log_msg
        elif fn:
            log_msg = '{}(): '.format(fn.__name__) + log_msg

        return log_msg

    def _log_status(self, fid):
        name = self._fid_to_name[fid]

        # builds log kwargs
        log_kwargs = {
            'success': self.get_successful_job_count(fn_id=fid),
            'failed': self.get_failed_job_count(fn_id=fid),
            'name': name,
            'fid': fid
        }

        log_kwargs.update({
            's_plural': pluralize(log_kwargs['success']),
            'f_plural': pluralize(log_kwargs['failed'])
        })

        if self._log_format:
            # logs custom periodic log message
            if self._use_c_str_fmt:
                self._logger.info(self._log_format % log_kwargs)
            else:
                self._logger.info(self._log_format.format_map(log_kwargs))
        else:
            # logs default periodic log message
            log_fmt = '{success} job{s_plural} completed successfully. {failed} job{f_plural} failed.'

            self._logger.info(self._prepend_name_for_log(log_fmt.format_map(log_kwargs), fid))

    def _periodic_log_wrapper(self):
        # sleeps to start so it doesn't immediately log 0 jobs completed/failed (or in other words, before it has
        # actually done any work)
        if self._logger_thread.wait(self._log_interval):
            return

        # as long as there is work still running
        while True:
            for fid in self._fid_to_name:
                self._log_status(fid)

            if self._logger_thread.wait(self._log_interval):
                break

    def _producer(self):
        """
        Executes the job from the input queue and puts the result in the output queue
        """
        while True:
            future = self._input_queue.get()
            self._input_queue.task_done()

            if not isinstance(future, _DummyItem):
                result = future.result()
                self._output_queue.put_nowait(result)
            else:
                break

        # adds dummy item to end of the output queue so we know there are no more results
        self._output_queue.put_nowait(_DummyItem())

    def set_consumer(self, fn, *args, **kwargs):
        """
        Your consumer function should call self.submit_job to add a job to the thread pool

        Here is an example of the code structure of the consumer function

        for value in self.generator_function:
            self.submit_job(task_function, *args, **kwargs)

        :param fn: The consumer function
        :param args: The args to the consumer function
        :param kwargs: The kwargs to the consumer function
        """
        self._consumer_fn = fn
        self._consumer_args = args
        self._consumer_kwargs = kwargs

    def _wrap_consumer(self):
        """
        Wraps the consumer call so that it can properly let the producer know that there are no more jobs being added
        to the thread pool
        """
        try:
            self._consumer_fn(*self._consumer_args, **self._consumer_kwargs)
            # adds dummy item to end of the input queue so we know there are no more jobs to do
            self._input_queue.put_nowait(_DummyItem())
        except RuntimeError:
            # catch RuntimeError, if it tries to schedule a new future after shutting down
            pass

    def submit_job(self, fn, *args, **kwargs):
        """
        Submits job to thread pool
        """
        self._submit_job_with_fid(0, FlowFunction('', fn, *args, **kwargs))

    def _submit_job_with_fid(self, fid: int, flow_fn: FlowFunction, prev=None):
        """
        Submits job to thread pool
        """
        self._fid_to_name[fid] = flow_fn.name
        self._input_queue.put_nowait(self._executor.submit(self._call_fn_and_catch_exception, fid, flow_fn, prev=prev))

    def _call_fn_and_catch_exception(self, fid: int, flow_fn: FlowFunction, prev=None):
        """
        A wrapper function to call function while catching and returning the exception
        """
        exception = None
        exec_info = None
        # noinspection PyProtectedMember
        args, kwargs = flow_fn._calc_args_and_kwargs(prev=prev)
        for i in range(1, self._total_count + 1):
            try:
                # noinspection PyProtectedMember
                return JobOutput(success=True, attempts=i, fn_id=fid, result=flow_fn._run(*args, **kwargs), args=args,
                                 kwargs=kwargs)
            except Exception as e:
                # noinspection PyProtectedMember
                exception, exec_info = flow_fn._handle(e, prev=prev)
                if not exec_info:
                    return JobOutput(success=True, attempts=i, fn_id=fid, result=exception, args=args, kwargs=kwargs)

                # if we are going to retry this job
                if i < self._total_count:
                    if self._log_retry and self._logger:
                        log_msg = 'Retrying job after catching exception: {}'.format(exception)
                        # noinspection PyProtectedMember
                        self._logger.warning(self._prepend_name_for_log(log_msg, fid, fn=flow_fn._fn))

                    time.sleep(i * self._sleep_seed)

        if self._log_error and self._logger:
            log_msg = 'Job failed with exception: {}'.format(exception)
            if not self._quiet_traceback:
                # gets traceback from exception
                tb = traceback.TracebackException.from_exception(exception)
                # formats traceback and removes last newline
                tb_msg = ''.join(tb.format())[:-1]
                log_msg += '\n{}'.format(tb_msg)
            # noinspection PyProtectedMember
            self._logger.error(self._prepend_name_for_log(log_msg, fid, fn=flow_fn._fn))
        elif not self._logger and not self._quiet_traceback:
            traceback.print_exception(exception, *exec_info[-2:])

        return JobOutput(success=False, attempts=self._total_count, fn_id=fid, exception=exception, args=args,
                         kwargs=kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            # shutdowns executor
            self._executor.shutdown(wait=False)

            # cancels unfinished tasks
            with self._input_queue.mutex:
                while len(self._input_queue.queue):
                    future = self._input_queue.queue.popleft()
                    if not isinstance(future, _DummyItem):
                        future.cancel()
                    self._input_queue.unfinished_tasks -= 1
                self._input_queue.all_tasks_done.notify_all()
                self._input_queue.unfinished_tasks = 0

            self._input_queue.put_nowait(_DummyItem())

            # also clears output
            with self._output_queue.mutex:
                self._output_queue.queue.clear()
                self._output_queue.all_tasks_done.notify_all()
                self._output_queue.unfinished_tasks = 0

            self._output_queue.put_nowait(_DummyItem())

            # stops logger thread
            if self._logger_thread:
                self._logger_thread.stop()
                self._logger_thread.join()

            if self._producer_thread:
                self._producer_thread.join()

            if self._consumer_thread:
                self._consumer_thread.join()

        self._executor.__exit__(exc_type, exc_val, exc_tb)

    def __iter__(self):
        yield from self.get_output()


class MultithreadedGenerator(ABC, MultithreadedGeneratorBase):
    def __init__(self, **kwargs):
        MultithreadedGeneratorBase.__init__(self, **kwargs)
        self.set_consumer(self.consumer)

    @abstractmethod
    def consumer(self):
        """
        Your consumer function should call self.submit_job to add a job to the thread pool

        Here is an example of the code structure

        for value in self.generator_function:
            self.submit_job(task_function, *args, **kwargs)
        """
        pass


class MultithreadedFlow:
    def __init__(self, log_only_last=False, **kwargs):
        """
        Like the MultithreadedGenerator, this accepts a generator, does some work in a thread pool, and returns a
        generator. This class also enables the ability chain multiple jobs after each other by passing the first job
        from the first thread pool to the next thread pool for the next job.

        :param log_only_last: If True, only the last item in the process flow will be periodically logged and log
            summary (provided that a logger is provided)
        :param kwargs: see kwargs for the MultithreadedGeneratorBase
        """

        # options for MultithreadedGeneratorBase
        self._options = kwargs
        self._has_thread_prefix = 'thread_prefix' in self._options

        # raises KeyError if there are problems with the log format before lazily failing in a child thread
        log_format = self._options.get('log_format')
        if log_format:
            use_c_string(log_format, _LOG_KWARGS)

        self._index_to_options = {}
        self._use_opts = True

        self._log_only_last = log_only_last

        # stores the input iterable item/function iterator to consume and its arguments
        self._fn = None
        self._iterable = None
        self._args = None
        self._kwargs = None

        # to keep track of the order of functions to call
        self._fn_calls = []

        # counts
        self._success_count = 0
        self._failed_count = 0

    def consume(self, *args, **kwargs):
        """
        Sets the input function or iterable to be consumed by the process flow

        :param args: The first item is either the callable function or iterable item to consume. The rest of the items
            are the args for the callable function. Not used if first iterable item.
        :param kwargs: kwargs for the callable function, otherwise not used
        """
        num_of_args = len(args)
        if num_of_args == 0:
            raise FlowException('Must provide a function or iterable item to consume.')
        elif isinstance(args[0], Callable):
            self._fn = args[0]
        elif isinstance(args[0], Iterable):
            self._iterable = args[0]
        else:
            raise FlowException('First item must be an iterable item or function returning an iterator')

        self._args = args[1:]
        self._kwargs = kwargs

    def add_function(self, *args, **kwargs):
        """
        Adds a function call to the process flow.

        :param args: The first argument must be a callable function or the name to be present in the periodic logs
            (string type). If the first argument is a string, then the second argument must be a callable function.
            All other arguments after that, are passed as positional arguments to the callable function after the return
            value of the previous function in the process flow (which will always be the first argument).
        :param kwargs: kwargs to be passed to the callable function in the process flow.
        """
        num_of_args = len(args)
        if num_of_args == 0:
            raise FlowException('Must provide a function to add to the process flow.')
        elif isinstance(args[0], str):
            name = args[0]
            if num_of_args == 1:
                raise FlowException('If the first argument is of type string, must provide a second argument that is a '
                                    'callable function.')

            fn = args[1]
            if not isinstance(fn, Callable):
                raise FlowException('If first argument is of type string, the second argument must be a callable '
                                    'function.')

            offset = 2
        elif isinstance(args[0], Callable):
            fn = args[0]
            name = fn.__name__
            offset = 1
        else:
            raise FlowException('The first argument must be a string or callable function, not of type {}.'
                                .format(type(args[0])))

        fn_args = args[offset:]

        flow_fn = FlowFunction(name, fn, *fn_args, **kwargs)

        self._fn_calls.append(flow_fn)

        return flow_fn

    def get_successful_job_count(self) -> int:
        return self._success_count

    def get_failed_job_count(self) -> int:
        return self._failed_count

    def get_output(self) -> Generator[JobOutput, None, None]:
        if not self._fn_calls:
            raise FlowException('Must add at least one consuming function')

        # the iterable item to consume
        iterable = self._fn(*self._args, **self._kwargs) if self._fn else self._iterable

        if iterable is None:
            raise FlowException('Must call consume() to consume an iterable function or iterable item.')

        # stores the MultithreadedGeneratorBase class instances for each step in the process flow
        process_flow = []

        # stores the additional job successes and exceptions that exited early by index
        additional_outputs = defaultdict(list)

        def consumer(index):
            """
            Sets the consumer of the current MultithreadedGeneratorBase to the output of the last one
            :param index: The current function's index
            """
            if index == 0:
                for item in iterable:
                    # noinspection PyProtectedMember
                    process_flow[index]._submit_job_with_fid(index, self._fn_calls[index], prev=item)
            else:
                with process_flow[index - 1] as prev_flow:
                    for item in prev_flow.get_output():
                        if item.get_result() is not None:
                            # noinspection PyProtectedMember
                            process_flow[index]._submit_job_with_fid(index, self._fn_calls[index],
                                                                     prev=item.get_result())
                        else:
                            additional_outputs[index].append(item)

        # builds the multithreaded process flow
        num_of_fns = len(self._fn_calls)
        for i in range(num_of_fns):
            # sets the thread prefix
            if num_of_fns > 1 and not self._has_thread_prefix:
                thread_prefix = 'Multiflow_{}_'.format(i)
                if self._use_opts:
                    self._options['thread_prefix'] = thread_prefix
                else:
                    self._index_to_options[i]['thread_prefix'] = thread_prefix

            multithreaded_generator = MultithreadedGeneratorBase(**(self._options if self._use_opts
                                                                    else self._index_to_options[i]))
            if num_of_fns == 1:
                multithreaded_generator._hide_fid = True
            elif self._log_only_last and i != num_of_fns - 1:
                multithreaded_generator._log_periodically = False
                multithreaded_generator._log_summary = False
            process_flow.append(multithreaded_generator)
            multithreaded_generator.set_consumer(consumer, i)

        # yields the output from the final process flow
        with process_flow[-1] as final_flow:
            for output in final_flow.get_output():
                if output:
                    self._success_count += 1
                else:
                    self._failed_count += 1
                yield output

        # yields output for upstream successes and exceptions
        for outputs in additional_outputs.values():
            for output in outputs:
                if output:
                    self._success_count += 1
                else:
                    self._failed_count += 1
                yield output

    def __add__(self, other):
        if not isinstance(other, MultithreadedFlow):
            raise FlowException('Must be an instance of MultithreadedFlow')

        new_flow = MultithreadedFlow()

        num_of_fns = len(self._fn_calls)
        num_of_other_fns = len(other._fn_calls)

        index = 0

        # first adds the first items
        new_flow._fn_calls.extend(self._fn_calls)
        for index in range(num_of_fns):
            new_flow._index_to_options[index] = self._options

        # if the first flow has no items
        if index:
            index += 1

        # then adds the second items
        new_flow._fn_calls.extend(other._fn_calls)
        for j in range(num_of_other_fns):
            new_flow._index_to_options[index + j] = other._options

        new_flow._use_opts = False

        return new_flow

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __iter__(self):
        yield from self.get_output()
