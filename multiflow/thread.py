"""
Multithreaded consumer/producer generator with periodic logging support and exception catching
"""
from abc import ABC, abstractmethod
from collections import defaultdict
import concurrent.futures
import logging
from queue import Queue
from threading import Thread, Event
import time
from typing import Any, Callable, Generator, Iterable, Union

from multiflow.utils import calc_args, pluralize


class DummyItem:
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

    def error_handler(self, fn: Callable):
        """
        Adds exception handler to job

        :param fn: The function to handle the exception. This function should accept the exception as the first
            positional argument, the previous task in the process flow's result, and all the args and kwargs passed to
            the job function.
        """
        self._handler = fn

    def _calc_args(self, prev):
        if prev is None:
            return self._args
        else:
            return (prev, *self._args)

    def handle(self, exception: Exception, prev=None):
        if self._handler:
            try:
                return self._handler(exception, *self._calc_args(prev), **self._kwargs)
            except Exception as e:
                return e
        else:
            return exception

    def run(self, prev=None):
        return self._fn(*self._calc_args(prev), **self._kwargs)


class StoppableThread(Thread):
    def __init__(self, *args, **kwargs):
        """
        Thread class with a stop event
        """
        Thread.__init__(self, *args, **kwargs)
        self._stop_flag = Event()

    def stop(self):
        self._stop_flag.set()

    def is_stopped(self):
        return self._stop_flag.is_set()


class JobOutput:
    def __init__(self, success: bool, attempts: int, job_id: int = 0, result: Any = None, exception: Exception = None):
        """
        Data class to hold the output from the MultithreadedGenerator/Multiflow

        :param success: If True, the job was successful; otherwise, it failed
        :param attempts: The number of attempts it ran the job
        :param job_id: The job id
        :param result: If successful, the output of the job run
        :param exception: If not successful, the exception caught
        """
        self._success = success
        self._attempts = attempts
        self._job_id = job_id
        self._result = result
        self._exception = exception

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

    def get_job_id(self) -> int:
        """
        Returns the job id
        """
        return self._job_id

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

    def __bool__(self):
        return self.is_successful()

    def __repr__(self):
        return repr(self.get_result())

    def __str__(self):
        return str(self.get_result())


class MultithreadedGeneratorBase:
    def __init__(
        self,
        max_workers: int = None,
        catch_exception: bool = False,
        retry_count: int = 0,
        sleep_seed: int = 1,
        logger: logging.Logger = None,
        log_interval: int = 30,
        log_periodically: bool = False,
        log_warning: bool = False,
        log_error: bool = False,
        log_summary: bool = False,
        log_function: Callable = None
    ):
        """
        This class enables the ability to consume a generator function and do some work in a thread pool before
        returning an generator function to iterate over

        :param max_workers: The maximum number of workers to use in the thread pool
        :param catch_exception: If True, will catch any exception in each individual job so it won't cause the thread
            pool to stop working
        :param retry_count: The number of additional tries to retry the job if it fails
        :param sleep_seed: If a job failed, it will retry after sleeping the job attempt number multiplied by this
            number. Any number less than or equal to zero will result in not sleeping between retries
        :param logger: A logger to use for periodic logging and error messages
        :param log_interval: The time in seconds to log the periodic success and failure statuses
        :param log_periodically: If True, will log periodic success and failure status message
        :param log_warning: If True, will log warning messages
        :param log_error: If true, will log error messages
        :param log_summary: If True, will log the total job success and failure count after all the jobs have been
            complete
        :param log_function: If provided, will call this function instead of the default periodic logger. Function must
            have 2 or 3 arguments. The first argument will be passed the number of successful jobs so far, the second
            will be passed the number of failed jobs. And the third if present, will be passed the job name
        """
        if logger is not None:
            assert isinstance(logger, logging.Logger)

        if log_function:
            self._log_fn_args = calc_args(log_function)
            assert self._log_fn_args == 2 or self._log_fn_args == 3, \
                'Log function can only have two or three arguments, the first being the number of successful jobs ' \
                'and the second being the number of failed jobs. Third argument is the job name (which is optional)'

        self._input_queue = Queue()  # for storing the job to execute
        self._output_queue = Queue()  # for storing the job result

        # consumer function and it's arguments
        self._consumer_fn = None
        self._consumer_args = None
        self._consumer_kwargs = None

        # the thread pool executor
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers,
                                                               thread_name_prefix='MultiflowThreadPool')

        # for catching exception and automatically retrying the job
        self._catch_exception = catch_exception
        self._sleep_seed = max(sleep_seed, 0)
        self._total_count = retry_count + 1

        # managing multiple types of jobs
        self._jid_to_name = {}

        # counts
        self._num_of_successful_jobs = defaultdict(int)
        self._num_of_failed_jobs = defaultdict(int)

        # logging
        self._logger_thread = None
        self._logger = logger
        self._log_interval = log_interval
        self._log_periodically = log_periodically
        self._log_warning = log_warning
        self._log_error = log_error
        self._log_summary = log_summary
        self._log_function = log_function

    def get_successful_job_count(self, job_id: int = 0) -> int:
        return self._num_of_successful_jobs[job_id]

    def get_failed_job_count(self, job_id: int = 0) -> int:
        return self._num_of_failed_jobs[job_id]

    def get_output(self) -> Generator[JobOutput, None, None]:
        """
        Returns a generator object

        :return: each item in the generator object is an instance of JobOutput
        """
        if not self._consumer_fn:
            raise FlowException('Must set the consumer function')

        producer_thread = Thread(target=self._producer, daemon=True, name='MultiflowProducer')
        consumer_thread = Thread(target=self._wrap_consumer, daemon=True, name='MultiflowConsumer')
        if self._log_periodically and (self._logger or self._log_function):
            self._logger_thread = StoppableThread(target=self._log_status, daemon=True, name='MultiflowLogger')
            self._logger_thread.start()

        producer_thread.start()
        consumer_thread.start()

        # takes items from output queue and produces generator output
        while True:
            output = self._output_queue.get()

            if not isinstance(output, DummyItem):
                # updates successful and failed counts
                if output.is_successful():
                    self._num_of_successful_jobs[output.get_job_id()] += 1
                else:
                    self._num_of_failed_jobs[output.get_job_id()] += 1

                self._output_queue.task_done()
                yield output
            else:
                self._output_queue.task_done()
                break

        producer_thread.join()
        consumer_thread.join()

        if self._logger_thread:
            self._logger_thread.stop()
            self._logger_thread.join()

    def _prepend_name_for_log(self, log_msg, job_id):
        name = self._jid_to_name[job_id]

        # if there is a name, prepend the name and job id
        if name:
            log_msg = '{} ({}): '.format(name, job_id) + log_msg

        return log_msg

    def _log_status(self):
        # sleeps to start so it doesn't immediately log 0 jobs completed/failed (or in other words, before it has
        # actually done any work)
        time.sleep(self._log_interval)

        # as long as there is work still running
        while not self._logger_thread.is_stopped():
            for jid, name in self._jid_to_name.items():
                if self._log_function:
                    # uses custom log function
                    if self._log_fn_args == 2:
                        self._log_function(self.get_successful_job_count(job_id=jid),
                                           self.get_failed_job_count(job_id=jid))
                    else:
                        self._log_function(self.get_successful_job_count(job_id=jid),
                                           self.get_failed_job_count(job_id=jid),
                                           name)
                else:
                    # logs default periodic log message
                    log_msg = '{} job{} completed successfully. {} job{} failed.'.format(
                        self.get_successful_job_count(job_id=jid), pluralize(self.get_successful_job_count(job_id=jid)),
                        self.get_failed_job_count(job_id=jid), pluralize(self.get_failed_job_count(job_id=jid)))

                    self._logger.info(self._prepend_name_for_log(log_msg, jid))

            time.sleep(self._log_interval)

    def _producer(self):
        """
        Executes the job from the input queue and puts the result in the output queue
        """
        while True:
            future = self._input_queue.get()

            if not isinstance(future, DummyItem):
                result = future.result()
                self._output_queue.put_nowait(result)
                self._input_queue.task_done()
            else:
                self._input_queue.task_done()
                break

        # adds dummy item to end of the output queue so we know there are no more results
        self._output_queue.put_nowait(DummyItem())

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
        self._consumer_fn(*self._consumer_args, **self._consumer_kwargs)
        # adds dummy item to end of the input queue so we know there are no more jobs to do
        self._input_queue.put_nowait(DummyItem())

    def submit_job(self, fn, *args, **kwargs):
        """
        Submits job to thread pool
        """
        self.submit_job_with_jid(0, FlowFunction('', fn, *args, **kwargs))

    def submit_job_with_jid(self, jid, flow_fn, prev=None):
        """
        Submits job to thread pool
        """
        self._jid_to_name[jid] = flow_fn.name
        self._input_queue.put_nowait(self._executor.submit(self._call_fn_and_catch_exception, jid, flow_fn, prev=prev))

    def _call_fn_and_catch_exception(self, jid, flow_fn: FlowFunction, prev=None):
        """
        A wrapper function to call function while catching and returning the exception
        """
        exception = None
        for i in range(1, self._total_count + 1):
            try:
                return JobOutput(success=True, attempts=i, job_id=jid, result=flow_fn.run(prev=prev))
            except Exception as e:
                exception = flow_fn.handle(e, prev=prev)
                if not isinstance(exception, Exception):
                    return JobOutput(success=True, attempts=i, job_id=jid, result=exception)

                # if we are going to retry this job
                if i < self._total_count:
                    if self._log_warning and self._logger:
                        log_msg = 'Retrying job after catching exception: {}'.format(exception)
                        self._logger.warning(self._prepend_name_for_log(log_msg, jid))

                    time.sleep(i * self._sleep_seed)

        if not self._catch_exception:
            raise exception
        elif self._log_error and self._logger:
            log_msg = 'Job failed with exception: {}'.format(exception)
            self._logger.error(self._prepend_name_for_log(log_msg, jid))

        return JobOutput(success=False, attempts=self._total_count, job_id=jid, exception=exception)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._executor.__exit__(exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)

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
    def __init__(self, it: Union[Callable, Iterable], *args, **kwargs):
        """
        Like the MultithreadedGenerator, this accepts a generator, does some work in a thread pool, and returns a
        generator. This class also enables the ability to reuse the same thread pool by doing a series of tasks instead
        of creating another thread pool to consume the output of the first thread pool.

        :param it: A callable function that returns an iterator or an iterable item like a list
        :param args: args for the callable function, otherwise not used
        :param kwargs: kwargs for the callable function, otherwise not used
        """

        # stores the input iterable item/function iterator to consume and its arguments
        self._fn = None
        self._iterable = None
        if isinstance(it, Callable):
            self._fn = it
        elif isinstance(it, Iterable):
            self._iterable = it
        else:
            raise FlowException('First item must be an iterable item or function returning an iterator')

        self._args = args
        self._kwargs = kwargs

        # to keep track of the order of functions to call
        self._fn_calls = []
        self._last_jid = 0

        # options for MultithreadedGeneratorBase
        self._options = {}

        # counts
        self._success_count = 0
        self._failed_count = 0

    def set_params(self, **kwargs):
        """
        See MultithreadedGeneratorBase for the kwargs that you can set
        """
        self._options = kwargs

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
        else:
            if isinstance(args[0], str):
                name = args[0]
                if num_of_args == 1:
                    raise FlowException('If the first argument is of type string, must provide a second argument that '
                                        'is a callable function.')

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

        process_flow = []

        iterable = self._fn(*self._args, **self._kwargs) if self._fn else self._iterable

        def consumer(index):
            if index == 0:
                for item in iterable:
                    process_flow[index].submit_job_with_jid(index, self._fn_calls[index], prev=item)
            else:
                with process_flow[index - 1] as prev_flow:
                    for item in prev_flow.get_output():
                        if item.get_result() is not None:
                            process_flow[index].submit_job_with_jid(index, self._fn_calls[index], prev=item.get_result())

        for i in range(len(self._fn_calls)):
            multithreaded_generator = MultithreadedGeneratorBase(**self._options)
            process_flow.append(multithreaded_generator)
            multithreaded_generator.set_consumer(consumer, i)

        with process_flow[-1] as final_flow:
            for output in final_flow.get_output():
                if output:
                    self._success_count += 1
                else:
                    self._failed_count += 1
                yield output

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __iter__(self):
        yield from self.get_output()
