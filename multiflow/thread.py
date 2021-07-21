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
from typing import Any, Callable, Generator

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
        Data class to hold the output from the MultithreadedGenerator

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
        This class enables the ability to consume a generator function from one thread pool and put it into another
        thread pool while returning a generator function

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
        self._done_consuming = Event()  # if it is done consuming from input function
        self._done_producing = Event()  # if it is done producing the results

        # consumer function and it's arguments
        self._consumer_fn = None
        self._consumer_args = None
        self._consumer_kwargs = None

        # if there has at least been one job added to the input queue
        self._has_at_least_one = False

        # the thread pool executor
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

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

        producer_thread = Thread(target=self._producer, daemon=True)
        consumer_thread = Thread(target=self._wrap_consumer, daemon=True)
        if self._log_periodically and (self._logger or self._log_function):
            self._logger_thread = StoppableThread(target=self._log_status, daemon=True)
            self._logger_thread.start()

        producer_thread.start()
        consumer_thread.start()

        # waits for the thread pool to have at least one job to do before continuing
        while not self._has_at_least_one:
            # exits if nothing has been added to the job queue
            if self._done_consuming.is_set():
                self._done_producing.set()
                producer_thread.join()
                consumer_thread.join()

                if self._logger_thread:
                    self._logger_thread.stop()
                    self._logger_thread.join()

                return

        # takes items from output queue and produces generator output
        while not self._done_producing.is_set() or not self._output_queue.empty():
            output = self._output_queue.get()

            if not isinstance(output, DummyItem):
                # updates successful and failed counts
                if output.is_successful():
                    self._num_of_successful_jobs[output.get_job_id()] += 1
                else:
                    self._num_of_failed_jobs[output.get_job_id()] += 1

                yield output
            else:
                break

            self._output_queue.task_done()

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
                    log_msg = '{} job{} completed succcessfully. {} job{} failed.'.format(
                        self.get_successful_job_count(job_id=jid), pluralize(self.get_successful_job_count(job_id=jid)),
                        self.get_failed_job_count(job_id=jid), pluralize(self.get_failed_job_count(job_id=jid)))

                    self._logger.info(self._prepend_name_for_log(log_msg, jid))

            time.sleep(self._log_interval)

    def _producer(self):
        """
        Executes the job from the input queue and puts the result in the output queue
        """
        while not self._has_at_least_one:
            if self._done_consuming.is_set():
                # properly exists if nothing is added to consume so that the thread does get stuck as alive when
                # trying to get the future
                self._done_producing.set()
                return

        while not self._done_consuming.is_set() or not self._input_queue.empty():
            future = self._input_queue.get()

            result = future.result()
            self._output_queue.put_nowait(result)
            self._input_queue.task_done()

        self._done_producing.set()
        # so the output queue doesn't block forever if the input queue is cleared before the _done_producing Event is
        # read from get_output()
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
        self._done_consuming.set()

    def submit_job(self, fn, *args, **kwargs):
        """
        Submits job to thread pool
        """
        self.submit_job_with_jid(0, '', fn, *args, **kwargs)

    def submit_job_with_jid(self, jid, name, fn, *args, **kwargs):
        """
        Submits job to thread pool
        """
        self._has_at_least_one = True
        self._jid_to_name[jid] = name
        self._input_queue.put_nowait(self._executor.submit(self._call_fn_and_catch_exception, jid, fn, *args, **kwargs))

    def _call_fn_and_catch_exception(self, jid, fn, *args, **kwargs):
        """
        A wrapper function to call function while catching and returning the exception
        """
        exception = None
        for i in range(1, self._total_count + 1):
            try:
                return JobOutput(success=True, attempts=i, job_id=jid, result=fn(*args, **kwargs))
            except Exception as e:
                exception = e
                if self._log_warning and self._logger and i < self._total_count:
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
    def __init__(self, fn: Callable, *args, **kwargs):
        # stores the input function iterator to consume and its arguments
        self._fn = fn
        self._args = args
        self._kwargs = kwargs

        # to keep track of the order of functions to call
        self._fn_calls = []
        self._last_jid = 0

        # an instance of the MultithreadedGeneratorBase
        self._multithreaded_generator = None

        self._initial_has_at_least_one = Event()  # to make sure there is at least one item consumed
        self._done_initially_consuming = Event()  # keeps track if the first consumer is done
        self._initial_count = None

        self._process_queue = Queue()

    def set_params(self, **kwargs):
        """
        See MultithreadedGeneratorBase for the kwargs that you can set
        """
        self._multithreaded_generator = MultithreadedGeneratorBase(**kwargs)
        self._multithreaded_generator.set_consumer(self._consumer)

    def add_function(self, name, fn):
        self._fn_calls.append((name, fn))

    def get_successful_job_count(self) -> int:
        if self._multithreaded_generator:
            return self._multithreaded_generator.get_successful_job_count(job_id=self._last_jid)
        else:
            return 0

    def get_failed_job_count(self) -> int:
        if self._multithreaded_generator:
            return self._multithreaded_generator.get_failed_job_count(job_id=self._last_jid)
        else:
            return 0

    def _initial_consumer(self):
        initial_name, initial_fn = self._fn_calls[0]
        initial_count = 0
        for i, x in enumerate(self._fn(*self._args, **self._kwargs)):
            if i == 0:
                self._initial_has_at_least_one.set()
            initial_count += 1
            self._multithreaded_generator.submit_job_with_jid(0, initial_name, initial_fn, x)

        self._initial_count = initial_count

        self._done_initially_consuming.set()

    def _process_flow(self):
        while not self._initial_has_at_least_one.is_set():
            if self._done_initially_consuming.is_set():
                return

        last_jid_count = 0

        while not self._process_queue.empty() or self._initial_count is None or self._initial_count != last_jid_count:
            jid, output = self._process_queue.get()
            result = output.get_result()
            name, fn = self._fn_calls[jid]
            self._multithreaded_generator.submit_job_with_jid(jid, name, fn, result)
            self._process_queue.task_done()
            if jid == self._last_jid:
                last_jid_count += 1

    def _consumer(self):
        self._last_jid = len(self._fn_calls) - 1
        initial_consumer_thread = Thread(target=self._initial_consumer, daemon=True)
        process_flow_thread = Thread(target=self._process_flow, daemon=True)

        initial_consumer_thread.start()
        process_flow_thread.start()

        initial_consumer_thread.join()
        process_flow_thread.join()

    def get_output(self) -> Generator[JobOutput, None, None]:
        if not self._fn_calls:
            raise FlowException('Must add at least one consuming function')

        # initialize multithreaded generator if there were no parameters set
        if not self._multithreaded_generator:
            self.set_params()

        for output in self._multithreaded_generator.get_output():
            current_jid = output.get_job_id()
            if current_jid < self._last_jid:
                new_jid = current_jid + 1
                self._process_queue.put_nowait((new_jid, output))
            else:
                yield output

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._multithreaded_generator:
            self._multithreaded_generator.__exit__(exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)

    def __iter__(self):
        yield from self.get_output()
