"""
A simple example that doesn't do anything computationally expensive, but shows how to pass consume
data and pass values between functions. Additionally, shows how to catch and handle exceptions

"""
import logging
from multiflow import MultithreadedFlow
import sys
import time


def get_logger(name):
    """
    Gets logger instance
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stdout
    )
    return logging.getLogger(name)


def add_n(v, n=1):
    """
    Adds two numbers
    """
    return v + n


def fail_or_sleep(v):
    """
    Returns exception if the value is odd. Otherwise, sleeps the value mod 5
    """
    if v % 2 == 0:
        x = v % 5
        time.sleep(x)
        return x
    else:
        raise Exception('Value is odd: {}'.format(v))


def handle_exception(e, v):
    """
    Handles exception from fail_or_sleep function
    """
    if v == 13:
        # after catching exception and if v is equal to 13, will return -1 and mark the job as successful
        return -1
    else:
        # otherwise raises error
        raise e


if __name__ == '__main__':
    logger = get_logger('test')

    with MultithreadedFlow(
        max_workers=32,
        retry_count=1,
        logger=logger,
        log_all=True,
        quiet_traceback=True
    ) as flow:
        # consumes the iterable values from 0 to 19
        flow.consume(range, 20)

        # calls add_n on all values yielded from range
        flow.add_function(add_n, n=5)
        # calls fail_or_sleep on all values yielded from add_n. If there is an exception, will
        # call handle_exception to handle the exception
        flow.add_function(fail_or_sleep).error_handler(handle_exception)

        # for all results from the thread pool
        for output in flow:
            # if the job was successful
            if output:
                # prints v (the first argument passed into the last function - fail_or_sleep)
                # and the output of the last function
                print(output[0], output)
            else:
                e = output.get_exception()

            # this value is 1 which represents the second function called - fail_or_sleep in this case
            fn_id = output.get_fn_id()

            # gets total number of tries (this will be 1 or 2 in the case of the exception raised by fail_or_sleep)
            num_of_tries = output.get_num_of_attempts()

        # gets total number of success (11) and failed counts (9)
        success_count = flow.get_successful_job_count()
        failed_count = flow.get_failed_job_count()
