
from multiflow import MultithreadedGeneratorBase, MultithreadedGenerator, MultithreadedFlow, JobOutput
from tests.setup_logger import get_logger
import time


logger = get_logger('test')


def printer(v):
    print(v)
    # time.sleep(v % 5)
    # if v == 4:
    #     raise Exception('asdfasdf')
    return v


def test_log_fn(s, f, n):
    print('adsfasdf', s, f, n)


class Test(MultithreadedGenerator):
    def __init__(self, x):
        super().__init__(max_workers=100, catch_exception=True, logger=logger, log_interval=1, log_periodically=True)
        self.x = x

    def consumer(self):
        for i in range(self.x):
            self.submit_job(printer, i)

    def get_stuff(self):
        for out in self.get_output():
            print('hi: {}'.format(out.get_result()))


# num = 5
#
# print('testing {} jobs'.format(num))
# with Test(num) as t:
#     t.get_stuff()
#     print(t.get_successful_job_count(), t.get_failed_job_count())


def iterator():
    for i in range(5):
        yield i


def add_one(x):
    # if x == 2:
    #     time.sleep(2)
    return x + 1


def add_two(x):
    return x + 2


with MultithreadedFlow(iterator) as flow:
    flow.add_function('add one', add_one)
    flow.add_function('add two', add_two)

    for i, v in enumerate(flow):
        print(i, v.get_result())

    print('results count:', flow.get_successful_job_count(), flow.get_failed_job_count())


# def numberGenerator(n):
#     number = yield
#     while number < n:
#         number = yield number
#         number += 1
#
#
# g = numberGenerator(10)  # Create our generator
# next(g)  #
# print(g.send(5))
#
#
# from typing import Generator
#
#
# def echo_round() -> Generator[float, int, None]:
#     while True:
#         val = yield
#         res = yield float(val)
#
#     # print('hi')
#
# x = echo_round()
# next(x)
# # for i in range(5):
# for o in (x.send(v) for v in range(5)):
#     next(x)
#     print(o)
#
# x.close()
