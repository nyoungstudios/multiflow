<h1 align="center">multiflow</h1>

[![tests](https://github.com/nyoungstudios/multiflow/actions/workflows/python-test.yml/badge.svg)](https://github.com/nyoungstudios/multiflow/actions/workflows/python-test.yml)
[![codecov](https://codecov.io/gh/nyoungstudios/multiflow/branch/main/graph/badge.svg?token=9M2UZ4WJ36)](https://codecov.io/gh/nyoungstudios/multiflow)
[![Gitpod ready](https://img.shields.io/badge/Gitpod-ready-blue?logo=gitpod)](https://gitpod.io/#https://github.com/nyoungstudios/multiflow)
[![PyPI version shields.io](https://img.shields.io/pypi/v/multiflow.svg)](https://pypi.python.org/project/multiflow/)
[![PyPI license](https://img.shields.io/pypi/l/multiflow.svg)](https://pypi.python.org/project/multiflow/)

## About
`multiflow` is a Python multithreading library for data processing pipelines/workflows, streaming, etc. It extends `concurrent.futures` by allowing the input and output to be generator objects. And, it makes it easy to string together multiple thread pools together to create a multithreaded pipeline.

Additionally, `multiflow` comes with periodic logging, automatic retries, error handling, and argument expansion.

## Why?
The ability to accept an input generator object while yielding an output generator object makes it ideal for concurrently doing multiple jobs where the output of the first job is the input of the second job. This means that it can start doing work on the second job before the first job completes; thus, completing the total work faster.

A great use case for this is streaming data. For example, with `multiflow` and [`smart_open`](https://github.com/RaRe-Technologies/smart_open), you could stream images from S3 and process them in a multithreaded environment before exporting them elsewhere.

## Install
```sh
pip install multiflow
```

## Quickstart
```python
from multiflow import MultithreadedFlow


image_paths = []  # list of images


def transform(image_path):
    # do some work
    return new_path


with MultithreadedFlow() as flow:
    flow.consume(image_paths)  # can accept generator object or iterable item (see examples below for generator)
    flow.add_function(transform)

    for output in flow:
        if output:  # if successful
            print(output)  # new_path
        else:
            e = output.get_exception()

    success = flow.get_successful_job_count()
    failed = flow.get_failed_job_count()

```

## Examples
For a working program using `multiflow`, see this [example](https://github.com/nyoungstudios/multiflow/blob/main/examples/resize/resize.py) which resizes a S3 bucket of images to 50% and saves the resized images locally.

## Documentation
The documentation is still a work in progress, but for the most up to date documentation, please see this [page](https://github.com/nyoungstudios/multiflow/blob/main/docs/thread.md).
