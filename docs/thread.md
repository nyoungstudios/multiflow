# Docs
There are three main classes within the `multiflow` library. They are `MultithreadedGeneratorBase`, `MultithreadedGenerator` and `MultithreadedFlow`. As their naming implies, `MultithreadedGeneratorBase` is the base class which the other two are built off of.


## Arugments
All three classes support these arguments when initializing them. All of them are optional. Here are their default values.
```python
with MultithreadedFlow(
    max_workers = None,
    retry_count = 0,
    sleep_seed = 1,
    quiet_traceback= False,
    logger = None,
    log_interval = 30,
    log_periodically = False,
    log_retry = False,
    log_error = False,
    log_summary = False,
    log_format = None,
    thread_prefix = None
) as flow:
    pass
```
* `max_workers` - The maximum number of workers to use in the thread pool
* `retry_count` -  The number of additional tries to retry the job if it fails
* `sleep_seed` - If a job failed, it will retry after sleeping the job attempt number multiplied by this number. Any number less than or equal to zero will result in not sleeping between retries
* `quiet_traceback` - If True, will not print or log the traceback message on an exception
* `logger` - A logger to use for periodic logging and error messages
* `log_interval` - The time in seconds to log the periodic success and failure statuses
* `log_periodically` - If True, will log periodic success and failure status message
* `log_retry` - If True, will log retry warning messages
* `log_error` - If True, will log error messages
* `log_summary` - If True, will log the total job success and failure count after all the jobs have been complete
* `log_format` - The periodic log string format
* `thread_prefix` - The prefix of the thread names. Defaults to "Multiflow"

Additionally, `MultithreadedFlow` supports these additional arguments:
* `log_only_last` - If True, only the last item in the process flow will be periodically logged and log summary (provided that a logger is provided)

## Job Output
All there classes return an instance pf `JobOutput`. Here are the functions of the `JobOutput` class and what they return.
* `is_successful()` - True if the job was successful, False otherwise
* `get_num_of_attempts()` - The total number of attempts it took (initial run plus retries)
* `get_fn_id()` - The function id (only really used for the `MultithreadedFlow` for the index of the function added to the process flow)
* `get_result()` - The result of the job
* `get_exception()` - The exception if not successful

In order to start the thread pool to do the work and get the output generator, call the `get_output()` function.
```python
with MultithreadedFlow() as flow:
    flow.consume(items)
    flow.add_function(fn)

    # get the output from the output generator
    for output in flow.get_output():
        # `output` is an instance of the `JobOutput` class
        pass

    # alternatively, you can omit the get_output() call and just iterate over the flow variable
    # this will result in exactly the same output (just less code to type)
    for output in flow:
        # `output` is an instance of the `JobOutput` class
        pass
```

Additionally, the `JobOutput` class has some special functions. Here is what they return.
* `bool` - will return `is_successful()`
* `repr` - will return the repr value of `get_result()`
* `str` - will return the string value of `get_result()`


## MultithreadedFlow Overview
`MultithreadedFlow` supports these functions.
* `consume` - Sets the input function or iterable to be consumed by the first function in the process flow
*
  ```python
  def generator(x, y, z, a=1, b=3):
      yield value

  # can accept a generator object
  with MultithreadedFlow() as flow:
      flow.consume(generator, 1, 2, 4, a=2, b=5)
      ...

  # can accept a iterable item
  items = []

  with MultithreadedFlow() as flow:
      flow.consume(items)
      ...
  ```

* `add_function` - Adds a function call to the process flow.
*
  ```python
  flow = MultithreadedFlow()

  # supports adding a function with args and kwargs
  flow.add_function(fn, *args, **kwargs)

  # supports adding a function with a custom name as the first positional argument used for logging, args, and kwargs
  flow.add_function('Custom name', fn, *args, **kwargs)

  ```

* `get_output` - As mentioned about, starts the thread pool and yields the outputs as a generator object
* `get_successful_job_count` - gets the number of successful jobs
* `get_failed_job_count` - gets the number of failed jobs


## Base Overview
`MultithreadedGeneratorBase` and `MultithreadedGenerator` both accept a generator object and output a generator object. In order to add jobs to the thread pool, you need to set a consumer function. In `MultithreadedGenerator`, this is already defined as an abstract function, but for `MultithreadedGeneratorBase`, you can set it to any function name you want with the `set_consumer` function call.

For example,
```python
# using MultithreadedGenerator
class MyTask(MultithreadedGenerator):
    def consumer():
        self.submit_job(fn, *args, **kwargs)


with MyTask() as my_task:
    for output in my_task:
        pass


# using MultithreadedGeneratorBase
flow = MultithreadedGeneratorBase()
def custom_consumer():
    flow.submit_job(fn, *args, **kwargs)

flow.set_consumer(custom_consumer)

with flow:
    for output in flow:
        pass

```

## Special features
These are some additional special features.

### Adding
If you have multiple `MultithreadedFlow` instances that you would like to chain together, you can simply add them together. In this example, it will call `fn1` in the first thread pool and it's output will be the input to `fn2`, with will be the input for `fn3`, etc. The initialization arguments for `flow1` and `flow2` will still be retained in the `flow_pipeline`. So, in this example, `fn1` and `fn2` will have `max_workers=4` while `fn3` and `fn4` will have `max_workers=8`.

```python
flow1 = MultithreadedFlow(max_workers=4)
flow1.add_function(fn1)
flow1.add_function(fn2)

flow2 = MultithreadedFlow(max_workers=8)
flow2.add_function(fn3)
flow2.add_function(fn4)

flow_pipeline = flow1 + flow2
with flow_pipeline:
    flow_pipeline.consume(fn, *args, **kwargs)

    for output in flow_pipeline:
        pass
```

### Custom error handling
Custom error handling can help you do specific tasks if there is a certain type of exception. Use cases may include marking a failed job as successful, querying to get additional information based off of the exception, etc.


```python
items = []

def task(x):
    # do something
    return value

def handle_error(e, x):
    """
    e is the Exception object that was caught
    x is the value passed into from the job function that was run (task() in this case)
    """
    if isinstance(e, CustomError):
        # if the exception is of `CustomError` type, do something and return a value
        # this marks this job as successful
        return value
    else:
        # otherwise, raise error to still mark it as failed
        raise e


with MultithreadedFlow() as flow:
    flow.consume(items)
    flow.add_function(task).error_handler(handle_error)

```


### Argument expansion
Argument expansion allows you to expand tuple and dictionary types in a process flow to be passed as `*args` and `**kwargs` to the next function.

```python
def iterator(n):
    for i in range(n):
        for j in range(n):
            yield i, j


def add(x, y):
    return x + y


with MultithreadedFlow() as flow:
    flow.consume(iterator, 10)
    flow.add_function(add).expand_params()

    for output in flow:
        print(output)
```