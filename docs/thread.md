# Docs
There are three main classes within the `multiflow` library. They are `MultithreadedGeneratorBase`, `MultithreadedGenerator` and `MultithreadedFlow`. As their naming implies, `MultithreadedGeneratorBase` is the base class which the other two are built off of.


## Arugments
All three classes support these arguments when initializing them. All of them are optional. Here are their default values.
```python
with MultithreadedFlow(
    max_workers = None,
    retry_count = 0,
    sleep_seed = 1,
    quiet_traceback = False,
    logger = None,
    log_interval = 30,
    log_periodically = False,
    log_retry = False,
    log_error = False,
    log_summary = False,
    log_all = False,
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
* `log_all`: If True, it is the equivalent of setting `log_periodically`, `log_retry`, `log_error`, and `log_summary` to true
* `log_format` - The periodic log string format
* `thread_prefix` - The prefix of the thread names. Defaults to "Multiflow"

Additionally, `MultithreadedFlow` supports these additional arguments:
* `log_only_last` - If True, only the last item in the process flow will be periodically logged and log summary (provided that a logger is provided)

## Job Output
All there classes yield an instance of `JobOutput` when you iterate over the `get_output()` function output. Here are the functions of the `JobOutput` class and what they return.
* `is_successful()` - True if the job was successful, False otherwise
* `is_last()` - True if the job output is from the last function in the process flow; otherwise, False (only used for the `MultithreadedFlow`)
* `get_num_of_attempts()` - The total number of attempts the job was run (initial run plus retries)
* `get_fn_id()` - The function id (only really used for the `MultithreadedFlow` for the index of the function added to the process flow)
* `get_result()` - The result of the job
* `get_exception()` - The exception if not successful
* `get()` - gets the value of the arg or kwarg passed into the function by name. Additionally, it will get the default value of the kwarg if it was not overwritten when it was run

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

Additionally, the `JobOutput` class has some special functions.
* If you cast the `JobOutput` object to boolean, it will return `is_successful()`
* If you cast the `JobOutput` object using `repr()`, it will return the reps value of `get_result()`
* If you cast the `JobOutput` object to string, it will return the string value of `get_result()`
* If you index the `JobOutput` object with the square brackets `[]`, it will return argument value at that index passed into the function
* If you use the `getattr()` function on the `JobOutput` object, it will be the same as calling the `get()` function


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
* `map` - Sets the consuming function and input iterable at the same time. A shortcut for calling the `consume` and `add_function` separately
* 
  ```python
  with MultithreadedFlow() as flow:
      flow.map(fn, iterable)

      for output in flow:
          pass
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

## Logging
All three classes support logging, and as mentioned above, you can pass a `log_format` for the periodic logger to use. The `log_format` supports both C style formatting (with %) and the newer Pythonic curly bracket formatting. Here are all of  the keys that you can use in the periodic log formatter.
```python
{
    'success': 0,       # the number of successfully completed jobs
    'failed': 0,        # the number of failed jobs
    's_plural': 's',    # "" if success is 1, "s" otherwise. For proper pluralization of noun
    'f_plural': 's',    # "" if failed is 1, "s" otherwise. For proper pluralization of noun
    'name': 'fn',       # the name of the function or custom name passed when calling add_function in MultithreadedFlow
    'fid': 0            # the function id (zero index number in the process flow) when using MultithreadedFlow
}
```

How to use?
```python
# C style formatting
log_fmt = '%(success)s job%(s_plural)s completed successfully. %(failed)s job%(f_plural)s failed.'
with MultithreadedFlow(log_format=log_fmt, logger=logger, log_periodically=True) as flow:
    ...

# Python style formatting
log_fmt = '{success} job{s_plural} completed successfully. {failed} job{f_plural} failed.'
with MultithreadedFlow(log_format=log_fmt, logger=logger, log_periodically=True) as flow:
    ...
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

### Argument inheritance
Argument inheritance allows you to inherit args or kwargs from a previous flow function to be passed onto the next function

```python
def fn1(x, y=5):
    return x

def fn2(x, y=0):
    return x + y

with MultithreadedFlow() as flow:
    flow.consume(range(10))
    flow.add_function(fn1)
    flow.add_function(fn2).inherit_params()

    for output in flow:
        print(output.get('y'))  # this is 5
```
