pynesis
=======

High level kinesis client. Support python 2.7 and 3.6, and has
helpers for using it within Django.


Usage
-----

```python
from pynesis.backends import KinesisBackend

stream = KinesisBackend("my-stream", region_name="eu-west-2")

for record in stream.read():
    print(record)

```


See the examples available [here](pynesis/tests/examples_tests.py) for 
more details


Development environment
=======================

Run `make shell` and then use the tests/examples.
 

Running tests
=============

To run all tests in all environments and python versions supported, run:
    
    make test
    
    
To run a single test in a single environment, from within a `make shell` run:

    tox -e py36-dj19-redis -- pynesis/tests/examples_tests.py::pynesis/tests/example_test.py::test_simple_reading_example
