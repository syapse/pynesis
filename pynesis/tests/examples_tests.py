import json

import pytest

from pynesis.streams import KinesisStream
from pynesis.checkpointers import RedisCheckpointer
from pynesis.tests.conftest import django_only, redis_only

pytestmark = pytest.mark.usefixtures("examples")


def test_simple_reading_example():
    """
    This example will loop forever polling records from the kinesis stream.
    When the process is stopped/started, the polling will start from the oldest
    still available record in the stream, despite it has been already processed or not.
    """
    stream = KinesisStream(stream_name="my-stream", region_name="us-east-2")

    for i, record in enumerate(stream.read()):
        print(record)
        if i == 10:
            stream.stop()


@redis_only
def test_redis_checkpointer_example():
    """
    This example, as the previous one, will loop indefinitely yielding records, but after
    each record is processed (within the for loop), the position in the stream will be recorded
    sot that after the process restarts, it will resume without yielding already processed records
    """

    redis_checkpointer = RedisCheckpointer(redis_host="localhost", key="my-stream-redis-key")
    stream = KinesisStream(stream_name="my-stream", region_name="us-east-2", checkpointer=redis_checkpointer)

    for i, record in enumerate(stream.read()):
        print(record)
        if i == 10:
            stream.stop()


def test_write_to_stream():
    """
    This example shows how to publish a record to a kinesis stream.
    The 'key' is a Kinesis concept, which is a way to decide the shard
    the message will be sent to, and thus guarantees ordering of records
    for all records published under the same key.
    """
    stream = KinesisStream(stream_name="my-stream", region_name="us-east-2")

    stream.put("1234", json.dumps({"message": "No problemo", "from": "Alf"}).encode("utf-8"))


@django_only
def test_django_usage():
    """
    This example shows how to use streams from django.

    Remember: It is safe to write to Kinesis streams from multiple processes, but when
    reading you have to be carefull of doing it from a single process if you don't want
    to process records more than once in your application

    Add a configuration like this to your django settings:

    PYNESIS_CONFIG = {
        "api-events": {
            "BACKEND": "pynesis.streams.KinesisStream",
            "BACKEND_OPTIONS": {
                "stream_name": "my-stream",
                "region_name": "eu-west-1",
            },
            "CHECKPOINTER": "pynesis.checkpointers.RedisCheckpointer",
            "CHECKPOINTER_OPTIONS": {
                "key": "kinesis:my-stream",
                "redis_host": os.environ.get("REDIS_HOST", "redis"),
                "redis_port": os.environ.get("REDIS_PORT", 6379),
                "redis_db": 0,
            }

           # Alternatively, you can use a database based checkpointer
           #
           # "CHECKPOINTER": "pynesis.djangoutils.DjangoCheckpointer",
           # "CHECKPOINTER_OPTIONS": {
           #     "key": "kinesis:my-stream",
           # }


        }
    }

    """
    from pynesis.djangoutils import get_stream

    stream = get_stream("my-stream")

    stream.put("1234", json.dumps({"message": "Hello from django"}).encode("utf-8"))

    for i, record in enumerate(stream.read()):
        print(record)
        if i == 10:
            stream.stop()
