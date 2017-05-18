from mock import call

from pynesis.checkpointers import InMemoryCheckpointer, RedisCheckpointer
from pynesis.tests.conftest import redis_only


def test_in_memory_checkpointer():
    checkpointer = InMemoryCheckpointer()
    checkpointer.checkpoint("myshard1", "sequence1")
    checkpointer.checkpoint("myshard2", "sequence2")

    assert checkpointer.get_checkpoint("myshard1") == "sequence1"
    assert checkpointer.get_checkpoint("myshard2") == "sequence2"
    assert checkpointer.get_all_checkpoints() == {"myshard1": "sequence1", "myshard2": "sequence2"}


@redis_only
def test_redis_checkpointer(mocker, redis_client):
    redis_client.hgetall.return_value = {}
    mocker.patch("redis.StrictRedis", return_value=redis_client)

    checkpointer = RedisCheckpointer()
    checkpointer.checkpoint("myshard1", "sequence1")
    checkpointer.checkpoint("myshard2", "sequence2")

    assert checkpointer.get_checkpoint("myshard1") == "sequence1"
    assert checkpointer.get_checkpoint("myshard2") == "sequence2"
    assert checkpointer.get_all_checkpoints() == {"myshard1": "sequence1", "myshard2": "sequence2"}
    assert redis_client.hset.mock_calls == [call("kinesis:sequences", "myshard1", "sequence1"),
                                            call("kinesis:sequences", "myshard2", "sequence2")]
