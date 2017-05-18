from copy import deepcopy

import pytest
from mock import MagicMock


class CopyingMock(MagicMock):
    """
    Stores arguments to mock calls by value instead of by reference,
    see: https://docs.python.org/3/library/unittest.mock-examples.html#coping-with-mutable-arguments
    """

    def __call__(self, *args, **kwargs):
        args = deepcopy(args)
        kwargs = deepcopy(kwargs)
        return super(CopyingMock, self).__call__(*args, **kwargs)


@pytest.fixture
def kinesis_client():  # type: ()->MagicMock
    mock = MagicMock()
    paginate_mock = MagicMock()
    paginate_mock.side_effect = [[  # Each line is a response page
        {"StreamDescription": {"Shards": [{"ShardId": "shard1"}]}}
    ]]
    mock.get_paginator.return_value.paginate = paginate_mock
    mock.get_shard_iterator.return_value = {"ShardIterator": "iterator1"}
    mock.get_records.return_value = {
        "Records": [
            {"Data": '{"_key":"1", "message":"message1"}', "SequenceNumber": "sequence1"},
            {"Data": '{"_key":"2", "message":"message2"}', "SequenceNumber": "sequence2"},
            {"Data": '{"_key":"3", "message":"message3"}', "SequenceNumber": "sequence3"},
        ],
        "NextShardIterator": "iterator2"}
    return mock


@pytest.fixture
def redis_client():  # type: ()->MagicMock
    mock = CopyingMock()
    return mock


@pytest.fixture
def examples(mocker, kinesis_client, redis_client):
    mocker.patch("boto3.client", return_value=kinesis_client)

    if module_installed("redis"):
        mocker.patch("redis.StrictRedis", return_value=redis_client)

def module_installed(module):  # type: (str) -> bool
    try:
        __import__(module)
        return True
    except ImportError:
        return False


django_only = pytest.mark.skipif(not module_installed("django"), reason="requires django")
redis_only = pytest.mark.skipif(not module_installed("redis"), reason="requires redis")
