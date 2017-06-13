import pytest
from mock import MagicMock, call

from pynesis.checkpointers import Checkpointer
from .. import streams


def test_kinesis_record():
    record = streams.KinesisRecord({"SequenceNumber": "123", "Data": b'{"some": "json"}'})

    assert record.sequence_number == "123"
    assert record.data == b'{"some": "json"}'


def test_dummy_backend(mocker):
    time_mock = mocker.patch(streams.__name__ + ".time")
    dummy_backend = streams.DummyStream(
        fake_values=[b'{"_key": "123", "some": "thing"}', b'{"_key": "2", "other": "stuff"}'], loop=True)

    generator = dummy_backend.read()

    message = next(generator)
    assert message.data == b'{"_key": "123", "some": "thing"}'
    message = next(generator)
    assert message.data == b'{"_key": "2", "other": "stuff"}'
    message = next(generator)
    assert message.data == b'{"_key": "123", "some": "thing"}'
    assert len(time_mock.sleep.mock_calls) == 2


def test_dummy_backend_without_loop():
    dummy_backend = streams.DummyStream(
        fake_values=[b'{"_key": "123", "some": "thing"}', b'{"_key": "2", "other": "stuff"}'], loop=False)

    generator = dummy_backend.read()

    message = next(generator)
    assert message.data == b'{"_key": "123", "some": "thing"}'
    message = next(generator)
    assert message.data == b'{"_key": "2", "other": "stuff"}'
    with pytest.raises(StopIteration):
        next(generator)


def test_kinesis_backend(kinesis_client):
    kinesis_backend = streams.KinesisStream(
        stream_name="test-stream",
        region_name="us-east-1",
        batch_size=10,
        kinesis_client=kinesis_client,
    )
    generator = kinesis_backend.read()

    message = next(generator)
    assert message.data == b'{"_key": "1", "message": "message1"}'
    message = next(generator)
    assert message.data == b'{"_key": "2", "message": "message2"}'
    message = next(generator)
    assert message.data == b'{"_key": "3", "message": "message3"}'
    assert kinesis_client.get_shard_iterator.mock_calls == [
        call(ShardId="shard1", ShardIteratorType="TRIM_HORIZON", StreamName="test-stream")
    ]


def test_kinesis_backend_resumes_sequences(kinesis_client):
    checkpointer_mock = MagicMock(spec=Checkpointer)  # type: Checkpointer

    checkpointer_mock.get_checkpoint.side_effect = lambda x: {"shard1": "sequence3"}.get(x)

    kinesis_backend = streams.KinesisStream(
        stream_name="test-streams",
        region_name="us-east-1",
        checkpointer=checkpointer_mock,
        kinesis_client=kinesis_client)
    generator = kinesis_backend.read()
    next(generator)
    next(generator)

    assert kinesis_client.get_shard_iterator.mock_calls == [
        call(ShardId="shard1",
             ShardIteratorType="AFTER_SEQUENCE_NUMBER",
             StartingSequenceNumber="sequence3",
             StreamName="test-streams")
    ]
