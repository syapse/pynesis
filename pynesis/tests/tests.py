from mock import MagicMock

from pynesis import checkpointers
from pynesis.djangoutils import get_stream
from pynesis.tests.conftest import django_only
from .. import backends


@django_only
def test_get_stream_django(mocker, settings):
    settings.PYNESIS_CONFIG = {
        "stream1": {
            "BACKEND": "pynesis.backends.KinesisBackend",
            "CHECKPOINTER": "pynesis.checkpointers.InMemoryCheckpointer",
            "BACKEND_OPTIONS": {
                "stream_name": "my-stream-1",
            }
        },
        "stream2": {
            "BACKEND": "pynesis.backends.KinesisBackend",
            "CHECKPOINTER": "pynesis.checkpointer.InMemoryCheckpointer",
            "BACKEND_OPTIONS": {
                "stream_name": "my-stream-2",
            }
        }
    }
    import_string_mock = mocker.patch("django.utils.module_loading.import_string")
    kinesis_class_mock = MagicMock(return_value=MagicMock(spec=backends.Backend))  # type: ignore
    checkpointer_class_mock = MagicMock(return_value=MagicMock(spec=checkpointers.Checkpointer))  # type: ignore

    def fake_importer(class_):
        if class_ == "pynesis.backends.KinesisBackend":
            return kinesis_class_mock
        if class_ == "pynesis.checkpointers.InMemoryCheckpointer":
            return checkpointer_class_mock

    import_string_mock.side_effect = fake_importer

    get_stream(name="stream1")

    assert len(kinesis_class_mock.call_args_list) == 1
    checkpointer = kinesis_class_mock.call_args[1]["checkpointer"]
    assert len(kinesis_class_mock.call_args[1]) == 2
    assert isinstance(checkpointer, checkpointers.Checkpointer)
    assert kinesis_class_mock.call_args[1]["stream_name"] == "my-stream-1"

#
#
# def test_kinesis_record() -> None:
#     record = backends.KinesisRecord({"SequenceNumber": "123", "Data": b'{"some":"json"}'})
#
#     assert record.sequence == "123"
#     assert record.data == {"some": "json"}
#
#
# def test_dummy_backend(mocker: Any) -> None:
#     time_mock = mocker.patch(backends.__name__ + ".time")
#     dummy_backend = backends.DummyBackend(
#         fake_values=[{"_key": "123", "some": "thing"}, {"_key": "2", "other": "stuff"}])
#
#     generator = dummy_backend.read()
#
#     message = next(generator)
#     assert message == {"_key": "123", "some": "thing"}
#     message = next(generator)
#     assert message == {"_key": "2", "other": "stuff"}
#     message = next(generator)
#     assert message == {"_key": "123", "some": "thing"}
#     assert len(time_mock.sleep.mock_calls) == 2
#
#
# def test_kinesis_backend(mocker: Any, kinesis_client: MagicMock, redis_client: MagicMock) -> None:
#     # Boto/kinesis mocks
#     get_service_client_mock = mocker.patch(backends.__name__ + ".get_service_client")
#     get_service_client_mock.return_value = kinesis_client
#
#     # Redis mocks
#     mocker.patch(backends.__name__ + ".StrictRedis", new=MagicMock(return_value=redis_client))  # type: ignore
#
#     kinesis_backend = backends.KinesisBackend(
#         stream_name="test-stream",
#         batch_size=10,
#         redis_host="127.0.0.1")
#     generator = kinesis_backend.read()
#
#     message = next(generator)
#     assert message == {"_key": "1", "message": "message1"}
#     message = next(generator)
#     assert message == {"_key": "2", "message": "message2"}
#     message = next(generator)
#     assert message == {"_key": "3", "message": "message3"}
#     assert redis_client.hmset.mock_calls == [
#         call("kinesis:sequences", {"shard1": "sequence1"}),
#         call("kinesis:sequences", {"shard1": "sequence2"}),
#     ]
#     assert kinesis_client.get_shard_iterator.mock_calls == [
#         call(ShardId="shard1", ShardIteratorType="TRIM_HORIZON", StreamName="test-stream")
#     ]
#
#
# def test_kinesis_backend_non_json_record(mocker: Any, kinesis_client: MagicMock, redis_client: MagicMock) -> None:
#     logger_mock = mocker.patch(backends.__name__ + ".logger")
#     # Setup kinesis GetRecords response
#     kinesis_client.get_records.return_value = {
#         "Records": [
#             {"Data": "This is not JSON"}
#         ],
#         "NextShardIterator": "iterator2"}
#
#     # Boto/kinesis mocks
#     get_service_client_mock = mocker.patch(backends.__name__ + ".get_service_client")
#     get_service_client_mock.return_value = kinesis_client
#
#     # Redis mocks
#     mocker.patch(backends.__name__ + ".StrictRedis", new=MagicMock(return_value=redis_client))  # type: ignore
#
#     kinesis_backend = backends.KinesisBackend(
#         stream_name="test-stream",
#         batch_size=10,
#         redis_host="127.0.0.1")
#     generator = kinesis_backend.read()
#
#     message = next(generator)
#     assert message == {}
#     assert logger_mock.error.mock_calls == [call("Cannot decode JSON payload from Kinesis Record: This is not JSON")]
#
#
# def test_kinesis_backend_resumes_sequences(mocker: Any, kinesis_client: MagicMock, redis_client: MagicMock) -> None:
#     # Boto/kinesis mocks
#     get_service_client_mock = mocker.patch(backends.__name__ + ".get_service_client")
#     get_service_client_mock.return_value = kinesis_client
#
#     # Redis mocks
#     mocker.patch(backends.__name__ + ".StrictRedis", new=MagicMock(return_value=redis_client))  # type: ignore
#     redis_client.hgetall.return_value = {
#         "shard1": "sequence3"
#     }
#
#     kinesis_backend = backends.KinesisBackend(stream_name="test-streams")
#     generator = kinesis_backend.read()
#     next(generator)
#     next(generator)
#
#     assert kinesis_client.get_shard_iterator.mock_calls == [
#         call(ShardId="shard1",
#              ShardIteratorType="AFTER_SEQUENCE_NUMBER",
#              StartingSequenceNumber="sequence3",
#              StreamName="test-streams")
#     ]
