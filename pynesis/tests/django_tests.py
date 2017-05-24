import pytest
from mock import MagicMock

from pynesis import checkpointers, backends
from pynesis.djangoutils import get_stream, DjangoCheckpointer
from pynesis.tests.conftest import django_only


@django_only
@pytest.mark.django_db
def test_django_model_checkpointer():
    checkpointer = DjangoCheckpointer(key="my-stream")
    checkpointer.checkpoint("my-shard1", "sequence1")
    checkpointer.checkpoint("my-shard2", "sequence2")

    assert checkpointer.get_checkpoint("my-shard1") == "sequence1"
    assert checkpointer.get_checkpoint("my-shard2") == "sequence2"
    assert checkpointer.get_all_checkpoints() == {"my-shard1": "sequence1", "my-shard2": "sequence2"}


@django_only
def test_get_stream_django(mocker, settings):
    settings.PYNESIS_CONFIG = {
        "stream1": {
            "BACKEND": "pynesis.backends.KinesisBackend",
            "CHECKPOINTER": "pynesis.checkpointers.InMemoryCheckpointer",
            "BACKEND_OPTIONS": {
                "stream_name": "my-stream-1",
                "region_name": "us-east-1",
            }
        },
        "stream2": {
            "BACKEND": "pynesis.backends.KinesisBackend",
            "CHECKPOINTER": "pynesis.checkpointer.InMemoryCheckpointer",
            "BACKEND_OPTIONS": {
                "stream_name": "my-stream-2",
                "region_name": "us-east-1",
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
    assert len(kinesis_class_mock.call_args[1]) == 3
    checkpointer = kinesis_class_mock.call_args[1]["checkpointer"]
    assert isinstance(checkpointer, checkpointers.Checkpointer)
    assert kinesis_class_mock.call_args[1]["stream_name"] == "my-stream-1"
    assert kinesis_class_mock.call_args[1]["region_name"] == "us-east-1"
