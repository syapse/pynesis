from threading import local
from typing import Dict  # noqa

from pynesis.backends import Backend
from pynesis.checkpointers import Checkpointer
from pynesis.models import Checkpoint

_cache = local()

try:
    import json
except ImportError:
    import simplejson as json  # type: ignore

try:
    JSONDecodeError = json.JSONDecodeError
except AttributeError:
    JSONDecodeError = ValueError


def get_stream(name):  # type: (str) -> Backend
    """
    This is a helper method which will return a Backend instance whose configuration
    will be obtained from django settings module

    See the project README for examples

    """
    from django.conf import settings
    from django.utils.module_loading import import_string

    try:
        backend_instance = _cache.instance
    except AttributeError:
        pynesis_config = getattr(settings, "PYNESIS_CONFIG", {}).get(name, {})
        backend_options = pynesis_config.get("BACKEND_OPTIONS", {})
        checkpointer_options = pynesis_config.get("CHECKPOINTER_OPTIONS", {})
        backend_class = import_string(pynesis_config.get("BACKEND", "pynesis.backends.DummyBackend"))
        checkpointer_class = import_string(
            pynesis_config.get("CHECKPOINTER", "pynesis.checkpointers.InMemoryCheckpointer"))
        checkpointer_instance = checkpointer_class(**checkpointer_options)
        backend_instance = backend_class(checkpointer=checkpointer_instance, **backend_options)
        assert isinstance(backend_instance, Backend)
        assert isinstance(checkpointer_instance, Checkpointer)
        _cache.instance = backend_instance
    return backend_instance


class DjangoCheckpointer(Checkpointer):
    def __init__(self, key="checkpoint"):
        self._key = key
        self._checkpoints = {}

    def get_all_checkpoints(self):  # type: ()->Dict[str,str]
        if not self._checkpoints:
            checkpoint, created = Checkpoint.objects.get_or_create(key=self._key, checkpoints="")
            self._checkpoints = json.loads(checkpoint.checkpoints)
        return self._checkpoints

    def checkpoint(self, shard_id, sequence):  # type: (str,str) -> None
        self._checkpoints[shard_id] = sequence
        checkpoint_data = json.dumps(self._checkpoints)
        Checkpoint.objects.update_or_create(key=self._key, defaults={"checkpoints": checkpoint_data})

    def get_checkpoint(self, shard_id):  # type: (str)->str
        if not self._checkpoints:
            self.get_all_checkpoints()
        return self._checkpoints.get(shard_id)
