from threading import local

from pynesis.backends import Backend
from pynesis.checkpointers import Checkpointer

_cache = local()


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
