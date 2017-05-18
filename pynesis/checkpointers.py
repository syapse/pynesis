import abc
from typing import Optional, Dict  # noqa

from six import with_metaclass


class Checkpointer(with_metaclass(abc.ABCMeta, object)):  # type: ignore
    @abc.abstractmethod
    def checkpoint(self, shard_id, sequence):  # type: (str, str) -> None
        pass

    @abc.abstractmethod
    def get_checkpoint(self, shard_id):  # type: (str) -> Optional[str]
        pass

    @abc.abstractmethod
    def get_all_checkpoints(self):  # type: () -> Dict[str,str]
        pass


class InMemoryCheckpointer(Checkpointer):
    def __init__(self):
        self._checkpoints = {}

    def get_all_checkpoints(self):
        return self._checkpoints.copy()

    def get_checkpoint(self, shard_id):
        return self._checkpoints.get(shard_id)

    def checkpoint(self, shard_id, sequence):
        self._checkpoints[shard_id] = sequence


class RedisCheckpointer(InMemoryCheckpointer):
    def __init__(self, redis_host="localhost",  # type: str
                 redis_port=6379,  # type: int
                 redis_db=0,  # type: int
                 redis_timeout=1,  # type: int
                 key="kinesis:sequences",  # type:str
                 ):  # type: (...)->None

        from redis import StrictRedis

        self._redis_client = StrictRedis(host=redis_host, port=redis_port, db=redis_db,
                                         socket_timeout=redis_timeout, decode_responses=True,
                                         socket_connect_timeout=redis_timeout)
        self._key = key
        super(RedisCheckpointer, self).__init__()

    def checkpoint(self, shard, position):
        self._redis_client.hset(self._key, shard, position)
        return super(RedisCheckpointer, self).checkpoint(shard, position)

    def get_checkpoint(self, shard_id):
        if not self._checkpoints:
            self._load_checkpoints()
        return super(RedisCheckpointer, self).get_checkpoint(shard_id)

    def get_all_checkpoints(self):
        if not self._checkpoints:
            self._load_checkpoints()
        return super(RedisCheckpointer, self).get_all_checkpoints()

    def _load_checkpoints(self):
        self._checkpoints = self._redis_client.hgetall(self._key)
