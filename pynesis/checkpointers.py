import abc

from six import with_metaclass


class Checkpointer(with_metaclass(abc.ABCMeta, object)):
    @abc.abstractmethod
    def checkpoint(self, shard_id, sequence):  # type: (str, str) -> None
        pass

    @abc.abstractmethod
    def get_checkpoint(self, shard_id):  # type: (str) -> Optional[str]
        pass

    @abc.abstractmethod
    def get_all_checkpoints(self):  # type: () -> Dict[str,str]
        pass


#
# class RedisCheckpointer(Checkpointer):
#    _REDIS_SEQUENCES_KEY = "kinesis:sequences"
#
#     def __init__(self, redis_host="localhost", redis_port=6379, redis_db=0, redis_timeout=1):
#         self._redis_client = StrictRedis(host=redis_host, port=redis_port, db=redis_db,
#                                          socket_timeout=redis_timeout, decode_responses=True,
#                                          socket_connect_timeout=redis_timeout)
#
#     def get_position(self, shard):
#         pass
#
#     def checkpoint(self, shard, position):
#         self._redis_client.hmset(self._REDIS_SEQUENCES_KEY, self._sequences)
#
# def get_all_checkpoints:
#             self._checkpoints = self._redis_client.hgetall(self._REDIS_SEQUENCES_KEY)




class InMemoryCheckpointer(Checkpointer):
    def __init__(self):
        self._checkpoints = {}

    def get_all_checkpoints(self):
        return self._checkpoints.copy()

    def get_checkpoint(self, shard_id):
        return self._checkpoints.get(shard_id)

    def checkpoint(self, shard_id, sequence):
        self._checkpoints[shard_id] = sequence
