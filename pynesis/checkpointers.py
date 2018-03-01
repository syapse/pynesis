import abc
from typing import Optional, Dict  # noqa

from six import with_metaclass


class Checkpointer(with_metaclass(abc.ABCMeta, object)):  # type: ignore
    """
    Checkpointer is the interface for persisting positions for Kinesis
    shards.

    Usually you will call checkpoint("shard", "sequence") to indicate that, up
    to that sequence (inclusive) the records from the shard for a given stream
    has been processed.

    You will need an individual Checkpointer for each stream in your application.
    """
    @abc.abstractmethod
    def checkpoint(self, shard_id, sequence):  # type: (str, str) -> None
        """
        Persist the sequence id for a given shard
        """

    @abc.abstractmethod
    def get_checkpoint(self, shard_id):  # type: (str) -> Optional[str]
        """
        Get the sequence id of the last succesfully processed record
        """

    @abc.abstractmethod
    def get_all_checkpoints(self):  # type: () -> Dict[str,str]
        """
        Get a dictionary whose keys are all the shard ids we are aware of, and whose
        values are the sequence id of the last record processed for its shard
        """


class InMemoryCheckpointer(Checkpointer):
    """
    A purely in-memory implementation of a Checkpointer. Intended to be used
    for development, testing, etc.
    """

    def __init__(self):
        self._checkpoints = {}

    def get_all_checkpoints(self):
        return self._checkpoints.copy()

    def get_checkpoint(self, shard_id):
        return self._checkpoints.get(shard_id)

    def checkpoint(self, shard_id, sequence):
        self._checkpoints[shard_id] = sequence


class RedisCheckpointer(InMemoryCheckpointer):
    """
    A redis based checkpointer implementation.
    You must have an individual Checkpointer instance for each Kinesis stream you connect to,
    and each RedisCheckpointer instance should have an individual `key` so that shard sequences
    are not mixed with different streams information.
    """
    def __init__(self, redis_host="localhost",  # type: str
                 redis_port=6379,  # type: int
                 redis_db=0,  # type: int
                 redis_timeout=1,  # type: int
                 key="kinesis:sequences",  # type: str
                 redis_password=None,  # type: Optional[str]
                 ):  # type: (...)->None

        from redis import StrictRedis

        self._redis_client = StrictRedis(host=redis_host, port=redis_port, db=redis_db,
                                         socket_timeout=redis_timeout, decode_responses=True,
                                         password=redis_password, socket_connect_timeout=redis_timeout)
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


class DynamoCheckpointer(Checkpointer):
    """
    DynamoDB based checkpointer implementation.

    Expects a Dynamo instance set up with a key of type S for the shard, where the value
    will be the sequence number for that shard. There is no concurrency control, so only
    one process may manipulate a given shard key at a time.

    Raises botocore.errorfactory.ResourceNotFoundException if the given table does not exist.
    """
    def __init__(self, table_name=None,  # type: str
                 region_name=None,  # type: str
                 key='shard_id',  # type: str
                 position_field='sequence_number',  # type: str
                 endpoint=None,  # type: str
                 ):  # type: (...)->None
        import boto3

        self._table = table_name
        self._key = key
        self._position_field = position_field
        self._endpoint = endpoint

        self._table = boto3.resource('dynamodb', region_name=region_name).Table(table_name)


    def checkpoint(self, shard, position):  # type: (str, str) -> None
        self._table.put_item(Item={self._key: shard, self._position_field: position})


    def get_checkpoint(self, shard_id):  # type: (str) -> Optional[str]
        """
        Returns the sequence number associated with the given shard. If it hasn't
        been set yet, None is returned.
        """
        item = self._table.get_item(Key={self._key: shard_id}).get('Item', {})
        return item.get(self._position_field)


    def get_all_checkpoints(self):  # type: () -> Dict[str,str]
        return { i[self._key]: i[self._position_field]
                 for i in self._table.scan()['Items'] }
