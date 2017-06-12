import abc
import logging
import time
from datetime import datetime
from itertools import cycle
from threading import local
from typing import Dict, Generator, List, Optional, Tuple, Any  # noqa

import boto3
from six import with_metaclass

from pynesis.checkpointers import Checkpointer, InMemoryCheckpointer  # noqa

try:
    import json
except ImportError:
    import simplejson as json  # type: ignore

try:
    JSONDecodeError = json.JSONDecodeError
except AttributeError:
    JSONDecodeError = ValueError

_cache = local()

logger = logging.getLogger(__name__)


class KinesisGetRecordsResponse(object):
    def __init__(self, raw_response):  # type: (Dict) -> None
        self._raw_response = raw_response

    @property
    def records(self):  # type: ()->List[KinesisRecord]
        return [KinesisRecord(record) for record in self._raw_response.get("Records", [])]

    @property
    def next_shard_iterator(self):  # type: ()->str
        return self._raw_response.get("NextShardIterator", "")


class KinesisDescribeStreamResponse(object):
    def __init__(self, raw_response):  # type: (Dict)->None
        self._raw_response = raw_response

    @property
    def shards(self):  # type: () -> List[KinesisShard]
        return [KinesisShard(shard) for shard in self._raw_response.get("StreamDescription", {}).get("Shards", [])]


class KinesisShard(object):
    def __init__(self, raw_shard):  # type: (Dict) -> None
        self._raw_shard = raw_shard

    @property
    def id(self):  # type ()->str
        return self._raw_shard.get("ShardId", "")


class KinesisRecord:
    def __init__(self, raw_record):  # type: (Dict) -> None
        self._raw_record = raw_record or {}

    @property
    def sequence(self):  # type ()-> str
        return str(self._raw_record.get("SequenceNumber", ""))

    @property
    def data(self):  # type: () -> Dict
        raw_data = self._raw_record.get("Data", b"")
        data = {}  # type: Dict
        try:
            data = json.loads(raw_data)
        except JSONDecodeError:
            logger.error("Cannot decode JSON payload from Kinesis Record: {}".format(raw_data))
        return data


class KinesisPutRecordRequest:
    def __init__(self, stream_name, data, key):  # type: (str,str,str)->None
        self._stream_name = stream_name
        self._data = data.encode()
        self._key = key

    def build(self):  # type: ()-> Dict
        return {
            "StreamName": self._stream_name,
            "Data": self._data,
            "PartitionKey": self._key,
        }


class Backend(with_metaclass(abc.ABCMeta)):  # type: ignore
    def __init__(self, *args, **kwargs):
        self._stop = False

    def stop(self):  # type: () -> None
        """
        Stops the yielding of records from the read() method and makes it return
        """
        self._stop = True

    @abc.abstractmethod
    def read(self):  # type: ()-> Generator[Dict, None, None]
        """
        Yields records from the stream, one at a time
        """

    @abc.abstractmethod
    def put(self, key, message):  # type: (str,Dict) -> None
        """
        Puts a record into a kinesis stream
        """


class KinesisBackend(Backend):
    """
    Kinesis stream backend
    """
    TYPE = "kinesis"

    def __init__(self,
                 stream_name,  # type: str
                 region_name,  # type: str
                 aws_access_key_id=None,  # type: str
                 aws_secret_access_key=None,  # type: str
                 kinesis_client=None,  # type: Any
                 batch_size=10000,  # type: int
                 read_interval=1,  # type: int
                 shard_sync_interval=60,  # type: int
                 checkpointer=None  # type: Checkpointer
                 ):  # type: (...) -> None
        super(KinesisBackend, self).__init__()
        self._stream_name = stream_name
        self._batch_size = batch_size
        self._read_interval = read_interval
        self._shard_sync_interval = shard_sync_interval
        self._checkpointer = checkpointer  # type: Checkpointer

        if self._checkpointer is None:
            self._checkpointer = InMemoryCheckpointer()

        self._kinesis_client = kinesis_client
        if self._kinesis_client is None:
            self._kinesis_client = boto3.client("kinesis", region_name=region_name, aws_access_key_id=aws_access_key_id,
                                                aws_secret_access_key=aws_secret_access_key)

        self._shards = []  # type: List[str]
        self._shards_sync_time = None  # type: Optional[datetime]

    def put(self, key, record):  # type: (str, Dict) -> None
        kinesis_record = KinesisPutRecordRequest(stream_name=self._stream_name, data=json.dumps(record),
                                                 key=key)
        self._kinesis_client.put_record(**kinesis_record.build())

    def read(self):  # type: (...) -> Generator[Dict, None, None]
        """
        Yields records from Kinesis one at a time.
        The process starts by loading the last processed positions by shard,
        then pulls a batch of events from each shard in a round-robin fashion until stop() is called
        """

        shard_iterators = {}  # type: Dict[str, str]
        while not self._stop:
            self._update_shard_iterators(shard_iterators)
            for shard_id, iterator in shard_iterators.items():
                records, next_iterator = self._get_records(iterator)
                for record in records:
                    yield record.data
                    self._checkpointer.checkpoint(shard_id, record.sequence)
                shard_iterators[shard_id] = next_iterator
            time.sleep(self._read_interval)

    def _update_shard_iterators(self, iterators):  # type: (Dict[str, str]) -> Dict[str, str]
        for shard_id in self._get_active_shards():
            if shard_id not in iterators:
                sequence = self._checkpointer.get_checkpoint(shard_id)
                iterators[shard_id] = self._get_shard_iterator(shard_id, sequence)
        return iterators

    def _get_records(self, iterator):  # type: (str) -> Tuple[List[KinesisRecord], str]
        raw_response = self._kinesis_client.get_records(
            ShardIterator=iterator,
            Limit=self._batch_size,
        )
        records = []
        response = KinesisGetRecordsResponse(raw_response)
        for record in response.records:
            records.append(record)
        return records, response.next_shard_iterator

    def _get_active_shards(self):  # type: ()-> List[str]
        current_time = datetime.now()
        if self._shards_sync_time is not None:
            if (current_time - self._shards_sync_time).seconds < self._shard_sync_interval:
                return self._shards

        self._shards = []
        paginator = self._kinesis_client.get_paginator("describe_stream")
        for raw_response in paginator.paginate(StreamName=self._stream_name):
            response = KinesisDescribeStreamResponse(raw_response)
            for shard_info in response.shards:
                self._shards.append(shard_info.id)
        self._shards_sync_time = current_time
        return self._shards

    def _get_shard_iterator(self, shard_id, sequence=None):  # type: (str,str) -> str
        request = {
            "StreamName": self._stream_name,
            "ShardId": shard_id,
        }

        iterator_type = "TRIM_HORIZON"
        if sequence is not None:
            iterator_type = "AFTER_SEQUENCE_NUMBER"
            request["StartingSequenceNumber"] = sequence
        request["ShardIteratorType"] = iterator_type
        response = self._kinesis_client.get_shard_iterator(**request)
        return str(response.get("ShardIterator"))


class DummyBackend(Backend):
    """
    A dummy Backend implementation that always yields the same dummy record
    """

    TYPE = "dummy"
    _DEFAULT_FAKE_VALUES = [{"_id": "1", "_type": "fake", "body": "Fake event from Dummy kinesis backend"}]

    def __init__(self, fake_values=None, loop=False, **options):  # type: (List[Dict], Any, bool) -> None
        super(DummyBackend, self).__init__(**options)
        if fake_values is None:
            fake_values = self._DEFAULT_FAKE_VALUES
        self._fake_values = fake_values
        self._loop = loop

    def read(self):  # type: ()->Generator[Dict, None, None]
        if self._loop:
            fake_values = cycle(self._fake_values)
        else:
            fake_values = self._fake_values

        for message in fake_values:
            yield message
            if self._stop:
                break
            time.sleep(1)

    def put(self, key, message):  # type: (str,Dict) -> None
        print("Sending outgoing message to eventbus: {}".format(json.dumps(message)))
