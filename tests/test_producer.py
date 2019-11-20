import pytest
import time

from mock import Mock, patch, ANY
from moto import mock_kinesis
from kiner.producer import encode_data
from kiner.producer import KinesisProducer


BATCH_SIZE=50

@pytest.fixture
def flush_callback():
    return Mock()

@pytest.fixture
def producer(flush_callback):
    producer = KinesisProducer('test_stream', batch_size=BATCH_SIZE,
                               batch_time=1, threads=3, flush_callback=flush_callback)
    return producer


@pytest.fixture
def client(producer):
    return producer.kinesis_client


@pytest.mark.parametrize('data', [1, '1', b'1'])
def test_encode_data(data):
    assert encode_data(data) == b'1'


@mock_kinesis
@pytest.mark.parametrize('n', [1, 101, 179, 234, 399])
def test_send_records(producer, client, n, flush_callback):
    client.create_stream(StreamName=producer.stream_name, ShardCount=1)

    with patch('kiner.producer.time') as mock_time:
        # Put records in the stream
        for i in range(n - 1):
            producer.put_record(i, metadata={'i': i, 'n': n})
        producer.put_record(n - 1, metadata={'i': n - 1, 'n': n}, partition_key='some-partition-key')

        producer.close()

        # Assert flush callback called for at least as many batches were sent
        assert flush_callback.call_count >= n // BATCH_SIZE + 1
        # Assert the final record was flushed
        flush_callback.assert_any_call(
            ANY, mock_time.time(), Data=str(n - 1).encode(), Metadata={'i': n-1, 'n': n}, PartitionKey='some-partition-key'
        )
        # Assert we flushed n records
        assert sum(call[1][0] for call in flush_callback.mock_calls) == n

    response = client.describe_stream(StreamName=producer.stream_name)
    shard_id = response['StreamDescription']['Shards'][0]['ShardId']
    shard_iterator = client.get_shard_iterator(
        StreamName=producer.stream_name,
        ShardId=shard_id,
        ShardIteratorType='TRIM_HORIZON'
    ).get('ShardIterator')

    records = client.get_records(ShardIterator=shard_iterator, Limit=n)['Records']

    assert len(records) == n


@mock_kinesis
@pytest.mark.parametrize('n', [49, 141])
def test_send_records_without_close(producer, client, n):
    client.create_stream(StreamName=producer.stream_name, ShardCount=1)

    # Put records in the stream
    for i in range(n):
        producer.put_record(i)

    time.sleep(2)

    assert producer.queue.empty()

    producer.close()

    response = client.describe_stream(StreamName=producer.stream_name)
    shard_id = response['StreamDescription']['Shards'][0]['ShardId']
    shard_iterator = client.get_shard_iterator(
        StreamName=producer.stream_name,
        ShardId=shard_id,
        ShardIteratorType='TRIM_HORIZON'
    ).get('ShardIterator')

    records = client.get_records(ShardIterator=shard_iterator, Limit=n)['Records']
    assert len(records) == n
