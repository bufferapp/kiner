import pytest
import time

from moto import mock_kinesis
from kiner.producer import encode_data
from kiner.producer import KinesisProducer


@pytest.fixture
def producer():
    producer = KinesisProducer('test_stream', batch_size=50,
                               batch_time=1, threads=5)
    return producer


@pytest.fixture
def client(producer):
    return producer.kinesis_client


@pytest.mark.parametrize('data', [1, '1', b'1'])
def test_encode_data(data):
    assert encode_data(data) == b'1'


@mock_kinesis
@pytest.mark.parametrize('n', [1, 101, 179, 234, 399])
def test_send_records(producer, client, n):
    client.create_stream(StreamName=producer.stream_name, ShardCount=1)

    # Put records in the stream
    for i in range(n):
        producer.put_record(i)

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


@mock_kinesis
@pytest.mark.parametrize('n', [49, 141])
def test_send_records_without_close(producer, client, n):
    client.create_stream(StreamName=producer.stream_name, ShardCount=1)

    # Put records in the stream
    for i in range(n):
        producer.put_record(i)

    time.sleep(1.5)

    assert producer.queue.empty()

    producer.close()
