import pytest

from moto import mock_kinesis
from kiner.producer import encode_data
from kiner.producer import KinesisProducer


@pytest.fixture
def producer():
    producer = KinesisProducer('test_stream', batch_size=100)
    return producer


@pytest.fixture
def client(producer):
    return producer.kinesis_client


@pytest.mark.parametrize('data', [1, '1', b'1'])
def test_encode_data(data):
    assert encode_data(data) == b'1'


@mock_kinesis
@pytest.mark.parametrize('n', [1, 100, 1000])
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

    records = client.get_records(ShardIterator=shard_iterator)['Records']
    assert len(records) == n
