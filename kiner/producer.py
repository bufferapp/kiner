import boto3
import time
import uuid
from queue import Queue
from concurrent.futures import ThreadPoolExecutor


def encode_data(data, encoding='utf_8'):
    if isinstance(data, bytes):
        return data
    else:
        return str(data).encode(encoding)


class KinesisProducer:
    """Basic Kinesis Producer.

    Parameters
    ----------
    stream_name : string
        Name of the stream to send the records.
    batch_size : int
        Numbers of records to batch before flushing the queue.
    max_retries: int
        Maximum number of times to retry the put operation.

    Attributes
    ----------
    records : array
        Queue of formated records.
    kinesis_client: boto3.client
        Kinesis client.
    pool: concurrent.futures.ThreadPoolExecutor
        Pool of threads handling client I/O.
    """
    def __init__(self, stream_name, batch_size=500, max_retries=5, threads=10):
        self.stream_name = stream_name
        self.queue = Queue()
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.pool = ThreadPoolExecutor(threads)
        self.kinesis_client = boto3.client('kinesis')

    def put_record(self, data, partition_key=None):
        """Add data to the record queue in the proper format.

        Parameters
        ----------
        data : str
            Data to send.
        partition_key: str
            Hash that determines which shard a given data record belongs to.

        """
        # Byte encode the data
        data = encode_data(data)

        # Create a random partition key if not provided
        if not partition_key:
            partition_key = uuid.uuid4().hex

        # Build the record
        record = {
            'Data': data,
            'PartitionKey': partition_key
        }

        # Flush the queue if it reaches the batch size
        if self.queue.qsize() >= self.batch_size:
            self.pool.submit(self.flush_queue)

        # Append the record
        self.queue.put(record)

    def close(self):
        """Flushes the queue and waits for the executor to finish."""
        self.flush_queue()
        self.pool.shutdown()

    def flush_queue(self):
        """Grab all the current records in the queue and send them."""
        records = []

        while not self.queue.empty() and len(records) < self.batch_size:
            records.append(self.queue.get())

        if records:
            self.send_records(records)

    def send_records(self, records, attempt=0):
        """Send records to the Kinesis stream.

        Falied records are sent again with an exponential backoff decay.

        Parameters
        ----------
        records : array
            Array of formated records to send.
        attempt: int
            Number of times the records have been sent without success.
        """

        # If we already tried more times than we wanted, save to a file
        if attempt > self.max_retries:
            with open('failed_records.dlq', 'ab') as f:
                for r in records:
                    f.write(r.get('Data'))
            return

        # Sleep before retrying
        if attempt:
            time.sleep(2 ** attempt * .1)

        response = self.kinesis_client.put_records(StreamName=self.stream_name,
                                                   Records=records)
        failed_record_count = response['FailedRecordCount']

        # Grab failed records
        if failed_record_count:
            failed_records = []
            for i, record in enumerate(response['Records']):
                if record.get('ErrorCode'):
                    failed_records.append(records[i])

            # Recursive call
            attempt += 1
            self.send_records(failed_records, attempt=attempt)
