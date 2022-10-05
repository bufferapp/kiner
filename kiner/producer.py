import boto3
from concurrent.futures import ThreadPoolExecutor
import logging
from queue import Queue
import sys
import threading
import time
import uuid
import atexit

logger = logging.getLogger(__name__)


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
    batch_time : int
        Maximum of seconds to wait before flushing the queue.
    max_retries: int
        Maximum number of times to retry the put operation.
    kinesis_client: boto3.client
        Kinesis client.
    flush_callback: [int, float, Data=bytes, PartitionKey=str, Metadata=Any] -> None
        An optional callback to be invoked upon flushing to kinesis,
        called with the number of records flushed, time.time() when flush occurred
        and kwargs of the last record flushed

    Attributes
    ----------
    records : array
        Queue of formated records.
    pool: concurrent.futures.ThreadPoolExecutor
        Pool of threads handling client I/O.
    """

    def __init__(self, stream_name, batch_size=500,
                 batch_size_bytes=1024,
                 batch_time=5, max_retries=5, threads=10,
                 kinesis_client=None, flush_callback=None):
        self.stream_name = stream_name
        self.queue = Queue()
        self.queue_size_bytes = 0  # size of queue content
        self.batch_size = batch_size
        self.batch_size_bytes = batch_size_bytes
        self.batch_time = batch_time
        self.max_retries = max_retries
        if kinesis_client is None:
            kinesis_client = boto3.client('kinesis')
        self.kinesis_client = kinesis_client
        self.flush_callback = flush_callback
        self.pool = ThreadPoolExecutor(threads)
        self.last_flush = time.time()
        self.monitor_running = threading.Event()
        self.monitor_running.set()
        self.pool.submit(self.monitor)

        atexit.register(self.close)

    def monitor(self):
        """Flushes the queue periodically."""
        while self.monitor_running.is_set():
            if time.time() - self.last_flush > self.batch_time:
                if not self.queue.empty():
                    logger.info("Queue Flush: time without flush exceeded")
                    self.flush_queue()
            time.sleep(self.batch_time)

    def put_records(self, records, partition_key=None):
        """Add a list of data records to the record queue in the proper format.
        Convinience method that calls self.put_record for each element.

        Parameters
        ----------
        records : list
            Lists of records to send.
        partition_key: str
            Hash that determines which shard a given data record belongs to.

        """
        for record in records:
            self.put_record(record, partition_key)

    def put_record(self, data, metadata=None, partition_key=None):
        """Add data to the record queue in the proper format.

        Parameters
        ----------
        data : str|bytes
            Data to send.
        metadata: dict
            Metadata associated with the record.
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
            'PartitionKey': partition_key,
            'Metadata': metadata
        }
        record_bytes = sys.getsizeof(record) / 1048576 # 1048576=1024**2

        # Flush the queue if it reaches the batch size
        if self.queue.qsize() >= self.batch_size or self.queue_size_bytes >= self.batch_size_bytes:
            logger.info("Queue Flush: batch size reached")
            self.pool.submit(self.flush_queue)

        # Append the record
        logger.debug('Putting record "{}"'.format(record['Data'][:100]))
        self.queue.put(record)
        self.queue_size_bytes += record_bytes


    def close(self):
        """Flushes the queue and waits for the executor to finish."""
        logger.info('Closing producer')
        self.flush_queue()
        self.monitor_running.clear()
        self.pool.shutdown()
        logger.info('Producer closed')

    def flush_queue(self):
        """Grab all the current records in the queue and send them."""
        records = []
        records_size_bytes = 0

        while \
            not self.queue.empty() and \
                len(records) < self.batch_size and \
                records_size_bytes < self.batch_size_bytes:
            record = self.queue.get()

            record_size = sys.getsizeof(record) / 1048576 # 1048576=1024**2
            records_size_bytes += record_size
            self.queue_size_bytes -= record_size

            # Pop metadata from the queued record for notifying the callback
            last_metadata = record.pop('Metadata')
            records.append(record)

        if records:
            self.send_records(records)
            self.last_flush = time.time()
            if self.flush_callback:
                self.flush_callback(len(records), self.last_flush, Metadata=last_metadata, **records[-1])

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
            logger.warning('Writing {} records to file'.format(len(records)))
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
            logger.warning('Retrying failed records')
            failed_records = []
            for i, record in enumerate(response['Records']):
                if record.get('ErrorCode'):
                    failed_records.append(records[i])

            # Recursive call
            attempt += 1
            self.send_records(failed_records, attempt=attempt)
