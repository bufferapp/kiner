import boto3
import time
import uuid
import logging


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
    client: boto3.client
        Kinesis client.
    """
    def __init__(self, stream_name, batch_size=500, max_retries=5):
        self.stream_name = stream_name
        self.records = []
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.client = boto3.client('kinesis')

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
        data = str(data).encode()

        # Create a random partition key if not provided
        if not partition_key:
            partition_key = uuid.uuid4().hex

        # Build the record
        record = {
            'Data': data,
            'PartitionKey': partition_key
        }

        # Flush the queue if it reaches the batch size
        if len(self.records) == self.batch_size:
            self.flush()

        # Append the record
        self.records.append(record)

    def flush(self):
        """Put all the current records in the queue into the stream."""
        records = self.records

        # Return if there are no records
        if not records:
            return

        response = self.client.put_records(StreamName=self.stream_name,
                                           Records=records)
        failed_record_count = response['FailedRecordCount']
        retries = 1

        # Retry failed records
        while failed_record_count > 0 and retries <= self.max_retries:
            logging.info('Retrying {} records'.format(failed_record_count))

            # Sleep before resending
            time.sleep(2 ** retries * .1)

            # Grab the failed records
            failed_records = []
            for i, record in enumerate(response['Records']):
                if record.get('ErrorCode'):
                    failed_records.append(records[i])

            response = self.client.put_records(StreamName=self.stream_name,
                                               Records=failed_records)

            failed_record_count = response['FailedRecordCount']

            retries += 1
            records = failed_records

        # Empty the record queue
        self.records = []

        # Handle
        if failed_record_count:
            logging.warning('{} records failed'.format(failed_record_count))
