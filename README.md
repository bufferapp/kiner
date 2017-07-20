# Kiner

Python AWS Kinesis Producer.

### Features

- Error handling and retrying with exponential backoff
- Automatic batching

Inspired by the AWS blog post [Implementing Efficient and Reliable Producers with the Amazon Kinesis Producer Library](https://aws.amazon.com/blogs/big-data/implementing-efficient-and-reliable-producers-with-the-amazon-kinesis-producer-library/).

## Installation

You can use `pip` to install Kiner.

```bash
pip install git+https://github.com/bufferapp/kiner
```

## Usage

To use Kiner, you'll need to have AWS authentication credentials configured
as stated in the [`boto3` documentation](https://boto3.readthedocs.io/en/latest/guide/quickstart.html#configuration)

```python
from kiner.producer import KinesisProducer

p = KinesisProducer('stream')

for i in range(10000):
    p.put_record(i)

p.flush()
```
