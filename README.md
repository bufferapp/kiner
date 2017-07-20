# Kiner

Python AWS Kinesis Producer. Currently it has the next features:

- Error handling and retrying with exponential backoff
- Automatic batching

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
