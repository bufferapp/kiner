from kiner.producer import encode_data
import pytest


@pytest.mark.parametrize('data', [1, '1', b'1'])
def test_encode_data(data):
    assert encode_data(data) == b'1'
