import pytest
from scripts.etl_functions import read_json, remove_type_key

def test_read_json_return_list():
    data = read_json('tests/mock_data.json')
    assert isinstance(data, list)
    assert 'properties' in data[0]