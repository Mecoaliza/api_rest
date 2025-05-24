import pytest
from scripts.transform import DataProcessor

def test_remove_type_key():
    sample = [{'type': 'Feature', 'properties': {'name': 'A'}}]
    result = DataProcessor.remove_type_key(sample)
    assert 'type' not in result[0]