import pytest
from unittest.mock import patch
from scripts.extract import get_data_from_api, build_url
import requests


@patch('scripts.extract.requests.get')
def test_get_data_from_api_success(mock_get):
    mock_response = mock_get.return_value
    mock_response.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"data": "ok"}

    url = "https://fakeurl.com"
    result = get_data_from_api(url)

    assert result == {"data": "ok"}


@patch("scripts.extract.requests.get")
def test_get_data_from_api_failure(mock_get):
    
    mock_get.side_effect = requests.exceptions.RequestException("Erro simulado")

    url = "https://fakeurl.com"
    result = get_data_from_api(url)

    assert result is None

def test_biuld_url():
    offset = 1000
    limit = 5000
    url = build_url(offset, limit)
    assert f"offset={offset}" in url
    assert f"limit={limit}" in url