import pytest
from unittest.mock import patch
from scripts.extract import get_data_from_api, build_url
import requests


@pytest.fixture
def mock_api_response():
    with patch("scripts.extract.requests.get") as mock_get:
        yield mock_get


def test_get_data_from_api_success(mock_api_response):
    mock_api_response.return_value.status_code = 200
    mock_api_response.return_value.json.return_value = {"data": "ok"}

    url = "https://fakeurl.com"
    result = get_data_from_api(url)

    assert result == {"data": "ok"}



def test_get_data_from_api_failure(mock_api_response):
    
    mock_api_response.side_effect = requests.exceptions.RequestException("Erro simulado")

    url = "https://fakeurl.com"
    result = get_data_from_api(url)

    assert result is None

def test_biuld_url():
    offset = 1000
    limit = 5000
    url = build_url(offset, limit)
    assert f"offset={offset}" in url
    assert f"limit={limit}" in url