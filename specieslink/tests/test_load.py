import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from scripts.load import load_data_to_mongo

@patch('scripts.load.MongoClient')
def test_load_to_mongo_success(mock_mongo):
    mock_client = MagicMock()
    mock_db = MagicMock()
    mock_collection = MagicMock()

    mock_mongo.return_value = mock_client
    mock_client.__getitem__.return_value = mock_db
    mock_db.__getitem__.return_value = mock_collection

    data = pd.DataFrame([{"a": 1}, {"b": 2}, {"c": 3}])
    

    load_data_to_mongo(data)

    assert mock_collection.insert_many.called

    args, _ = mock_collection.insert_many.call_args

    assert len(args[0]) == 3