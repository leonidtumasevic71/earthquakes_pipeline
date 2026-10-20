import os
import json
from unittest.mock import MagicMock, Mock, patch
import pytest
import requests
from src import loader

# Тестовые переменные окружения для импорта src.loader.
os.environ.setdefault("API_URL", "http://localhost/test")
os.environ.setdefault("RUSTFS_ACCESS_KEY", "test_key")
os.environ.setdefault("RUSTFS_SECRET_KEY", "test_secret")
os.environ.setdefault(
    "DB_CONN_STRING",
    "postgresql://test:test@localhost/test",
)




# Проверяет, что существующий бакет не создаётся повторно.
@patch.object(loader, "client")
def test_bucket_exists(mock_client):
    mock_client.bucket_exists.return_value = True

    loader.bucket_check("earthquakes")

    mock_client.bucket_exists.assert_called_once_with("earthquakes")
    mock_client.make_bucket.assert_not_called()


# Проверяет, что отсутствующий бакет создаётся.
@patch.object(loader, "client")
def test_bucket_does_not_exist(mock_client):
    mock_client.bucket_exists.return_value = False

    loader.bucket_check("earthquakes")

    mock_client.bucket_exists.assert_called_once_with("earthquakes")
    mock_client.make_bucket.assert_called_once_with("earthquakes")


# Проверяет, что ошибка RustFS преобразуется в RuntimeError.
@patch.object(loader, "client")
def test_bucket_check_error(mock_client):
    mock_client.bucket_exists.side_effect = Exception("Connection error")

    with pytest.raises(RuntimeError) as exc_info:
        loader.bucket_check("earthquakes")

    assert isinstance(exc_info.value.__cause__, Exception)
    assert str(exc_info.value.__cause__) == "Connection error"


# Проверяет получение и возврат успешного ответа API.
@patch("src.loader.rq.get")
def test_api_check_success(mock_get):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.ok = True
    mock_response.json.return_value = {"features": []}
    mock_get.return_value = mock_response

    result = loader.api_check("http://test-api.com")

    assert result == {"features": []}
    mock_get.assert_called_once_with("http://test-api.com", timeout=5)


# Проверяет обработку HTTP-ошибки клиента без повторных запросов.
@patch("src.loader.rq.get")
def test_api_check_client_error(mock_get):
    mock_response = Mock()
    mock_response.status_code = 404
    mock_response.ok = False
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        "Not Found"
    )
    mock_get.return_value = mock_response

    with pytest.raises(requests.exceptions.HTTPError, match="Not Found"):
        loader.api_check("http://test-api.com")

    # Клиентская ошибка 4xx не должна запускать повторные запросы.
    mock_get.assert_called_once()


# Проверяет загрузку JSON в RustFS и правильность содержимого.
@patch.object(loader, "client")
def test_load_to_rustfs_success(mock_client):
    data = {"features": [{"id": "earthquake_1"}]}

    result = loader.load_to_rustfs(
        bucket_name="earthquakes",
        data=data,
        run_id="test_run",
    )

    assert result == "test_run/data.json"
    mock_client.put_object.assert_called_once()

    args, kwargs = mock_client.put_object.call_args
    assert args[0] == "earthquakes"
    assert args[1] == "test_run/data.json"

    uploaded_bytes = args[2].getvalue()
    assert json.loads(uploaded_bytes.decode("utf-8")) == data
    assert kwargs["length"] == len(uploaded_bytes)
    assert kwargs["content_type"] == "application/json"


# Проверяет, что ошибка загрузки в RustFS передаётся вызывающему коду.
@patch.object(loader, "client")
def test_load_to_rustfs_error(mock_client):
    mock_client.put_object.side_effect = Exception("Upload error")

    with pytest.raises(Exception, match="Upload error"):
        loader.load_to_rustfs(
            "earthquakes",
            {"features": []},
            "test_run",
        )

    mock_client.put_object.assert_called_once()


# Проверяет успешное подключение к PostgreSQL.
def test_db_access_check_success():
    mock_engine = MagicMock()
    mock_connection = mock_engine.connect.return_value.__enter__.return_value

    result = loader.db_access_check(mock_engine)

    assert result is True
    mock_engine.connect.assert_called_once()
    mock_connection.execute.assert_called_once()


# Проверяет, что при ошибке подключения возвращается False.
def test_db_access_check_error():
    mock_engine = MagicMock()
    mock_engine.connect.side_effect = Exception("Database unavailable")

    result = loader.db_access_check(mock_engine)

