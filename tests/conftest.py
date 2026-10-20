"""Тестовые переменные окружения и общие фикстуры pytest."""

import copy
import os

import pytest


_TEST_ENV = {
    "API_URL": "https://example.invalid/earthquakes",
    "RUSTFS_ACCESS_KEY": "test-access-key",
    "RUSTFS_SECRET_KEY": "test-secret-key",
    "DB_CONN_STRING": "postgresql://test:test@localhost:5432/test_db",
}

# config.py проверяет переменные окружения при импорте.
for _name, _value in _TEST_ENV.items():
    if not os.environ.get(_name, "").strip():
        os.environ[_name] = _value


@pytest.fixture
def valid_feature():
    """Возвращает корректную запись землетрясения для тестов."""
    return {
        "type": "Feature",
        "id": "us-test-001",
        "properties": {
            "mag": 4.2,
            "time": 1700000000000,
            "updated": 1700000001000,
            "nst": 10,
            "rms": 0.5,
            "gap": 120,
            "dmin": 0.4,
            "place": "Test location",
        },
        "geometry": {
            "type": "Point",
            "coordinates": [30.5, 50.2, 10.0],
        },
    }


@pytest.fixture
def valid_response(valid_feature):
    """Возвращает корректный ответ API с одной записью."""
    return {
        "type": "FeatureCollection",
        "features": [copy.deepcopy(valid_feature)],
    }
