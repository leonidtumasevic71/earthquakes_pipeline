import copy
import json
from unittest.mock import Mock, patch
import pytest
import src.validation


# Проверяет, что корректная структура FeatureCollection принимается.
def test_response_root_check_accepts_feature_collection(valid_response):
    assert src.validation.response_root_check(valid_response) is True


# Проверяет, что некорректные варианты корневой структуры отклоняются.
@pytest.mark.parametrize(
    "data",
    [
        None,
        [],
        {},
        {"type": "Feature", "features": []},
        {"type": "FeatureCollection", "features": {}},
    ],
)
def test_response_root_check_rejects_invalid_root(data):
    assert src.validation.response_root_check(data) is False


# Проверяет, что конечные числовые значения распознаются как числа.
@pytest.mark.parametrize("value", [0, 1, -2, 1.5])
def test_is_number_accepts_finite_numbers(value):
    assert src.validation.is_number(value) is True


# Проверяет отклонение строк, None, bool, NaN и бесконечностей.
@pytest.mark.parametrize(
    "value",
    ["1", None, True, False, float("nan"), float("inf"), -float("inf")],
)
def test_is_number_rejects_invalid_values(value):
    assert src.validation.is_number(value) is False


# Проверяет, что корректный ответ проходит проверку структуры.
def test_response_structure_check_accepts_valid_response(valid_response):
    assert src.validation.response_structure_check(valid_response) is True


# Проверяет, что логическое значение не принимается как магнитуда.
def test_response_structure_check_rejects_boolean_magnitude(valid_response):
    # bool is a subclass of int, but it is not a valid magnitude.
    valid_response["features"][0]["properties"]["mag"] = True

    assert src.validation.response_structure_check(valid_response) is False


# Проверяет, что недопустимые значения свойств отклоняются.
@pytest.mark.parametrize(
    ("property_name", "bad_value"),
    [
        ("mag", 16),
        ("time", -1),
        ("updated", -1),
        ("nst", -1),
        ("rms", -1),
        ("gap", -1),
        ("dmin", -1),
    ],
)
def test_response_values_check_rejects_invalid_properties(
    valid_response, property_name, bad_value
):
    valid_response["features"][0]["properties"][property_name] = bad_value

    assert src.validation.response_values_check(valid_response, logging_success=False) is False


# Проверяет отклонение координат за допустимыми границами.
@pytest.mark.parametrize(
    "coordinates",
    [
        [181, 0, 5],   # longitude is outside [-180, 180]
        [0, 91, 5],    # latitude is outside [-90, 90]
        [0, 0, -11],   # depth is below -10 km
    ],
)
def test_response_values_check_rejects_invalid_coordinates(
    valid_response, coordinates
):
    valid_response["features"][0]["geometry"]["coordinates"] = coordinates

    assert src.validation.response_values_check(valid_response, logging_success=False) is False


# Проверяет, что фильтр оставляет только корректные записи.
def test_filter_valid_features_keeps_only_valid_records(valid_feature):
    invalid_feature = copy.deepcopy(valid_feature)
    invalid_feature["id"] = "invalid-feature"
    invalid_feature["geometry"]["coordinates"] = [1, 2]
    data = {
        "type": "FeatureCollection",
        "features": [valid_feature, invalid_feature],
    }

    result = src.validation.filter_valid_features(data)

    assert result == [valid_feature]


# Проверяет удаление записей без обязательных данных.
def test_remove_nulls_drops_records_without_required_values(valid_feature):
    missing_id = copy.deepcopy(valid_feature)
    missing_id.pop("id")

    missing_time = copy.deepcopy(valid_feature)
    missing_time["properties"]["time"] = None

    short_coordinates = copy.deepcopy(valid_feature)
    short_coordinates["geometry"]["coordinates"] = [1, 2]

    result = src.validation.remove_nulls(
        {
            "features": [
                valid_feature,
                missing_id,
                missing_time,
                short_coordinates,
            ]
        }
    )

    assert result == [valid_feature]


# Проверяет удаление дубликатов по ID с сохранением первой записи.
def test_remove_duplicates_keeps_first_record_for_each_id(valid_feature):
    duplicate = copy.deepcopy(valid_feature)
    duplicate["properties"]["mag"] = 9.0

    second_feature = copy.deepcopy(valid_feature)
    second_feature["id"] = "us-test-002"

    result = src.validation.remove_duplicates(
        [valid_feature, duplicate, second_feature]
    )

    assert result == [valid_feature, second_feature]


# Проверяет загрузку объекта из RustFS и декодирование JSON.
@patch("src.validation.client")
def test_data_extraction_returns_decoded_json(mock_client):
    response = Mock()
    response.read.return_value = json.dumps({"status": "ok"}).encode("utf-8")
    mock_client.get_object.return_value = response

    result = src.validation.data_extraction("raw", "events/test.json")

    assert result == {"status": "ok"}
    mock_client.get_object.assert_called_once_with("raw", "events/test.json")
    response.close.assert_called_once()
    response.release_conn.assert_called_once()


# Проверяет, что ошибка RustFS передаётся вызывающему коду.
@patch("src.validation.client")
def test_data_extraction_propagates_storage_error(mock_client):
    mock_client.get_object.side_effect = RuntimeError("storage unavailable")

    with pytest.raises(RuntimeError, match="storage unavailable"):
        src.validation.data_extraction("raw", "events/test.json")
