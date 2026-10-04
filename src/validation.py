import json
import logging as log
from minio import Minio
from config import rustfs_access_key, rustfs_secret_key



logger = log.getLogger(__name__)

client = Minio(
    "rustfs:9000",
    access_key=rustfs_access_key,
    secret_key=rustfs_secret_key,
    secure=False
)

def data_extraction(bucket_name: str) -> dict:
    """чтение данных из бакета"""
    objects = client.list_objects(
        bucket_name,
        recursive=True
    )

    for obj in objects:

        if not obj.object_name.endswith(".json"):
            continue

        try:
            response = client.get_object(
                bucket_name,
                obj.object_name
            )
            data = json.loads(
                response.read().decode("utf-8")
            )

        except Exception as e:
            log.error(
                f"ошибка чтения {obj.object_name}: {e}"
            )
    return data


def response_structure_check(data: dict) -> bool:
    """проверка структуры api ответа и типов данных """
    if not isinstance(data, dict):
        return False

    if data.get("type") != "FeatureCollection":
        return False

    if not isinstance(data.get("features"), list):
        return False

    for feature in data["features"]:
        if not isinstance(feature, dict):
            return False

        if feature.get("type") != "Feature":
            return False

        if not isinstance(feature.get("properties"), dict):
            return False

        if not isinstance(feature.get("geometry"), dict):
            return False

    return True


def response_values_check(data: dict) -> bool:
    """проверяет диапазоны значений"""
    for feature in data["features"]:
        properties = feature["properties"]
        coordinates = feature["geometry"]["coordinates"]

        if properties["mag"] < -10 or properties["mag"] > 15:
            return False

        if properties["time"] < 0:
            return False

        if properties["updated"] < 0:
            return False

        if properties["nst"] is not None and properties["nst"] < 0:
            return False

        if properties["rms"] is not None and properties["rms"] < 0:
            return False

        if properties["gap"] is not None and properties["gap"] < 0:
            return False

        if properties["dmin"] is not None and properties["dmin"] < 0:
            return False

        longitude = coordinates[0]
        latitude = coordinates[1]
        depth = coordinates[2]

        if longitude < -180 or longitude > 180:
            return False

        if latitude < -90 or latitude > 90:
            return False

        if depth < 0:
            return False

    return True


def remove_nulls(data: dict) -> list:
    """Удаляет только записи с критически отсутствующими полями."""

    cleaned_features = []

    for feature in data.get("features", []):

        properties = feature.get("properties", {})
        geometry = feature.get("geometry", {})
        coordinates = geometry.get("coordinates", [])

        # обязательные поля
        if feature.get("id") is None:
            continue

        if properties.get("time") is None:
            continue

        if len(coordinates) < 3:
            continue

        cleaned_features.append(feature)

    return cleaned_features


def remove_duplicates(features: list) -> list:
    """фильтр дубликатов"""
    unique_features = []
    seen_ids = set()

    for feature in features:
        feature_id = feature["id"]

        if feature_id not in seen_ids:
            unique_features.append(feature)
            seen_ids.add(feature_id)

    return unique_features



