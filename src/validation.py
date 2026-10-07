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


def data_extraction(
    bucket_name: str,
    object_name: str
) -> dict:
    """чтение данных из бакета"""

    try:
        response = client.get_object(
            bucket_name,
            object_name
        )

        try:
            data = json.loads(
                response.read().decode("utf-8")
            )

        finally:
            response.close()
            response.release_conn()

        logger.info(
            f"файл {object_name} успешно прочитан "
            f"из бакета {bucket_name}"
        )

        return data

    except Exception as e:
        log.error(
            f"ошибка чтения {object_name}: {e}"
        )
        raise


def response_structure_check(data: dict) -> bool:
    """Проверка структуры API ответа и типов данных."""

    if not isinstance(data, dict):
        return False

    # Корневой объект
    if data.get("type") != "FeatureCollection":
        return False

    if not isinstance(data.get("features"), list):
        return False

    # Проверка каждого объекта
    for feature in data["features"]:

        if not isinstance(feature, dict):
            return False

        # Обязательные поля Feature
        if not isinstance(feature.get("id"), str):
            return False

        if feature.get("type") != "Feature":
            return False

        if not isinstance(feature.get("properties"), dict):
            return False

        if not isinstance(feature.get("geometry"), dict):
            return False

        properties = feature["properties"]
        geometry = feature["geometry"]

        # Проверка properties
        if not isinstance(properties.get("mag"), (int, float)):
            return False

        if not isinstance(properties.get("time"), (int, float)):
            return False

        if not isinstance(properties.get("updated"), (int, float)):
            return False

        if properties.get("nst") is not None and not isinstance(properties["nst"], (int, float)):
            return False

        if properties.get("rms") is not None and not isinstance(properties["rms"], (int, float)):
            return False

        if properties.get("gap") is not None and not isinstance(properties["gap"], (int, float)):
            return False

        if properties.get("dmin") is not None and not isinstance(properties["dmin"], (int, float)):
            return False

        # Проверка geometry
        if geometry.get("type") != "Point":
            return False

        coordinates = geometry.get("coordinates")

        if not isinstance(coordinates, list):
            return False

        if len(coordinates) != 3:
            return False

        if not all(isinstance(value, (int, float)) for value in coordinates):
            return False

    return True


def response_values_check(data: dict) -> bool:
    """проверяет диапазоны значений"""

    for feature in data["features"]:
        feature_id = feature["id"]
        properties = feature["properties"]
        coordinates = feature["geometry"]["coordinates"]

        if properties["mag"] < -10 or properties["mag"] > 15:
            log.error(
                f"id={feature_id}: "
                f"mag={properties['mag']} вне диапазона [-10, 15]"
            )
            return False

        if properties["time"] < 0:
            log.error(
                f"id={feature_id}: "
                f"time={properties['time']} < 0"
            )
            return False

        if properties["updated"] < 0:
            log.error(
                f"id={feature_id}: "
                f"updated={properties['updated']} < 0"
            )
            return False

        if properties["nst"] is not None and properties["nst"] < 0:
            log.error(
                f"id={feature_id}: "
                f"nst={properties['nst']} < 0"
            )
            return False

        if properties["rms"] is not None and properties["rms"] < 0:
            log.error(
                f"id={feature_id}: "
                f"rms={properties['rms']} < 0"
            )
            return False

        if properties["gap"] is not None and properties["gap"] < 0:
            log.error(
                f"id={feature_id}: "
                f"gap={properties['gap']} < 0"
            )
            return False

        if properties["dmin"] is not None and properties["dmin"] < 0:
            log.error(
                f"id={feature_id}: "
                f"dmin={properties['dmin']} < 0"
            )
            return False

        longitude = coordinates[0]
        latitude = coordinates[1]
        depth = coordinates[2]

        if longitude < -180 or longitude > 180:
            log.error(
                f"id={feature_id}: "
                f"longitude={longitude} вне диапазона [-180, 180]"
            )
            return False

        if latitude < -90 or latitude > 90:
            log.error(
                f"id={feature_id}: "
                f"latitude={latitude} вне диапазона [-90, 90]"
            )
            return False

        if depth < -10:
            log.error(
                f"id={feature_id}: "
                f"depth={depth} < -10"
            )
            return False

    log.info(
        f"Проверка диапазонов успешно пройдена. "
        f"Проверено записей: {len(data['features'])}"
    )

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