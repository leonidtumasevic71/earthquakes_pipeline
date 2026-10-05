import json
import logging as log
import requests as rq
from datetime import datetime
from io import BytesIO
from minio import Minio
from minio.error import S3Error
from tenacity import retry, stop_after_attempt, wait_exponential
from sqlalchemy import text, inspect
from config import rustfs_access_key, rustfs_secret_key


logger = log.getLogger(__name__)

client = Minio(
    "rustfs:9000",
    access_key=rustfs_access_key,
    secret_key=rustfs_secret_key,
    secure=False
)


def bucket_check(bucket_name: str) -> None:
    """создание rustfs bucket если bucket еще не был созан"""
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        log.info(f"бакет {bucket_name} был успешно создан")
    else:
        log.info(f"бакет: {bucket_name} уже существует")


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=30),
)
def api_check(api_url: str) -> dict:
    """проверка доступности api. Ретраит при 5ХХ и тайамаутах"""
    response = rq.get(api_url, timeout=5)
    if response.ok:
        log.info(f"api доступен, status_code:{response.status_code}")
        return response.json()

    # при 4ХХ нет смысла ретраить
    if 500 <= response.status_code < 600:
        log.error(f"api НЕдоступен, status_code:{response.status_code}")
        response.raise_for_status()

    log.error(f"Клиентская ошибка: {response.status_code}")
    response.raise_for_status()


def load_to_rustfs(
    bucket_name: str,
    data: dict,
    run_id: str
) -> str:
    """
    Загружает raw данные в RustFS.

    Object name строится на основе run_id,
    поэтому один запуск DAG соответствует одному raw-файлу.
    """

    object_name = f"{run_id}/data.json"

    json_bytes = json.dumps(
        data,
        ensure_ascii=False
    ).encode("utf-8")

    try:
        client.put_object(
            bucket_name,
            object_name,
            BytesIO(json_bytes),
            length=len(json_bytes),
            content_type="application/json",
        )

        log.info(
            f"файл {object_name}, успешно загружен "
            f"в бакет: {bucket_name}"
        )

        return object_name

    except S3Error as e:
        log.error(
            f"ошибка rustfs при загрузке объекта: "
            f"{object_name}: {e}"
        )
        raise

    except Exception as e:
        log.error(
            f"неизвестная ошибка при загрузке: {e}"
        )
        raise


def check_db_and_table(engine, table_name: str) -> bool:
    """Проверяет доступность БД и наличие таблицы. Engine передаётся снаружи."""
    try:
        # Проверка подключения
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        # Проверка наличия таблицы
        if inspect(engine).has_table(table_name):
            logger.info(f"Таблица '{table_name}' существует")
            return True

        logger.error(f"Таблицы '{table_name}' нет в БД")
        return False

    except Exception as e:
        logger.error(f"БД недоступна: {e}")
        return False


def load_to_postgres(engine, table_name: str, data) -> None:
    """Записывает землетрясения в PostgreSQL. data — dict с ключом features или list."""
    if isinstance(data, dict):
        features = data.get("features", [])
    elif isinstance(data, list):
        features = data
    else:
        raise TypeError(f"Ожидался dict или list, получено: {type(data)}")

    if not features:
        logger.info("Нет данных для записи в PostgreSQL")
        return

    try:
        with engine.begin() as conn:
            for feature in features:
                properties = feature.get("properties", {})
                geometry = feature.get("geometry", {})

                earthquake_id = feature.get("id")
                coordinates = geometry.get("coordinates", [])

                if len(coordinates) < 3:
                    logger.warning(
                        f"Некорректные координаты для earthquake_id={earthquake_id}"
                    )
                    continue

                time_ms = properties.get("time")
                if time_ms is None:
                    logger.warning(
                        f"Нет поля 'time' для earthquake_id={earthquake_id}"
                    )
                    continue

                longitude = coordinates[0]
                latitude = coordinates[1]
                depth = coordinates[2]

                query = text(f"""
                    INSERT INTO {table_name} (
                        id, time, latitude, longitude, depth, magnitude, place
                    )
                    VALUES (
                        :id, :time, :latitude, :longitude, :depth, :magnitude, :place
                    )
                    ON CONFLICT (id) DO NOTHING
                """)

                conn.execute(
                    query,
                    {
                        "id": earthquake_id,
                        "time": datetime.fromtimestamp(time_ms / 1000),
                        "latitude": latitude,
                        "longitude": longitude,
                        "depth": depth,
                        "magnitude": properties.get("mag"),
                        "place": properties.get("place"),
                    },
                )

        logger.info(f"Обработано землетрясений: {len(features)}")

    except Exception as e:
        logger.error(f"Ошибка при загрузке данных в PostgreSQL: {e}")
        raise