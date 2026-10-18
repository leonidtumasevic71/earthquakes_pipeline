import json
import logging
import requests as rq
from datetime import datetime, timezone
from io import BytesIO
from minio import Minio
from minio.error import S3Error
from sqlalchemy.engine import Engine
from sqlalchemy import text, inspect
from config import rustfs_access_key, rustfs_secret_key
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception,
)


logger = logging.getLogger(__name__)

client = Minio(
    "rustfs:9000",
    access_key=rustfs_access_key,
    secret_key=rustfs_secret_key,
    secure=False
)


def bucket_check(bucket_name: str) -> None:
    """Создаёт RustFS bucket, если он не существует."""

    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"Bucket '{bucket_name}' успешно создан")
        else:
            logger.info(f"Bucket '{bucket_name}' уже существует")

    except Exception as e:
        logger.exception(
            f"Ошибка при проверке или создании bucket '{bucket_name}'"
        )
        raise RuntimeError(
            f"Не удалось проверить или создать bucket '{bucket_name}'"
        ) from e


@retry(
    retry=retry_if_exception(
        lambda e: (
            isinstance(e, (rq.exceptions.Timeout, rq.exceptions.ConnectionError))
            or (
                isinstance(e, rq.exceptions.HTTPError)
                and e.response is not None
                and 500 <= e.response.status_code < 600
            )
        )
    ),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1.25, min=1, max=30),
)
def api_check(api_url: str) -> dict:
    """Проверка доступности API. Retry только для timeout, connection errors и HTTP 5xx."""

    response = rq.get(api_url, timeout=5)

    if response.status_code == 204:
        logging.error(f"отсутствие тела ответа")

    if response.ok:
        logging.info(f"API доступен, status_code: {response.status_code}")
        return response.json()

    if 400 <= response.status_code < 500:
        logging.error(f"Клиентская ошибка: {response.status_code}")
        response.raise_for_status()

    logging.error(f"Ошибка сервера API, status_code: {response.status_code}")
    response.raise_for_status()


def load_to_rustfs(
    bucket_name: str,
    data: dict | list,
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

        logging.info(
            f"файл {object_name}, успешно загружен "
            f"в бакет: {bucket_name}"
        )

        return object_name

    except S3Error as e:
        logging.error(
            f"ошибка rustfs при загрузке объекта: "
            f"{object_name}: {e}"
        )
        raise

    except Exception as e:
        logging.error(
            f"неизвестная ошибка при загрузке: {e}"
        )
        raise


def db_access_check(engine: Engine) -> bool:
    """Проверяет доступность PostgreSQL."""

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        logger.info("PostgreSQL доступен")
        return True

    except Exception:
        logger.exception("Не удалось подключиться к PostgreSQL")
        return False


def check_db_table(engine: Engine, table_name: str) -> bool:
    """Проверяет наличие таблицы в PostgreSQL."""

    try:
        table_exists = inspect(engine).has_table(table_name)

    except Exception:
        logger.exception(
            f"Ошибка при проверке таблицы '{table_name}'"
        )
        raise

    if not table_exists:
        logger.error(f"Таблица '{table_name}' не существует")
        return False

    logger.info(f"Таблица '{table_name}' существует")
    return True


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
                        "time": datetime.fromtimestamp(
                            time_ms / 1000,
                            tz=timezone.utc
                        ),
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