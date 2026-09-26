import json
import logging as log
import requests as rq
from datetime import datetime
from io import BytesIO
from minio import Minio
from minio.error import S3Error
from tenacity import retry, stop_after_attempt, wait_exponential
from config import minio_access_key, minio_secret_key


logger = log.getLogger(__name__)

client = Minio(
    "localhost:9000",
    access_key=minio_access_key,
    secret_key=minio_secret_key,
    secure=False
)

def bucket_check(bucket_name: str) -> None:
    """создание minio bucket если bucket еще не был созан"""
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
    """проверка доступности api. Ретраит при 5ХХ и таймаутах"""
    response = rq.get(api_url, timeout=5)
    if response.ok:
        log.info(f"api доступен, status_code:{response.status_code}")
        return response.json()
    # при 4ХХ нет смысла ретраить
    if 500 <= response.status_code < 600:
        log.error(f"api НЕдоступен, status_code:{response.status_code}")
        response.raise_for_status()
        log.error(f"Клиентская ошибка: {response.status_code}")
    return response.json()


def load_to_minio(bucket_name: str, data: dict) -> str | None:
    now = datetime.now()
    object_name = f"{now:%Y/%m/%d}/data_{now:%H%M%S_%f}.json"

    json_bytes = json.dumps(data, ensure_ascii=False).encode("utf-8")
    try:
        client.put_object(
            bucket_name,
            object_name,
            BytesIO(json_bytes),
            length=len(json_bytes),
            content_type="application/json",
        )
        log.info(f"файл {object_name}, успешно загружен в бакет: {bucket_name}")
        return object_name
    except S3Error as e:
        log.error(f"ошибка Minio при загрузке объекта: {object_name}: {e}")
        return None
    except Exception as e:
        log.error(f"неизвестная ошибка при загрузке: {e}")
        return None



