from datetime import datetime, timedelta
from sqlalchemy import create_engine

from airflow.sdk import DAG, task

from config import api_url, db_conn_string
from src.loader import (
    bucket_check as _bucket_check,
    api_check,
    load_to_minio,
    check_db_and_table as _check_db_and_table,
    load_to_postgres,
)
from src.validation import (
    data_extraction,
    response_structure_check,
    remove_duplicates,
    remove_nulls,
    response_values_check,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="pipeline_dag",
    default_args=default_args,
    description="Earthquakes data pipeline",
    schedule=timedelta(minutes=5),
    start_date=datetime(2026, 9, 28),
    catchup=False,
    tags=["earthquakes"],
) as dag:

    @task
    def check_bucket_task():
        return _bucket_check("earthquakes")

    @task
    def check_db_task(): # стоит разбить функцию из этой таски на 2 отдельных(таблица / БД)
        engine = create_engine(db_conn_string)
        result = _check_db_and_table(engine, "earthquakes")
        if not result:
            raise RuntimeError("ошибка проверки БД или таблицы")
        return

    @task
    def fetch_api_task():
        return api_check(api_url)

    @task
    def upload_raw_to_minio_task(raw_data):
        return load_to_minio("earthquakes", raw_data)

    @task
    def extract_task():
        return data_extraction("earthquakes")

    @task
    def check_structure_task(data):
        valid_response = response_structure_check(data)

        if not valid_response:
            raise ValueError("валидация структуры не пройдена!")
        return data

    @task
    def check_values_task(data):
        valid_response = response_values_check(data)
        if not valid_response:
            raise ValueError("проверка диапазонов значений не пройдена!")
        return data

    @task
    def clean_nulls_task(data):
        return remove_nulls(data)

    @task
    def clean_duplicates_task(data):
        return remove_duplicates(data)

    @task
    def load_to_postgres_task(clean_data):
        engine = create_engine(db_conn_string)
        return load_to_postgres(engine, "earthquakes", clean_data)

    # Связываем задачи в граф
    bucket = check_bucket_task()
    db = check_db_task()
    raw = fetch_api_task()
    minio = upload_raw_to_minio_task(raw)
    extracted = extract_task()

    struct = check_structure_task(extracted)
    values = check_values_task(struct)

    no_nulls = clean_nulls_task(values)          # чистим после валидации
    clean = clean_duplicates_task(no_nulls)
    loaded = load_to_postgres_task(clean)

    # Порядок выполнения
    bucket >> db >> raw >> minio >> extracted
    extracted >> struct >> values
    values >> no_nulls >> clean >> loaded