import pendulum
from datetime import timedelta
from sqlalchemy import create_engine
from airflow.sdk import DAG, task, get_current_context
from config import api_url, db_conn_string
from src.loader import (
    bucket_check,
    api_check,
    load_to_rustfs,
    db_access_check,
    check_db_table,
    load_to_postgres,
)
from src.validation import (
    data_extraction,
    filter_valid_features,
    remove_duplicates,
    remove_nulls,
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
    schedule=timedelta(minutes=30),
        start_date=pendulum.datetime(
            2026,
            9,
            28,
            tz="UTC"
        ),
    catchup=False,
    tags=["earthquakes"],
) as dag:

    @task
    def storages_check_task():
        bucket_check("earthquakes")

        engine = create_engine(db_conn_string)

        try:
            if not db_access_check(engine):
                raise ConnectionError(
                    "Не удалось подключиться к PostgreSQL"
                )

            if not check_db_table(engine, "earthquakes"):
                raise ValueError(
                    "Таблица 'earthquakes' не существует"
                )

        finally:
            engine.dispose()

    @task
    def get_api_response_and_load_to_rustfs_task():
        response = api_check(api_url)

        # Получаем ID текущего запуска DAG
        context = get_current_context()
        run_id = context["run_id"]

        return load_to_rustfs("earthquakes", response, run_id)


    @task
    def make_processed_data_task(raw_object_name: str) -> str:
        """Читает RAW, валидирует, очищает и сохраняет обработанные данные"""

        data = data_extraction("earthquakes", raw_object_name)

        # кривой корневой ответ прерывает таску
        # а некорректные отдельные записи пропускаются
        valid_features = filter_valid_features(data)
        if data["features"] and not valid_features:
            raise ValueError("Все записи не прошли валидацию, пустой набор не загружается")

        # Сохраняем существующие этапы очистки и удаления дубликатов
        cleaned_data = remove_nulls({"features": valid_features})
        unique_data = remove_duplicates(cleaned_data)

        run_id = get_current_context()["run_id"]

        processed_object_name = load_to_rustfs(
            "earthquakes",
            unique_data,
            f"{run_id}/processed",
        )

        return processed_object_name


    @task
    def load_clean_data_to_postgres_task(processed_object_name: str):
        # берём обработанные данные из RustFS
        processed_data = data_extraction(
            "earthquakes",
            processed_object_name
        )

        engine = create_engine(db_conn_string)

        try:
            return load_to_postgres(
                engine,
                "earthquakes",
                processed_data
            )
        finally:
            engine.dispose()


    storages_check = storages_check_task()
    raw_object_name = get_api_response_and_load_to_rustfs_task()
    processed_object_name = make_processed_data_task(raw_object_name)
    loaded = load_clean_data_to_postgres_task(processed_object_name)

    storages_check >> raw_object_name