import logging as log
from sqlalchemy import create_engine
from config import api_url, db_conn_string
from src.loader import bucket_check, api_check, load_to_minio, check_db_and_table, load_to_postgres
from src.validation import data_extraction, response_structure_check, remove_duplicates, remove_nulls, response_values_check


log.basicConfig(
    filename="app.log",
    level=log.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

if __name__ == "__main__":
    engine = create_engine(db_conn_string)

    #проверки
    bucket_check("earthquakes")
    check_db_and_table(engine, "earthquakes")

    raw_data = api_check(api_url)
    load_to_minio("earthquakes", raw_data)
    extracted_data = data_extraction("earthquakes")



    # проверки
    response_structure_check(extracted_data)
    response_values_check(extracted_data)

    processed_data = remove_nulls(extracted_data)
    clean_data = remove_duplicates(processed_data)


    load_to_postgres(engine, "earthquakes", clean_data)


# TODO описать тесты, докер файл, реад ми, визуализацию и тд.