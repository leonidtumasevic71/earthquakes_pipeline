import logging as log
from config import api_url
from loader import bucket_check, api_check, load_to_minio
from validation import data_extraction


log.basicConfig(
    filename="app.log",
    level=log.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

if __name__ == "__main__":
    bucket_check("weather")
    res = api_check(api_url)
    load_to_minio("weather", res)
    data_extraction("weather")


# TODO описать тесты, докер файл, реад ми, и тд.