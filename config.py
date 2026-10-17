import os
from dotenv import load_dotenv

load_dotenv()

REQUIRED_ENV_VARS = [
    "API_URL",
    "RUSTFS_ACCESS_KEY",
    "RUSTFS_SECRET_KEY",
    "DB_CONN_STRING",
]

missing_vars = [
    var for var in REQUIRED_ENV_VARS
    if not os.getenv(var) or not os.getenv(var).strip()
]

if missing_vars:
    raise ValueError(
        f"Отсутствуют обязательные переменные окружения: "
        f"{', '.join(missing_vars)}"
    )

api_url = os.getenv("API_URL")
rustfs_access_key = os.getenv("RUSTFS_ACCESS_KEY")
rustfs_secret_key = os.getenv("RUSTFS_SECRET_KEY")
db_conn_string = os.getenv("DB_CONN_STRING")