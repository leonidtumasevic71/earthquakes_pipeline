import os
from dotenv import load_dotenv

load_dotenv()

api_url = os.getenv('API_URL')

rustfs_access_key = os.getenv('RUSTFS_ACCESS_KEY')
rustfs_secret_key = os.getenv('RUSTFS_SECRET_KEY')

db_conn_string = os.getenv('DB_CONN_STRING')