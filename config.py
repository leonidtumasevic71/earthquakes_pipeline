import os
from dotenv import load_dotenv

load_dotenv()

api_url = os.getenv('API_URL')

minio_access_key = os.getenv('MINIO_ACCESS_KEY')
minio_secret_key = os.getenv('MINIO_SECRET_KEY')