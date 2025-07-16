import os
from prefect.client.orchestration import PrefectClient
from storage import create_minio_client, ensure_bucket_exists
from database import create_database_connection

PREFECT_API_URL = os.environ.get('PREFECT_API_URL', 'http://prefect-server:4200/api')
BUCKET_NAME = "uploaded-files"

_minio_client = None
_prefect_client = None
_session_local = None

def get_minio_client():
    global _minio_client
    if _minio_client is None:
        _minio_client = create_minio_client()
        ensure_bucket_exists(_minio_client, BUCKET_NAME)
    return _minio_client

def get_prefect_client():
    global _prefect_client
    if _prefect_client is None:
        _prefect_client = PrefectClient(api=PREFECT_API_URL)
    return _prefect_client

def get_session_local():
    global _session_local
    if _session_local is None:
        _, _session_local = create_database_connection()
    return _session_local