import os
from minio import Minio
from minio.error import S3Error

def create_minio_client():
    """Create and return a MinIO client instance."""
    return Minio(
        os.environ.get("MINIO_URL", "localhost:9000"),
        access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        secure=False
    )

def ensure_bucket_exists(client, bucket_name):
    """Ensure bucket exists, create if it doesn't."""
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

def upload_file(client, bucket_name, object_name, file_path):
    """Upload a file to MinIO."""
    try:
        client.fput_object(bucket_name, object_name, file_path)
        return True
    except S3Error:
        return False

def download_file(client, bucket_name, object_name, file_path):
    """Download a file from MinIO."""
    try:
        client.fget_object(bucket_name, object_name, file_path)
        return True
    except S3Error:
        return False

def list_objects(client, bucket_name, prefix=""):
    """List objects in a bucket."""
    try:
        objects = client.list_objects(bucket_name, prefix=prefix)
        return [obj.object_name for obj in objects]
    except S3Error:
        return []

def delete_object(client, bucket_name, object_name):
    """Delete an object from MinIO."""
    try:
        client.remove_object(bucket_name, object_name)
        return True
    except S3Error:
        return False