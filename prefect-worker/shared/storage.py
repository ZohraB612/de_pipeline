import os
from minio import Minio
from minio.error import S3Error

def create_minio_client():
    """Create and return a MinIO client instance"""
    minio_url = os.environ.get("MINIO_URL", "localhost:9000")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    
    client = Minio(
        minio_url,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False  # Set to True for HTTPS
    )
    
    return client

def ensure_bucket_exists(client, bucket_name):
    """Ensure that a bucket exists, create it if it doesn't"""
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Created bucket: {bucket_name}")
        else:
            print(f"Bucket {bucket_name} already exists")
    except S3Error as e:
        print(f"Error creating bucket {bucket_name}: {e}")
        raise

def upload_file(client, bucket_name, object_name, file_path):
    """Upload a file to MinIO"""
    try:
        client.fput_object(bucket_name, object_name, file_path)
        print(f"Uploaded {file_path} as {object_name} to {bucket_name}")
        return True
    except S3Error as e:
        print(f"Error uploading file: {e}")
        return False

def download_file(client, bucket_name, object_name, file_path):
    """Download a file from MinIO"""
    try:
        client.fget_object(bucket_name, object_name, file_path)
        print(f"Downloaded {object_name} from {bucket_name} to {file_path}")
        return True
    except S3Error as e:
        print(f"Error downloading file: {e}")
        return False

def list_objects(client, bucket_name, prefix=""):
    """List objects in a bucket"""
    try:
        objects = client.list_objects(bucket_name, prefix=prefix)
        return [obj.object_name for obj in objects]
    except S3Error as e:
        print(f"Error listing objects: {e}")
        return []

def delete_object(client, bucket_name, object_name):
    """Delete an object from MinIO"""
    try:
        client.remove_object(bucket_name, object_name)
        print(f"Deleted {object_name} from {bucket_name}")
        return True
    except S3Error as e:
        print(f"Error deleting object: {e}")
        return False