import os
import sys
import json
import tempfile
from datetime import datetime
from typing import Dict, List, Any

import pandas as pd
import httpx
from prefect import flow, task
from sqlalchemy.orm import sessionmaker

# Add shared directory to path
sys.path.append('/app/shared')
from database import BedOccupancy, create_database_connection
# --- FIX: Import your direct MinIO client functions ---
from storage import create_minio_client, ensure_bucket_exists, upload_file

# Initialize connections
engine, SessionLocal = create_database_connection()
# --- FIX: Use your direct MinIO client ---
minio_client = create_minio_client()
BUCKET_NAME = "prefect-results"

# Ensure the bucket exists using your storage function
ensure_bucket_exists(minio_client, BUCKET_NAME)

@task
def download_data_task(url: str) -> str:
    """Download data from MinIO storage and return the temporary file path."""
    # Now ALL data comes from MinIO - simplified logic!
    if not url.startswith("minio://"):
        raise ValueError(f"Expected MinIO URL, got: {url}")
    
    print(f"Downloading from MinIO: {url}")
    
    # Parse MinIO URL: minio://bucket/object_path
    url_parts = url.replace("minio://", "").split("/", 1)
    bucket_name = url_parts[0]
    object_name = url_parts[1]
    
    # Download from MinIO
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        try:
            minio_client.fget_object(bucket_name, object_name, tmp.name)
            print(f"SUCCESS: Downloaded {object_name} from MinIO bucket {bucket_name}")
            return tmp.name
        except Exception as e:
            print(f"ERROR: Failed to download from MinIO: {e}")
            raise

@task
def process_bed_occupancy_data_task(file_path: str) -> List[Dict[str, Any]]:
    """
    Process the downloaded NHS bed occupancy Excel file.
    """
    # (No changes to the inside of this task)
    print(f"Processing Excel file at: {file_path}")
    
    try:
        df = pd.read_excel(file_path, sheet_name='NHS Trust by Sector', header=[13, 14])
    except ValueError:
        df = pd.read_excel(file_path, sheet_name=0, header=[13, 14])

    df.columns = ['_'.join(col).strip().lower().replace('unnamed: ', 'col') for col in df.columns.values]

    column_keywords = {
        'organisation_code': 'org code',
        'organisation_name': 'org name',
        'beds_available': 'available_total',
        'beds_occupied': 'occupied_total'
    }
    
    column_mapping = {}
    for standard_name, keyword in column_keywords.items():
        matching_cols = [col for col in df.columns if keyword in col]
        if matching_cols:
            column_mapping[standard_name] = matching_cols[0]

    if not all(key in column_mapping for key in column_keywords):
        raise ValueError("Could not find all required columns.")

    processed_df = pd.DataFrame()
    for standard_name, actual_col in column_mapping.items():
        processed_df[standard_name] = df[actual_col]

    numeric_cols = ['beds_available', 'beds_occupied']
    for col in numeric_cols:
        processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce')
    
    processed_df.dropna(subset=numeric_cols, inplace=True)
    
    processed_df['occupancy_rate'] = 0.0
    processed_df.loc[processed_df['beds_available'] > 0, 'occupancy_rate'] = (processed_df['beds_occupied'] / processed_df['beds_available']) * 100
    
    processed_df['quarter'] = "Q4"
    processed_df['year'] = 2024
    
    return processed_df.to_dict('records')

@task
def store_bed_occupancy_data_task(data: List[Dict[str, Any]]):
    """Store the processed bed occupancy data in the database."""
    # (No changes to the inside of this task)
    if not data:
        return

    db = SessionLocal()
    try:
        db.query(BedOccupancy).delete()
        for record in data:
            db_record = BedOccupancy(**record)
            db.add(db_record)
        db.commit()
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()


# --- FIX: Remove result_storage and persist_result from the flow decorator ---
@flow
def nhs_bed_occupancy_pipeline_flow(url: str, is_upload: bool = False):
    """The main flow to download, process, and store NHS bed occupancy data."""
    # Download file (handles both HTTP URLs and MinIO URLs)
    file_path = download_data_task(url)
    
    try:
        processed_data = process_bed_occupancy_data_task(file_path)
        store_bed_occupancy_data_task(processed_data)
        
        # Manually store a JSON artifact in MinIO
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=".json") as tmp:
            json.dump({
                "status": "success", 
                "records_processed": len(processed_data),
                "source": "upload" if is_upload else "url",
                "source_location": url
            }, tmp)
            tmp.flush()
            upload_file(minio_client, BUCKET_NAME, f"pipeline_run_{datetime.now().isoformat()}.json", tmp.name)
    finally:
        # Always clean up the downloaded/temp file
        os.remove(file_path)

@task
def get_bed_occupancy_data_task() -> List[Dict[str, Any]]:
    """Get all bed occupancy data from the database."""
    # (No changes to the inside of this task)
    db = SessionLocal()
    try:
        data = db.query(BedOccupancy).all()
        if not data:
            return {"message": "No data found."}
        return [
            {
                "organisation_name": d.organisation_name,
                "occupancy_rate": d.occupancy_rate,
                "quarter": d.quarter,
                "year": d.year,
            }
            for d in data
        ]
    finally:
        db.close()


# --- FIX: Remove result_storage and persist_result from the flow decorator ---
@flow
def get_bed_occupancy_data_flow() -> List[Dict[str, Any]]:
    """Flow to get bed occupancy data for the API."""
    return get_bed_occupancy_data_task()