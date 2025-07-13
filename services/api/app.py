from fastapi import FastAPI, HTTPException, UploadFile, File, Request
from pydantic import BaseModel
import httpx
import os
import asyncio
import tempfile
import sys
from prefect.client.orchestration import PrefectClient
from prefect.exceptions import ObjectNotFound
from uuid import UUID
from typing import List, Dict, Any
from datetime import datetime # <-- Missing import added

# Add shared directory to path
sys.path.append('/app/shared')
from database import BedOccupancy, create_database_connection
from storage import create_minio_client, ensure_bucket_exists, upload_file

# Initialize database and storage connections
engine, SessionLocal = create_database_connection()
minio_client = create_minio_client()
BUCKET_NAME = "uploaded-files"
ensure_bucket_exists(minio_client, BUCKET_NAME)

# --- Pydantic Model ---
class PipelineTrigger(BaseModel):
    url: str

# --- FastAPI App Initialization ---
app = FastAPI()

# --- DEBUGGING MIDDLEWARE ---
# This will print the exact path of every request the API receives.
@app.middleware("http")
async def log_requests(request: Request, call_next):
    print(f"--> API receiving request for path: {request.url.path}")
    response = await call_next(request)
    return response

# --- Global Configuration & Client ---
PREFECT_API_URL = os.environ.get('PREFECT_API_URL', 'http://prefect-server:4200/api')
prefect_client = PrefectClient(api=PREFECT_API_URL)


# --- API Endpoints ---
@app.post("/nhs-data/trigger-pipeline")
async def trigger_nhs_pipeline(trigger: PipelineTrigger):
    """
    Downloads file from URL, saves to MinIO, then triggers pipeline with MinIO path.
    This ensures ALL data flows through MinIO storage consistently.
    """
    print("--- Endpoint /nhs-data/trigger-pipeline HIT ---")
    try:
        # Step 1: Download file from URL and save to MinIO
        print(f"Downloading file from URL: {trigger.url}")
        
        async with httpx.AsyncClient() as client:
            response = await client.get(trigger.url)
            response.raise_for_status()
            
            # Extract filename from URL
            url_filename = trigger.url.split('/')[-1] if '/' in trigger.url else 'downloaded_file.xlsx'
            if not url_filename.endswith(('.xlsx', '.xls')):
                url_filename += '.xlsx'
            
            # Create timestamped object name
            object_name = f"url_downloads/{datetime.now().isoformat()}_{url_filename}"
            
            # Save to temporary file then upload to MinIO
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
                tmp_file.write(response.content)
                tmp_file_path = tmp_file.name
            
            # Upload to MinIO
            upload_file(minio_client, BUCKET_NAME, object_name, tmp_file_path)
            minio_url = f"minio://{BUCKET_NAME}/{object_name}"
            print(f"SUCCESS: Downloaded and saved to MinIO as {object_name}")
            
            # Clean up temp file
            os.unlink(tmp_file_path)
        
        # Step 2: Trigger Prefect pipeline with MinIO URL (consistent with file uploads)
        deployment = await prefect_client.read_deployment_by_name("nhs-bed-occupancy-pipeline-flow/Data Pipeline Flow")
        
        flow_run = await prefect_client.create_flow_run_from_deployment(
            deployment_id=deployment.id,
            parameters={"url": minio_url, "is_upload": False}  # Now uses MinIO URL!
        )
        
        print(f"SUCCESS: Created flow run '{flow_run.name}' (ID: {flow_run.id})")
        return {
            "message": "File downloaded to MinIO and pipeline started successfully",
            "original_url": trigger.url,
            "minio_path": object_name,
            "flow_run_id": str(flow_run.id),
            "flow_run_name": flow_run.name,
            "status_url": f"/nhs-data/status/{flow_run.id}"
        }
        
    except httpx.HTTPError as e:
        print(f"ERROR: Failed to download from URL {trigger.url}: {e}")
        raise HTTPException(status_code=400, detail=f"Failed to download file from URL: {str(e)}")
    except ObjectNotFound:
         raise HTTPException(status_code=404, detail="Deployment 'nhs-bed-occupancy-pipeline-flow/Data Pipeline Flow' not found. Please wait for the worker to initialize.")
    except Exception as e:
        print(f"ERROR: Could not download file and trigger pipeline. Reason: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/nhs-data/status/{flow_run_id}")
async def get_flow_status(flow_run_id: str):
    """Check the status of a specific flow run."""
    print(f"--- Endpoint /nhs-data/status/{flow_run_id} HIT ---")
    try:
        flow_run = await prefect_client.read_flow_run(UUID(flow_run_id))
        return {
            "flow_run_id": flow_run_id,
            "name": flow_run.name,
            "state": flow_run.state.name if flow_run.state else "Unknown",
            "is_completed": flow_run.state.is_completed() if flow_run.state else False,
            "is_failed": flow_run.state.is_failed() if flow_run.state else False,
            "message": flow_run.state.message if flow_run.state else None,
            "created_at": flow_run.created.isoformat(),
            "updated_at": flow_run.updated.isoformat() if flow_run.updated else None
        }
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Flow run not found or invalid ID: {str(e)}")


@app.get("/nhs-data/occupancy")
async def get_occupancy_data() -> List[Dict[str, Any]]:
    """
    --- FIX: Retrieves all processed bed occupancy data directly from the database. ---
    This is more reliable and efficient than running a separate Prefect flow.
    """
    print("--- Endpoint /nhs-data/occupancy HIT ---")
    db = SessionLocal()
    try:
        data = db.query(BedOccupancy).all()
        if not data:
            # Return an empty list instead of raising an error, so the frontend can handle it gracefully.
            return []
        
        # Format the data to be returned by the API
        return [
            {
                "organisation_code": d.organisation_code,
                "organisation_name": d.organisation_name,
                "beds_available": d.beds_available,
                "beds_occupied": d.beds_occupied,
                "occupancy_rate": d.occupancy_rate,
                "quarter": d.quarter,
                "year": d.year,
            }
            for d in data
        ]
    except Exception as e:
        print(f"ERROR: Could not retrieve occupancy data from database. Reason: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    finally:
        db.close()


@app.post("/nhs-data/upload")
async def upload_nhs_file(file: UploadFile = File(...)):
    """Upload an NHS Excel file to MinIO and trigger processing pipeline."""
    print(f"--- Endpoint /nhs-data/upload HIT with file: {file.filename} ---")
    
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Only Excel files (.xlsx, .xls) are supported")
    
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{file.filename}") as tmp_file:
            content = await file.read()
            tmp_file.write(content)
            tmp_file_path = tmp_file.name

        # Upload to MinIO
        object_name = f"nhs_uploads/{datetime.now().isoformat()}_{file.filename}"
        upload_file(minio_client, BUCKET_NAME, object_name, tmp_file_path)
        minio_url = f"minio://{BUCKET_NAME}/{object_name}"
        print(f"SUCCESS: File uploaded to MinIO as {object_name}")
        
        # Trigger processing pipeline with MinIO URL
        deployment = await prefect_client.read_deployment_by_name("nhs-bed-occupancy-pipeline-flow/Data Pipeline Flow")
        
        flow_run = await prefect_client.create_flow_run_from_deployment(
            deployment_id=deployment.id,
            parameters={"url": minio_url, "is_upload": True}
        )
        
        print(f"SUCCESS: Created flow run '{flow_run.name}' (ID: {flow_run.id})")
        
        return {
            "message": "File uploaded and pipeline started successfully",
            "filename": file.filename,
            "minio_path": object_name,
            "flow_run_id": str(flow_run.id),
            "flow_run_name": flow_run.name,
            "status_url": f"/nhs-data/status/{flow_run.id}"
        }
            
    except ObjectNotFound:
        raise HTTPException(status_code=404, detail="Deployment 'Data Pipeline Flow' not found. Please wait for the worker to initialize.")
    except Exception as e:
        print(f"ERROR: Could not upload file and trigger pipeline. Reason: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # Clean up temp file
        if 'tmp_file_path' in locals() and os.path.exists(tmp_file_path):
            os.unlink(tmp_file_path)


@app.get("/nhs-data/sample")
async def get_sample_data():
    """Get sample data (column names and first 10 rows) from the database."""
    print("--- Endpoint /nhs-data/sample HIT ---")
    
    db = SessionLocal()
    try:
        sample_records = db.query(BedOccupancy).limit(10).all()
        
        if not sample_records:
            return {"message": "No data found in database", "data": [], "columns": []}
        
        data = [
            {
                "organisation_code": record.organisation_code,
                "organisation_name": record.organisation_name,
                "beds_available": record.beds_available,
                "beds_occupied": record.beds_occupied,
                "occupancy_rate": record.occupancy_rate,
                "quarter": record.quarter,
                "year": record.year
            } for record in sample_records
        ]
        
        columns = list(data[0].keys()) if data else []
        
        return {
            "message": f"Sample data retrieved successfully",
            "total_records": len(sample_records),
            "columns": columns,
            "data": data
        }
        
    except Exception as e:
        print(f"ERROR: Could not retrieve sample data. Reason: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.get("/health")
async def health_check():
    """A simple health check endpoint to verify the service is running."""
    return {"status": "healthy"}