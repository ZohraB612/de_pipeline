from fastapi import FastAPI, HTTPException, UploadFile, File
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

# --- Global Configuration & Client ---
PREFECT_API_URL = os.environ.get('PREFECT_API_URL', 'http://prefect-server:4200/api')
prefect_client = PrefectClient(api=PREFECT_API_URL)


# --- API Endpoints ---
@app.post("/nhs-data/trigger-pipeline")
async def trigger_nhs_pipeline(trigger: PipelineTrigger):
    """
    Triggers the NHS data processing pipeline and returns immediately with flow run details.
    """
    print("--- Endpoint /nhs-data/trigger-pipeline HIT ---")
    try:
        # NOTE: The deployment name is now just the flow name, as defined in worker.py
        deployment = await prefect_client.read_deployment_by_name("Data Pipeline Flow")
        
        flow_run = await prefect_client.create_flow_run_from_deployment(
            deployment_id=deployment.id,
            parameters={"url": trigger.url, "is_upload": False}
        )
        
        print(f"SUCCESS: Created flow run '{flow_run.name}' (ID: {flow_run.id})")
        return {
            "message": "Pipeline started successfully",
            "flow_run_id": str(flow_run.id),
            "flow_run_name": flow_run.name,
            "status_url": f"/nhs-data/status/{flow_run.id}"
        }
    except ObjectNotFound:
         raise HTTPException(status_code=404, detail="Deployment 'Data Pipeline Flow' not found. Please wait for the worker to initialize.")
    except Exception as e:
        print(f"ERROR: Could not trigger pipeline. Reason: {e}")
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
        deployment = await prefect_client.read_deployment_by_name("Data Pipeline Flow")
        
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
