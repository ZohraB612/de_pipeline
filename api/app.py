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
FLOW_RUN_TIMEOUT = int(os.environ.get('FLOW_RUN_TIMEOUT', 180))
prefect_client = PrefectClient(api=PREFECT_API_URL)


# --- Core Prefect Flow Runner for getting data ---
async def run_and_wait_for_flow(deployment_identifier: str, parameters: dict = None) -> dict:
    """
    Finds a deployment, runs it, and waits for the final result.
    """
    print(f"--- Starting run_and_wait_for_flow for '{deployment_identifier}' ---")
    try:
        deployment = await prefect_client.read_deployment_by_name(deployment_identifier)
        print(f"SUCCESS: Found deployment '{deployment_identifier}'.")

        flow_run = await prefect_client.create_flow_run_from_deployment(
            deployment_id=deployment.id,
            parameters=parameters or {}
        )
        print(f"SUCCESS: Created flow run '{flow_run.name}' (ID: {flow_run.id}).")
        print("POLLING: Waiting for flow run to complete...")

        attempts = 0
        while attempts < FLOW_RUN_TIMEOUT:
            await asyncio.sleep(5)
            flow_run_result = await prefect_client.read_flow_run(flow_run.id)
            state = flow_run_result.state
            print(f"POLLING: Current state of '{flow_run.name}' is '{state.name if state else 'Unknown'}'.")

            if not state:
                attempts += 5
                continue

            if state.is_completed():
                print(f"SUCCESS: Flow run '{flow_run.name}' completed.")
                # --- FIX: Remove 'await' since fetch=False returns a synchronous result ---
                result_data = state.result(fetch=False)
                print(f"DEBUG: State result type: {type(result_data)}")
                print(f"DEBUG: State result value: {result_data}")
                return result_data
            elif state.is_failed() or state.is_crashed():
                error_message = state.message or "No specific error message."
                print(f"ERROR: Flow run '{flow_run.name}' failed. Reason: {error_message}")
                raise HTTPException(status_code=500, detail=f"Flow run failed: {error_message}")
            
            attempts += 5

        print(f"ERROR: Flow run '{flow_run.name}' timed out.")
        raise HTTPException(status_code=408, detail="Flow run timed out.")

    except ObjectNotFound:
        print(f"ERROR: Deployment '{deployment_identifier}' not found.")
        raise HTTPException(status_code=404, detail=f"Deployment '{deployment_identifier}' not found.")
    except Exception as e:
        print(f"CRITICAL ERROR in run_and_wait_for_flow: {e}")
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")


# --- API Endpoints ---
# (No changes needed for the endpoints)
@app.post("/nhs-data/trigger-pipeline")
async def trigger_nhs_pipeline(trigger: PipelineTrigger):
    """
    Triggers the NHS data processing pipeline and returns immediately with flow run details.
    """
    print("--- Endpoint /nhs-data/trigger-pipeline HIT ---")
    try:
        deployment = await prefect_client.read_deployment_by_name("nhs-bed-occupancy-pipeline-flow/nhs-bed-occupancy-pipeline-deployment")
        
        flow_run = await prefect_client.create_flow_run_from_deployment(
            deployment_id=deployment.id,
            parameters={"url": trigger.url}
        )
        
        print(f"SUCCESS: Created flow run '{flow_run.name}' (ID: {flow_run.id})")
        return {
            "message": "Pipeline started successfully",
            "flow_run_id": str(flow_run.id),
            "flow_run_name": flow_run.name,
            "status_url": f"/nhs-data/status/{flow_run.id}"
        }
    except ObjectNotFound:
         raise HTTPException(status_code=404, detail="Deployment 'nhs-bed-occupancy-pipeline-deployment' not found. Please wait for the worker to initialize.")
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
            "flow_run__id": flow_run_id,
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
async def get_occupancy_data():
    """Retrieves all processed bed occupancy data from the database."""
    print("--- Endpoint /nhs-data/occupancy HIT ---")
    return await run_and_wait_for_flow("get-bed-occupancy-data-flow/get-bed-occupancy-data-deployment")


@app.post("/nhs-data/upload")
async def upload_nhs_file(file: UploadFile = File(...)):
    """Upload an NHS Excel file to MinIO and trigger processing pipeline."""
    print(f"--- Endpoint /nhs-data/upload HIT with file: {file.filename} ---")
    
    # Validate file type
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Only Excel files (.xlsx, .xls) are supported")
    
    try:
        # Create temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{file.filename}") as tmp_file:
            content = await file.read()
            tmp_file.write(content)
            tmp_file.flush()
            
            # Upload to MinIO
            object_name = f"nhs_uploads/{file.filename}"
            upload_file(minio_client, BUCKET_NAME, object_name, tmp_file.name)
            
            print(f"SUCCESS: File uploaded to MinIO as {object_name}")
            
            # Trigger processing pipeline with local file path
            deployment = await prefect_client.read_deployment_by_name("nhs-bed-occupancy-pipeline-flow/nhs-bed-occupancy-pipeline-deployment")
            
            flow_run = await prefect_client.create_flow_run_from_deployment(
                deployment_id=deployment.id,
                parameters={"file_path": tmp_file.name, "is_upload": True}
            )
            
            print(f"SUCCESS: Created flow run '{flow_run.name}' (ID: {flow_run.id})")
            
            # Clean up temp file
            os.unlink(tmp_file.name)
            
            return {
                "message": "File uploaded and pipeline started successfully",
                "filename": file.filename,
                "minio_path": object_name,
                "flow_run_id": str(flow_run.id),
                "flow_run_name": flow_run.name,
                "status_url": f"/nhs-data/status/{flow_run.id}"
            }
            
    except ObjectNotFound:
        raise HTTPException(status_code=404, detail="Deployment 'nhs-bed-occupancy-pipeline-deployment' not found. Please wait for the worker to initialize.")
    except Exception as e:
        print(f"ERROR: Could not upload file and trigger pipeline. Reason: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/nhs-data/sample")
async def get_sample_data():
    """Get sample data (column names and first 10 rows) from the database."""
    print("--- Endpoint /nhs-data/sample HIT ---")
    
    db = SessionLocal()
    try:
        # Get first 10 records
        sample_records = db.query(BedOccupancy).limit(10).all()
        
        if not sample_records:
            return {"message": "No data found in database", "data": [], "columns": []}
        
        # Convert to dict format
        data = []
        for record in sample_records:
            data.append({
                "id": record.id if hasattr(record, 'id') else None,
                "organisation_code": record.organisation_code,
                "organisation_name": record.organisation_name,
                "beds_available": record.beds_available,
                "beds_occupied": record.beds_occupied,
                "occupancy_rate": record.occupancy_rate,
                "quarter": record.quarter,
                "year": record.year
            })
        
        # Get column information
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


@app.get("/nhs-data/organization/{org_code}")
async def get_data_by_organization_code(org_code: str):
    """Get data for a specific organization by organization code."""
    print(f"--- Endpoint /nhs-data/organization/{org_code} HIT ---")
    
    db = SessionLocal()
    try:
        # Query by organization code
        records = db.query(BedOccupancy).filter(BedOccupancy.organisation_code == org_code).all()
        
        if not records:
            raise HTTPException(status_code=404, detail=f"No data found for organization code: {org_code}")
        
        # Convert to dict format
        data = []
        for record in records:
            data.append({
                "id": record.id if hasattr(record, 'id') else None,
                "organisation_code": record.organisation_code,
                "organisation_name": record.organisation_name,
                "beds_available": record.beds_available,
                "beds_occupied": record.beds_occupied,
                "occupancy_rate": record.occupancy_rate,
                "quarter": record.quarter,
                "year": record.year
            })
        
        return {
            "organisation_code": org_code,
            "organisation_name": data[0]["organisation_name"] if data else None,
            "total_records": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"ERROR: Could not retrieve data for organization {org_code}. Reason: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.get("/nhs-data/organization-name/{org_name}")
async def get_data_by_organization_name(org_name: str):
    """Get data for a specific organization by organization name (partial match)."""
    print(f"--- Endpoint /nhs-data/organization-name/{org_name} HIT ---")
    
    db = SessionLocal()
    try:
        # Query by organization name (case-insensitive partial match)
        records = db.query(BedOccupancy).filter(
            BedOccupancy.organisation_name.ilike(f"%{org_name}%")
        ).all()
        
        if not records:
            raise HTTPException(status_code=404, detail=f"No data found for organization name containing: {org_name}")
        
        # Convert to dict format
        data = []
        for record in records:
            data.append({
                "id": record.id if hasattr(record, 'id') else None,
                "organisation_code": record.organisation_code,
                "organisation_name": record.organisation_name,
                "beds_available": record.beds_available,
                "beds_occupied": record.beds_occupied,
                "occupancy_rate": record.occupancy_rate,
                "quarter": record.quarter,
                "year": record.year
            })
        
        return {
            "search_term": org_name,
            "total_records": len(data),
            "organizations_found": len(set(record["organisation_name"] for record in data)),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"ERROR: Could not retrieve data for organization name {org_name}. Reason: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.get("/health")
async def health_check():
    """A simple health check endpoint to verify the service is running."""
    return {"status": "healthy"}