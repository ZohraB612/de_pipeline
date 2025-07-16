"""Pipeline management endpoints."""
from fastapi import APIRouter, HTTPException, UploadFile, File
from pydantic import BaseModel
import httpx
import tempfile
import os
from datetime import datetime
from prefect.client.orchestration import PrefectClient
from prefect.exceptions import ObjectNotFound
from uuid import UUID

import sys
sys.path.append('/app/shared')

from config import get_prefect_client, get_minio_client, BUCKET_NAME
from storage import upload_file

router = APIRouter(prefix="/nhs-data", tags=["pipeline"])

class PipelineTrigger(BaseModel):
    url: str

@router.post("/trigger-pipeline")
async def trigger_nhs_pipeline(trigger: PipelineTrigger):
    """Download file from URL and trigger processing pipeline."""
    minio_client = get_minio_client()
    prefect_client = get_prefect_client()
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(trigger.url)
            response.raise_for_status()
            
            url_filename = trigger.url.split('/')[-1] if '/' in trigger.url else 'downloaded_file.xlsx'
            if not url_filename.endswith(('.xlsx', '.xls')):
                url_filename += '.xlsx'
            
            object_name = f"url_downloads/{datetime.now().isoformat()}_{url_filename}"
            
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
                tmp_file.write(response.content)
                tmp_file_path = tmp_file.name
            
            upload_file(minio_client, BUCKET_NAME, object_name, tmp_file_path)
            minio_url = f"minio://{BUCKET_NAME}/{object_name}"
            os.unlink(tmp_file_path)
        
        deployment = await prefect_client.read_deployment_by_name("nhs-bed-occupancy-pipeline-flow/Data Pipeline Flow")
        
        flow_run = await prefect_client.create_flow_run_from_deployment(
            deployment_id=deployment.id,
            parameters={"url": minio_url, "is_upload": False}
        )
        
        return {
            "message": "File downloaded and pipeline started successfully",
            "original_url": trigger.url,
            "minio_path": object_name,
            "flow_run_id": str(flow_run.id),
            "flow_run_name": flow_run.name,
            "status_url": f"/nhs-data/status/{flow_run.id}"
        }
        
    except httpx.HTTPError as e:
        raise HTTPException(status_code=400, detail=f"Failed to download file from URL: {str(e)}")
    except ObjectNotFound:
        # Try to list available deployments to help debug
        try:
            deployments = await prefect_client.read_deployments()
            available_deployments = [f"{d.flow_name}/{d.name}" for d in deployments]
            detail = f"Deployment 'nhs-bed-occupancy-pipeline-flow/Data Pipeline Flow' not found. Available deployments: {available_deployments}. Please wait for the worker to initialize."
        except Exception:
            detail = "Deployment not found. Please wait for the worker to initialize."
        raise HTTPException(status_code=404, detail=detail)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/upload")
async def upload_nhs_file(file: UploadFile = File(...)):
    """Upload an NHS Excel file and trigger processing pipeline."""
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Only Excel files (.xlsx, .xls) are supported")
    
    minio_client = get_minio_client()
    prefect_client = get_prefect_client()
    tmp_file_path = None
    
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{file.filename}") as tmp_file:
            content = await file.read()
            tmp_file.write(content)
            tmp_file_path = tmp_file.name

        object_name = f"nhs_uploads/{datetime.now().isoformat()}_{file.filename}"
        upload_file(minio_client, BUCKET_NAME, object_name, tmp_file_path)
        minio_url = f"minio://{BUCKET_NAME}/{object_name}"
        
        deployment = await prefect_client.read_deployment_by_name("nhs-bed-occupancy-pipeline-flow/Data Pipeline Flow")
        
        flow_run = await prefect_client.create_flow_run_from_deployment(
            deployment_id=deployment.id,
            parameters={"url": minio_url, "is_upload": True}
        )
        
        return {
            "message": "File uploaded and pipeline started successfully",
            "filename": file.filename,
            "minio_path": object_name,
            "flow_run_id": str(flow_run.id),
            "flow_run_name": flow_run.name,
            "status_url": f"/nhs-data/status/{flow_run.id}"
        }
            
    except ObjectNotFound:
        # Try to list available deployments to help debug
        try:
            deployments = await prefect_client.read_deployments()
            available_deployments = [f"{d.flow_name}/{d.name}" for d in deployments]
            detail = f"Deployment 'nhs-bed-occupancy-pipeline-flow/Data Pipeline Flow' not found. Available deployments: {available_deployments}. Please wait for the worker to initialize."
        except Exception:
            detail = "Deployment not found. Please wait for the worker to initialize."
        raise HTTPException(status_code=404, detail=detail)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            os.unlink(tmp_file_path)

@router.get("/status/{flow_run_id}")
async def get_flow_status(flow_run_id: str):
    """Check the status of a specific flow run."""
    prefect_client = get_prefect_client()
    
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