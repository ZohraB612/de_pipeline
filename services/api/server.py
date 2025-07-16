"""Modular FastAPI application."""
import sys
from fastapi import FastAPI

# Add shared directory to path
sys.path.append('/app/shared')

from routers import pipeline, data

app = FastAPI(
    title="NHS Bed Occupancy API",
    description="API for NHS bed occupancy data processing and analytics",
    version="2.1.0"
)

# Include routers
app.include_router(pipeline.router)
app.include_router(data.router)

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "version": "2.1.0"}