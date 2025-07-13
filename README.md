# NHS Bed Occupancy Analytics Platform

A comprehensive platform for processing, storing, and visualizing NHS bed occupancy data using FastAPI, Prefect, PostgreSQL, MinIO, and Streamlit.

## Architecture

- **FastAPI API**: REST API for data processing and retrieval
- **Prefect**: Workflow orchestration for data pipelines
- **PostgreSQL**: Database for storing processed data
- **MinIO**: Object storage for file uploads and results
- **Streamlit**: Interactive web dashboard for data visualization

## Services

- **API** (port 5001): FastAPI application with endpoints for data processing
- **Prefect Server** (port 4200): Workflow orchestration server
- **Prefect Worker**: Executes data processing workflows
- **PostgreSQL** (port 5433): Database service
- **MinIO** (ports 9000/9001): Object storage service
- **Streamlit** (port 8501): Web dashboard

## Quick Start

1. **Start all services:**
   ```bash
   docker-compose up -d
   ```

2. **Check service health:**
   ```bash
   docker-compose ps
   ```

3. **Access the applications:**
   - **Streamlit Dashboard**: http://localhost:8501
   - **FastAPI API**: http://localhost:5001
   - **Prefect UI**: http://localhost:4200
   - **MinIO Console**: http://localhost:9001 (admin/minioadmin)

## API Endpoints

### Data Processing
- `POST /nhs-data/trigger-pipeline` - Trigger pipeline with URL
- `POST /nhs-data/upload` - Upload Excel file for processing
- `GET /nhs-data/status/{flow_run_id}` - Check pipeline status

### Data Retrieval
- `GET /nhs-data/occupancy` - Get all occupancy data
- `GET /nhs-data/sample` - Get sample data
- `GET /nhs-data/organization/{org_code}` - Get data by organization code
- `GET /nhs-data/organization-name/{org_name}` - Search by organization name

### System
- `GET /health` - Health check

## Usage

### Via Streamlit Dashboard (Recommended)
1. Go to http://localhost:8501
2. Use the upload page to process Excel files
3. View visualizations on the dashboard
4. Search for specific organizations

### Via API
1. Upload a file:
   ```bash
   curl -X POST "http://localhost:5001/nhs-data/upload" \
        -H "accept: application/json" \
        -H "Content-Type: multipart/form-data" \
        -F "file=@your_nhs_file.xlsx"
   ```

2. Get processed data:
   ```bash
   curl "http://localhost:5001/nhs-data/occupancy"
   ```

## Development

### Stop services:
```bash
docker-compose down
```

### View logs:
```bash
docker-compose logs -f [service_name]
```

### Rebuild after code changes:
```bash
docker-compose up -d --build
```

## Data Format

The system expects NHS bed occupancy Excel files with columns containing:
- Organization Code
- Organization Name  
- Available Beds Total
- Occupied Beds Total

The system will automatically calculate occupancy rates and store quarterly data.