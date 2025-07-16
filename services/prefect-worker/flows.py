import os
import sys
import json
import tempfile
from datetime import datetime
from typing import Dict, List, Any
import re

import pandas as pd
from prefect import flow, task

sys.path.append('/app/shared')
from database import BedOccupancy, BedOccupancyBySpeciality, create_database_connection
from storage import create_minio_client, ensure_bucket_exists, upload_file

engine, SessionLocal = create_database_connection()
minio_client = create_minio_client()
BUCKET_NAME = "prefect-results"
ensure_bucket_exists(minio_client, BUCKET_NAME)

@task
def download_data_task(url: str) -> str:
    """Download data from MinIO storage and return temp file path."""
    if not url.startswith("minio://"):
        raise ValueError(f"Expected MinIO URL, got: {url}")
    
    url_parts = url.replace("minio://", "").split("/", 1)
    bucket_name = url_parts[0]
    object_name = url_parts[1]
    
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        minio_client.fget_object(bucket_name, object_name, tmp.name)
        return tmp.name

def extract_quarter_year_from_filename(file_path: str) -> tuple:
    """Extract quarter and year from filename patterns."""
    filename = os.path.basename(file_path)
    
    # Remove MinIO timestamp prefix if present
    cleaned_filename = re.sub(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+_', '', filename)
    
    # Try to extract quarter from patterns like "Q4", "Quarter 4", etc.
    quarter_match = re.search(r'Quarter (\d)|Q(\d)', cleaned_filename, re.IGNORECASE)
    
    # Try to extract year from patterns like "2024-25", "2023-24", or "2015-16"
    year_match = re.search(r'(\d{4})-(\d{2})', cleaned_filename)
    if not year_match:
        year_match = re.search(r'(\d{4})', cleaned_filename)
    
    quarter = f"Q{quarter_match.group(1) or quarter_match.group(2)}" if quarter_match else "Q1"
    
    year = int(year_match.group(1)) if year_match else datetime.now().year
    
    return quarter, year

@task
def process_bed_occupancy_data_task(file_path: str, source_url: str = None) -> List[Dict[str, Any]]:
    """Process NHS bed occupancy Excel file."""
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
    valid_beds = processed_df['beds_available'] > 0
    processed_df.loc[valid_beds, 'occupancy_rate'] = (
        processed_df.loc[valid_beds, 'beds_occupied'] / processed_df.loc[valid_beds, 'beds_available']
    ) * 100
    
    filename_to_parse = source_url if source_url else file_path
    quarter, year = extract_quarter_year_from_filename(filename_to_parse)
    processed_df['quarter'] = quarter
    processed_df['year'] = year
    
    return processed_df.to_dict('records')

@task
def process_speciality_data_task(file_path: str, source_url: str = None) -> List[Dict[str, Any]]:
    """Process NHS bed occupancy by speciality Excel sheet."""
    try:
        possible_sheets = [
            'Occupied by Speciality', 
            'Occupied by Specialty',
            'By Speciality',
            'By Specialty',
            'Speciality',
            'Specialty'
        ]
        
        xl_file = pd.ExcelFile(file_path)
        df = None
        sheet_found = None
        for sheet_name in possible_sheets:
            try:
                for header_row in [14, 13, 15, 12, 11, 10, 9, 8]:
                    try:
                        df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row)
                        col_names = [str(col).strip() for col in df.columns]
                        if any('Org Code' in col or 'Org Name' in col or 'Organization' in col for col in col_names):
                            sheet_found = sheet_name
                            break
                    except (ValueError, IndexError):
                        continue
                if df is not None and sheet_found:
                    break
            except ValueError:
                continue
        
        if df is None:
            speciality_sheets = [sheet for sheet in xl_file.sheet_names if 'special' in sheet.lower()]
            if speciality_sheets:
                for header_row in [14, 13, 15, 12, 11, 10, 9, 8]:
                    try:
                        df = pd.read_excel(file_path, sheet_name=speciality_sheets[0], header=header_row)
                        col_names = [str(col).strip() for col in df.columns]
                        if any('Org Code' in col or 'Org Name' in col or 'Organization' in col for col in col_names):
                            sheet_found = speciality_sheets[0]
                            break
                    except (ValueError, IndexError):
                        continue
                if df is None:
                    return []
            else:
                return []
                
    except Exception as e:
        print(f"Could not find speciality sheet: {e}")
        return []

    if df.empty:
        return []

    expected_cols = ['Year', 'Period End', 'Region Code', 'Org Code', 'Org Name']
    
    if not all(col in df.columns for col in ['Org Code', 'Org Name']):
        return []

    processed_records = []
    
    filename_to_parse = source_url if source_url else file_path
    quarter, year = extract_quarter_year_from_filename(filename_to_parse)
    
    speciality_columns = [col for col in df.columns if col not in expected_cols and col.strip()]
    
    for _, row in df.iterrows():
        org_code = row['Org Code']
        org_name = row['Org Name']
        region_code = row.get('Region Code', '')
        
        if pd.isna(org_code) or pd.isna(org_name):
            continue
            
        if str(org_code).strip().lower() in ['england', 'total']:
            continue
        
        for speciality_col in speciality_columns:
            occupied_beds = pd.to_numeric(row[speciality_col], errors='coerce')
            
            if not pd.isna(occupied_beds) and occupied_beds > 0:
                speciality_parts = speciality_col.split(' ', 1)
                if len(speciality_parts) >= 2:
                    speciality_code = speciality_parts[0]
                    speciality_name = speciality_parts[1]
                else:
                    speciality_code = speciality_col
                    speciality_name = speciality_col
                
                processed_records.append({
                    'organisation_code': str(org_code).strip(),
                    'organisation_name': str(org_name).strip(),
                    'region_code': str(region_code).strip() if not pd.isna(region_code) else '',
                    'speciality': speciality_name.strip(),
                    'speciality_code': speciality_code.strip(),
                    'beds_occupied': int(occupied_beds),
                    'quarter': quarter,
                    'year': year
                })
    
    return processed_records

@task
def store_bed_occupancy_data_task(data: List[Dict[str, Any]]):
    """Store processed bed occupancy data in the database."""
    if not data:
        return

    db = SessionLocal()
    try:
        if data:
            quarter = data[0]['quarter']
            year = data[0]['year']
            
            db.query(BedOccupancy).filter(
                BedOccupancy.quarter == quarter,
                BedOccupancy.year == year
            ).delete()
            
            for record in data:
                db.add(BedOccupancy(**record))
            db.commit()
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

@task
def store_speciality_data_task(data: List[Dict[str, Any]]):
    """Store processed speciality bed occupancy data in the database."""
    if not data:
        return

    db = SessionLocal()
    try:
        if data:
            quarter = data[0]['quarter']
            year = data[0]['year']
            
            db.query(BedOccupancyBySpeciality).filter(
                BedOccupancyBySpeciality.quarter == quarter,
                BedOccupancyBySpeciality.year == year
            ).delete()
            
            for record in data:
                db.add(BedOccupancyBySpeciality(**record))
            db.commit()
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

@flow
def nhs_bed_occupancy_pipeline_flow(url: str, is_upload: bool = False):
    """Main flow to download, process, and store NHS bed occupancy data."""
    file_path = download_data_task(url)
    
    try:
        processed_data = process_bed_occupancy_data_task(file_path, url)
        store_bed_occupancy_data_task(processed_data)
        
        speciality_data = process_speciality_data_task(file_path, url)
        store_speciality_data_task(speciality_data)
        
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
        os.remove(file_path)

@task
def get_bed_occupancy_data_task() -> List[Dict[str, Any]]:
    """Get all bed occupancy data from the database."""
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

@flow
def get_bed_occupancy_data_flow() -> List[Dict[str, Any]]:
    """Flow to get bed occupancy data for the API."""
    return get_bed_occupancy_data_task()