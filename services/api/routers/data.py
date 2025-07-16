"""Data access endpoints."""
from fastapi import APIRouter, HTTPException
from typing import List, Dict, Any
import sys
sys.path.append('/app/shared')

from config import get_session_local
from database import BedOccupancy, BedOccupancyBySpeciality

router = APIRouter(prefix="/nhs-data", tags=["data"])

@router.get("/occupancy")
async def get_occupancy_data(year: int = None, quarter: str = None) -> List[Dict[str, Any]]:
    """Retrieve bed occupancy data from the database with optional year/quarter filtering."""
    session_local = get_session_local()
    db = session_local()
    
    try:
        query = db.query(BedOccupancy)
        
        # Apply filters if provided
        if year is not None:
            query = query.filter(BedOccupancy.year == year)
        if quarter is not None:
            query = query.filter(BedOccupancy.quarter == quarter)
            
        data = query.all()
        if not data:
            return []
        
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
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    finally:
        db.close()

@router.get("/sample")
async def get_sample_data():
    """Get sample data from the database."""
    session_local = get_session_local()
    db = session_local()
    
    try:
        sample_records = db.query(BedOccupancy).limit(10).all()
        
        if not sample_records:
            return {"message": "No data found", "data": [], "columns": []}
        
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
        
        return {
            "message": "Sample data retrieved",
            "total_records": len(sample_records),
            "columns": list(data[0].keys()) if data else [],
            "data": data
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@router.get("/occupancy-by-speciality")
async def get_occupancy_by_speciality(year: int = None, quarter: str = None) -> List[Dict[str, Any]]:
    """Retrieve bed occupancy data by speciality from the database with optional year/quarter filtering."""
    session_local = get_session_local()
    db = session_local()
    
    try:
        query = db.query(BedOccupancyBySpeciality)
        
        # Apply filters if provided
        if year is not None:
            query = query.filter(BedOccupancyBySpeciality.year == year)
        if quarter is not None:
            query = query.filter(BedOccupancyBySpeciality.quarter == quarter)
            
        data = query.all()
        if not data:
            return []
        
        return [
            {
                "organisation_code": d.organisation_code,
                "organisation_name": d.organisation_name,
                "region_code": d.region_code,
                "speciality": d.speciality,
                "speciality_code": d.speciality_code,
                "beds_occupied": d.beds_occupied,
                "quarter": d.quarter,
                "year": d.year,
            }
            for d in data
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@router.get("/available-periods")
async def get_available_periods():
    """Get available years and quarters in the database."""
    session_local = get_session_local()
    db = session_local()
    
    try:
        # Get distinct years and quarters from both tables
        occupancy_periods = db.query(BedOccupancy.year, BedOccupancy.quarter).distinct().all()
        speciality_periods = db.query(BedOccupancyBySpeciality.year, BedOccupancyBySpeciality.quarter).distinct().all()
        
        # Combine and deduplicate
        all_periods = set(occupancy_periods + speciality_periods)
        
        # Organize by year
        periods_by_year = {}
        for year, quarter in all_periods:
            if year not in periods_by_year:
                periods_by_year[year] = []
            periods_by_year[year].append(quarter)
        
        # Sort quarters within each year
        for year in periods_by_year:
            periods_by_year[year] = sorted(periods_by_year[year], key=lambda x: int(x[1]))
        
        return {
            "available_years": sorted(periods_by_year.keys()),
            "periods_by_year": periods_by_year,
            "total_periods": len(all_periods)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@router.get("/organization/{org_code}")
async def get_organization_by_code(org_code: str):
    """Get organization data by code."""
    session_local = get_session_local()
    db = session_local()
    
    try:
        data = db.query(BedOccupancy).filter(BedOccupancy.organisation_code == org_code).all()
        if not data:
            raise HTTPException(status_code=404, detail="Organization not found")
        
        return {
            "organisation_name": data[0].organisation_name,
            "total_records": len(data),
            "data": [
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
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@router.get("/organization-name/{org_name}")
async def get_organization_by_name(org_name: str):
    """Get organizations by partial name match."""
    session_local = get_session_local()
    db = session_local()
    
    try:
        data = db.query(BedOccupancy).filter(BedOccupancy.organisation_name.ilike(f"%{org_name}%")).all()
        if not data:
            raise HTTPException(status_code=404, detail="No organizations found")
        
        unique_orgs = len(set(d.organisation_name for d in data))
        
        return {
            "organizations_found": unique_orgs,
            "total_records": len(data),
            "data": [
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
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()