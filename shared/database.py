import os
import time
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import OperationalError

Base = declarative_base()

class BedOccupancy(Base):
    __tablename__ = "bed_occupancy"
    id = Column(Integer, primary_key=True, index=True)
    organisation_code = Column(String, index=True)
    organisation_name = Column(String)
    quarter = Column(String)
    year = Column(Integer)
    beds_available = Column(Integer)
    beds_occupied = Column(Integer)
    occupancy_rate = Column(Float)

class BedOccupancyBySpeciality(Base):
    __tablename__ = "bed_occupancy_by_speciality"
    id = Column(Integer, primary_key=True, index=True)
    organisation_code = Column(String, index=True)
    organisation_name = Column(String)
    region_code = Column(String, index=True)
    speciality = Column(String, index=True)
    speciality_code = Column(String, index=True)
    quarter = Column(String)
    year = Column(Integer)
    beds_occupied = Column(Integer)

def create_database_connection():
    """Create database connection with retry logic."""
    database_url = os.environ.get("DATABASE_URL")
    
    while True:
        try:
            engine = create_engine(database_url)
            Base.metadata.create_all(bind=engine)
            break
        except OperationalError:
            time.sleep(5)
    
    session_local = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return engine, session_local

def get_db():
    """Database dependency for FastAPI."""
    _, session_local = create_database_connection()
    db = session_local()
    try:
        yield db
    finally:
        db.close()