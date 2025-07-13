import os
import time
from sqlalchemy import create_engine, Column, Integer, String, Date, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import OperationalError

Base = declarative_base()

# New table for NHS Bed Occupancy data
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

def create_database_connection():
    DATABASE_URL = os.environ.get("DATABASE_URL")
    
    engine = None
    while engine is None:
        try:
            engine = create_engine(DATABASE_URL)
            Base.metadata.create_all(bind=engine) # Ensure all tables are created
            print("Database connection successful!")
        except OperationalError:
            print("Database connection failed. Retrying in 5 seconds...")
            time.sleep(5)
    
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return engine, SessionLocal

def get_db():
    _, SessionLocal = create_database_connection()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()