
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel, validator
from typing import Optional, List
from datetime import datetime

DATABASE_URL = "sqlite:///./weather.db"
engine = create_engine(DATABASE_URL, connect_args={{"check_same_thread": False}})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Database model
class ClimateEntry(Base):
    __tablename__ = "weather_records"
    id = Column(Integer, primary_key=True, index=True)
    location = Column(String, index=True, nullable=False)
    date = Column(DateTime, default=datetime.utcnow, nullable=False)
    temp_celsius = Column(Float, nullable=False)
    moisture_pct = Column(Float, nullable=False)
    wind_kph = Column(Float, nullable=True)
    summary = Column(String, nullable=True)

# Pydantic Schemas
class ClimateEntryBase(BaseModel):
    location: str
    temp_celsius: float
    moisture_pct: float
    wind_kph: Optional[float] = None
    summary: Optional[str] = None

    @validator('temp_celsius')
    def validate_temp(cls, v):
        if v < -80 or v > 60:
            raise ValueError("Temperature must be between -80°C and 60°C")
        return v

    @validator('moisture_pct')
    def validate_humidity(cls, v):
        if v < 0 or v > 100:
            raise ValueError("Humidity must be between 0% and 100%")
        return v

class ClimateEntryCreate(ClimateEntryBase):
    pass

class ClimateEntryUpdate(BaseModel):
    temp_celsius: Optional[float] = None
    moisture_pct: Optional[float] = None
    wind_kph: Optional[float] = None
    summary: Optional[str] = None

    @validator('temp_celsius')
    def validate_temp(cls, v):
        if v is not None and (v < -80 or v > 60):
            raise ValueError("Temperature must be between -80°C and 60°C")
        return v

    @validator('moisture_pct')
    def validate_humidity(cls, v):
        if v is not None and (v < 0 or v > 100):
            raise ValueError("Humidity must be between 0% and 100%")
        return v

class ClimateEntryOut(ClimateEntryBase):
    id: int
    date: datetime

    class Config:
        orm_mode = True

# FastAPI setup
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# CRUD and Routes
@app.post("/records/", response_model=ClimateEntryOut, status_code=status.HTTP_201_CREATED)
def create_entry(record: ClimateEntryCreate, db: Session = Depends(get_db)):
    db_record = ClimateEntry(**record.dict())
    db.add(db_record)
    db.commit()
    db.refresh(db_record)
    return db_record

@app.get("/records/", response_model=List[ClimateEntryOut])
def read_all(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return db.query(ClimateEntry).offset(skip).limit(limit).all()

@app.get("/records/{{record_id}}", response_model=ClimateEntryOut)
def read_one(record_id: int, db: Session = Depends(get_db)):
    record = db.query(ClimateEntry).filter(ClimateEntry.id == record_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Record not found")
    return record

@app.get("/records/location/{{loc}}", response_model=List[ClimateEntryOut])
def read_by_location(loc: str, db: Session = Depends(get_db)):
    result = db.query(ClimateEntry).filter(ClimateEntry.location == loc).all()
    if not result:
        raise HTTPException(status_code=404, detail="No data for that location")
    return result

@app.get("/records/location/{{loc}}/avg/temp", response_model=dict)
def average_temp(loc: str, db: Session = Depends(get_db)):
    avg = db.query(func.avg(ClimateEntry.temp_celsius).label("avg_temp")).filter(ClimateEntry.location == loc).scalar()
    return {"location": loc, "average_temperature": avg}

@app.get("/records/location/{{loc}}/avg/moisture", response_model=dict)
def average_humidity(loc: str, db: Session = Depends(get_db)):
    avg = db.query(func.avg(ClimateEntry.moisture_pct).label("avg_moisture")).filter(ClimateEntry.location == loc).scalar()
    return {"location": loc, "average_moisture": avg}

@app.put("/records/{{record_id}}", response_model=ClimateEntryOut)
def update_entry(record_id: int, record: ClimateEntryUpdate, db: Session = Depends(get_db)):
    db_record = db.query(ClimateEntry).filter(ClimateEntry.id == record_id).first()
    if not db_record:
        raise HTTPException(status_code=404, detail="Record not found")
    for key, val in record.dict(exclude_unset=True).items():
        setattr(db_record, key, val)
    db.commit()
    db.refresh(db_record)
    return db_record

@app.delete("/records/{{record_id}}", status_code=status.HTTP_204_NO_CONTENT)
def delete_entry(record_id: int, db: Session = Depends(get_db)):
    record = db.query(ClimateEntry).filter(ClimateEntry.id == record_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Record not found")
    db.delete(record)
    db.commit()
    return {{"ok": True}}
