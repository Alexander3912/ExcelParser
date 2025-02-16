from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
from sqlalchemy.orm import Session
from dependencies import get_db
from services.excel_processor import ExcelProcessor
from services.stats_service import StatsService
from models import ProcessedFile

router = APIRouter()

@router.get("/")
def list_files(db: Session = Depends(get_db)):
    files = db.query(ProcessedFile).all()
    return files

@router.post("/upload/")
async def upload_file(file: UploadFile = File(...), db: Session = Depends(get_db)):
    processor = ExcelProcessor()
    return await processor.process_upload(file, db)

@router.get("/stats/")
def stats_endpoint(db: Session = Depends(get_db)):
    service = StatsService(db)
    return service.get_stats()