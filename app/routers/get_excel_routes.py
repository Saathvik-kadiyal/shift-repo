"""
Excel Routes for Shift Allowance Data.
 
Provides an API endpoint to download filtered employee shift allowance
data as an Excel file. Uses openpyxl for fast Excel generation.
"""
 
 
import io
from typing import Optional
 
 
from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from pydantic import BaseModel
 
 
from db import get_db
from services.get_excel_service import export_filtered_excel_openpyxl
from utils.dependencies import get_current_user
 
router = APIRouter(prefix="/excel", tags=["Excel Data"])
 
 
class ExcelFilters(BaseModel):
    """Optional filters for exporting shift allowance data."""
    emp_id: Optional[str] = None
    account_manager: Optional[str] = None
    department: Optional[str] = None
    client: Optional[str] = None
    start_month: Optional[str] = None
    end_month: Optional[str] = None
 
 
@router.get("/download")
def download_excel(
    filters: ExcelFilters = Depends(),
    db: Session = Depends(get_db),
    _current_user=Depends(get_current_user),
):
    """
    Download filtered shift allowance data as an Excel file.
 
    Filters (all optional):
        - emp_id: Employee ID
        - account_manager: Account manager name
        - department: Employee department
        - client: Client name
        - start_month / end_month: Month range in YYYY-MM format
 
    Returns:
        StreamingResponse: Excel file stream with proper headers for download.
    """
    # Generate workbook using service
    wb = export_filtered_excel_openpyxl(
        db=db,
        emp_id=filters.emp_id,
        account_manager=filters.account_manager,
        department=filters.department,
        client=filters.client,
        start_month=filters.start_month,
        end_month=filters.end_month,
    )
 
    # Save workbook to in-memory bytes buffer
    file_stream = io.BytesIO()
    wb.save(file_stream)
    file_stream.seek(0)
 
    return StreamingResponse(
        file_stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=shift_data.xlsx"}
    )
 
 