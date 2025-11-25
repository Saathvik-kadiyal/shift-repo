from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session,joinedload
from db import get_db
from models.models import ShiftAllowances,ShiftMapping
from utils.dependencies import get_current_user
from schemas.displayschema import PaginatedShiftResponse,EmployeeResponse,ShiftUpdateRequest,ShiftUpdateResponse
from services.display_service import update_shift_service,display_emp_details
from sqlalchemy import func

router = APIRouter(prefix="/display")

@router.get("/", response_model=PaginatedShiftResponse)
def get_all_data(
    start: int = Query(0, ge=0),
    limit: int = Query(10, gt=0),
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
):
    query = (
        db.query(
            ShiftAllowances.id.label("id"),
            ShiftAllowances.emp_id.label("emp_id"),
            ShiftAllowances.emp_name.label("emp_name"),
            ShiftAllowances.department.label("department"),
            ShiftAllowances.payroll_month.label("month"),
            ShiftAllowances.client.label("client"),
            ShiftAllowances.project_code.label("project_code"),
            func.array_agg(ShiftMapping.shift_type).label("shift_category")
        )
        .outerjoin(ShiftMapping, ShiftAllowances.id == ShiftMapping.shiftallowance_id)
        .group_by(ShiftAllowances.id)
    )
 
    total_records = query.count()
 
    data = (
        query.order_by(ShiftAllowances.id.asc())
        .offset(start)
        .limit(limit)
        .all()
    )
 
    if not data:
        raise HTTPException(status_code=404, detail="No data found for given range")
 
    return {
        "total_records": total_records,
        "data": data
    }

@router.get("/{emp_id}")
def get_employee_shift_details(
    emp_id: str,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    return display_emp_details(emp_id, db)


@router.put("/update/{record_id}", response_model=ShiftUpdateResponse)
def update_detail_data(record_id: int, req: ShiftUpdateRequest, db: Session = Depends(get_db), current_user = Depends(get_current_user)):
    updates = req.model_dump() 
    
    result = update_shift_service(db, record_id, updates)
    
    return {
        "message": "Shift updated successfully",
        "updated_fields": [k for k, v in updates.items() if v > 0],
        "total_days": result["total_days"],
        "total_allowance": result["total_allowance"],
        "shift_details": result["shift_details"]
    }

