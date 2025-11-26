from fastapi import HTTPException
from sqlalchemy.orm import Session,joinedload
from sqlalchemy import extract
from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount
from datetime import datetime

def parse_shift_value(value: str):
    """Convert input to float and validate shift value."""
    if value is None or str(value).strip() == "":
        return 0
    try:
        num = float(str(value).strip())
    except:
        raise HTTPException(status_code=400, detail=f"Invalid shift value '{value}'. Only numbers allowed.")
    if num < 0:
        raise HTTPException(status_code=400, detail=f"Negative values not allowed: '{value}'.")
    if num > 22:
        raise HTTPException(status_code=400, detail=f"Can't add more than 22 days per shift.")
    return num
 
def update_shift_service(db: Session, emp_id: str, payroll_month: str, updates: dict):
    """
    Update shift days for a given employee and payroll month.
    """
    allowed_fields = ["shift_a", "shift_b", "shift_c", "prime"]
    extra_fields = [k for k in updates if k not in allowed_fields]
    if extra_fields:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid fields: {extra_fields}. Only {allowed_fields} allowed."
        )
 
    # Convert raw strings to numeric values
    numeric_updates = {k: parse_shift_value(v) for k, v in updates.items()}
 
    # Map to DB shift types
    shift_map = {"shift_a": "A", "shift_b": "B", "shift_c": "C", "prime": "PRIME"}
    mapped_updates = {shift_map[k]: numeric_updates[k] for k in numeric_updates if numeric_updates[k] >= 0}
 
    if not mapped_updates:
        raise HTTPException(status_code=400, detail="No valid shift values provided.")
 
    # Parse payroll_month YYYY-MM to date
    try:
        payroll_date = datetime.strptime(payroll_month, "%Y-%m")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid payroll_month format. Use YYYY-MM")
 
    # Get record by emp_id + payroll_month
    record = (
        db.query(ShiftAllowances)
        .filter(
            ShiftAllowances.emp_id == emp_id,
            extract("year", ShiftAllowances.payroll_month) == payroll_date.year,
            extract("month", ShiftAllowances.payroll_month) == payroll_date.month
        )
        .first()
    )
 
    if not record:
        raise HTTPException(status_code=404, detail=f"No shift record found for employee {emp_id} and month {payroll_month}")
 
    # Get rates from DB
    rate_rows = db.query(ShiftsAmount).all()
    rates = {r.shift_type.upper(): float(r.amount) for r in rate_rows}
 
    for stype in mapped_updates:
        if stype not in rates:
            raise HTTPException(status_code=400, detail=f"Missing rate for shift '{stype}'.")
 
    existing = {m.shift_type: m for m in record.shift_mappings}
 
    # Apply updates temporarily for validation
    for stype, days in mapped_updates.items():
        if stype in existing:
            existing[stype].days = days
        else:
            temp = ShiftMapping(
                shiftallowance_id=record.id,
                shift_type=stype,
                days=days
            )
            existing[stype] = temp
 
    # Validate total days (<=22)
    total_days_temp = float(sum(float(m.days) for m in existing.values()))
    if total_days_temp > 22:
        db.rollback()
        raise HTTPException(
            status_code=400,
            detail=f"Total days cannot exceed 22 in a month. Current total = {total_days_temp}"
        )
 
    # Commit real updates
    for stype, days in mapped_updates.items():
        if stype not in [m.shift_type for m in record.shift_mappings]:
            db.add(existing[stype])
 
    db.commit()
    db.refresh(record)
 
    # Prepare response
    shift_details = [
        {"shift": m.shift_type, "days": float(m.days)}
        for m in record.shift_mappings
        if m.shift_type in mapped_updates
    ]
 
    total_days = float(sum(float(m.days) for m in record.shift_mappings))
    total_allowance = float(sum(float(m.days) * rates[m.shift_type] for m in record.shift_mappings))
 
    return {
        "emp_id": emp_id,
        "payroll_month": payroll_month,
        "updated_fields": list(mapped_updates.keys()),
        "total_days": total_days,
        "total_allowance": total_allowance,
        "shift_details": shift_details
    }

def fetch_shift_record(emp_id: str, duration_month: str, payroll_month: str, db: Session):

    # convert input month strings to DATE format (YYYY-MM)
    try:
        duration_dt = datetime.strptime(duration_month + "-01", "%Y-%m-%d").date()
        payroll_dt = datetime.strptime(payroll_month + "-01", "%Y-%m-%d").date()
    except:
        raise HTTPException(status_code=400, detail="Invalid month format. Expected YYYY-MM")

    record = (
        db.query(ShiftAllowances)
        .options(joinedload(ShiftAllowances.shift_mappings))
        .filter(
            ShiftAllowances.emp_id == emp_id,
            ShiftAllowances.duration_month == duration_dt,
            ShiftAllowances.payroll_month == payroll_dt,
        )
        .first()
    )

    if not record:
        raise HTTPException(status_code=404, detail="Record not found")

    # Base response body
    result = {
        "id": record.id,
        "emp_id": record.emp_id,
        "emp_name": record.emp_name,
        "grade": record.grade,
        "department": record.department,
        "client": record.client,
        "project": record.project,
        "project_code": record.project_code,
        "account_manager": record.account_manager,
        "practice_lead": record.practice_lead,
        "delivery_manager": record.delivery_manager,
        "duration_month": record.duration_month.strftime("%Y-%m"),
        "payroll_month": record.payroll_month.strftime("%Y-%m"),
        "billability_status": record.billability_status,
        "practice_remarks": record.practice_remarks,
        "rmg_comments": record.rmg_comments,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
        # initialize shifts with default 0
        "A": 0,
        "B": 0,
        "C": 0,
        "PRIME": 0
    }

    # update shifts with values from mappings
    for m in record.shift_mappings:
        stype = m.shift_type.strip().upper()
        if stype in ("A", "B", "C", "PRIME"):
            result[stype] = float(m.days)

    return result