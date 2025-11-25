from fastapi import HTTPException
from sqlalchemy.orm import Session,joinedload
from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount

def parse_shift_value(value: str):
    if value is None or str(value).strip() == "":
        return 0
    try:
        num = float(str(value).strip())
    except:
        raise HTTPException(status_code=400, detail=f"Invalid shift value '{value}'. Only numbers allowed.")
    if num < 0:
        raise HTTPException(status_code=400, detail=f"Negative values not allowed: '{value}'.")
    return num


def update_shift_service(db: Session, record_id: int, updates: dict):
    allowed_fields = ["shift_a", "shift_b", "shift_c", "prime"]
    extra_fields = [k for k in updates if k not in allowed_fields]
    if extra_fields:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid fields: {extra_fields}. Only {allowed_fields} allowed."
        )

    # Convert raw strings to numeric values
    numeric_updates = {k: parse_shift_value(v) for k, v in updates.items()}

    # Rename to DB shift types + ignore zero updates
    shift_map = {"shift_a": "A", "shift_b": "B", "shift_c": "C", "prime": "PRIME"}
    mapped_updates = {shift_map[k]: numeric_updates[k] for k in numeric_updates if numeric_updates[k] > 0}

    if not mapped_updates:
        raise HTTPException(status_code=400, detail="No valid shift values provided.")

    # Get record
    record = db.query(ShiftAllowances).filter(ShiftAllowances.id == record_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Shift allowance record not found")

    # Get rates from DB
    rate_rows = db.query(ShiftsAmount).all()
    rates = {r.shift_type.upper(): float(r.amount) for r in rate_rows}

    # Ensure every required rate exists
    for stype in mapped_updates:
        if stype not in rates:
            raise HTTPException(status_code=400, detail=f"Missing rate for shift '{stype}'.")

    # Save updated mapping
    existing = {m.shift_type: m for m in record.shift_mappings}

    for stype, days in mapped_updates.items():
        if stype in existing:
            existing[stype].days = days
        else:
            db.add(ShiftMapping(
                shiftallowance_id=record.id,
                shift_type=stype,
                days=days
            ))

    db.commit()
    db.refresh(record)

    # Format response
    shift_details = [
        {"shift": m.shift_type, "days": float(m.days)}
        for m in record.shift_mappings
        if m.shift_type in mapped_updates
    ]

    total_days = float(sum(float(m.days) for m in record.shift_mappings if m.shift_type in mapped_updates))
    total_allowance = float(sum(float(m.days) * rates[m.shift_type] for m in record.shift_mappings if m.shift_type in mapped_updates))

    return {
        "updated_fields": list(mapped_updates.keys()),
        "total_days": total_days,
        "total_allowance": total_allowance,
        "shift_details": shift_details
    }
def display_emp_details(emp_id: str, db: Session):
    data = (
        db.query(ShiftAllowances)
        .options(joinedload(ShiftAllowances.shift_mappings))
        .filter(ShiftAllowances.emp_id == emp_id)
        .order_by(ShiftAllowances.payroll_month.asc())
        .all()
    )

    if not data:
        raise HTTPException(status_code=404, detail="Employee not found")

    base = data[0]

    result = {
        "emp_id": base.emp_id,
        "emp_name": base.emp_name,
        "available_payroll_months": [],
        "months": []
    }

    for row in data:
        payroll_month_str = row.payroll_month.strftime("%Y-%m")
        result["available_payroll_months"].append(payroll_month_str)

        month_obj = {
            "id": row.id,
            "payroll_month": payroll_month_str,
            "grade": row.grade,
            "department": row.department,
            "client": row.client,
            "project": row.project,
            "project_code": row.project_code,
            "account_manager": row.account_manager,
            "practice_lead": row.practice_lead,
            "delivery_manager": row.delivery_manager,
            "duration_month": row.duration_month,
            "billability_status": row.billability_status,
            "practice_remarks": row.practice_remarks,
            "rmg_comments": row.rmg_comments,
            "created_at": row.created_at,
            "updated_at": row.updated_at,

            # shift days
            "A": 0,
            "B": 0,
            "C": 0,
            "PRIME": 0
        }

        for m in row.shift_mappings:
            stype = m.shift_type.strip().upper()
            if stype in month_obj:
                month_obj[stype] += float(m.days)

        result["months"].append(month_obj)

    return result