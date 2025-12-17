from datetime import datetime
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func

from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount
from utils.client_enums import Company

def export_filtered_excel(
    db: Session,
    emp_id: str | None = None,
    account_manager: str | None = None,
    department: str | None = None,
    client: str | None = None,
    start_month: str | None = None,
    end_month: str | None = None,
    start: int | None = 0,
    limit: int | None = 10,
):

    
    rates = {}
    for r in db.query(ShiftsAmount).all():
        if r.shift_type:
            rates[r.shift_type.upper()] = float(r.amount or 0)

 
    if end_month and not start_month:
        raise HTTPException(400, "start_month is required when end_month is provided")

    for m in [start_month, end_month]:
        if m:
            try:
                datetime.strptime(m, "%Y-%m")
            except Exception:
                raise HTTPException(400, "Month must be YYYY-MM")

    if start_month and end_month and start_month > end_month:
        raise HTTPException(400, "start_month cannot be greater than end_month")

    current_month = datetime.now().strftime("%Y-%m")

    if start_month and start_month > current_month:
        raise HTTPException(400, "start_month cannot be in future")
    if end_month and end_month > current_month:
        raise HTTPException(400, "end_month cannot be in future")


    base = db.query(
        ShiftAllowances.id,
        ShiftAllowances.emp_id,
        ShiftAllowances.emp_name,
        ShiftAllowances.grade,
        ShiftAllowances.department,
        ShiftAllowances.client,
        ShiftAllowances.project,
        ShiftAllowances.account_manager,
        func.to_char(ShiftAllowances.duration_month, "YYYY-MM").label("duration_month"),
        func.to_char(ShiftAllowances.payroll_month, "YYYY-MM").label("payroll_month"),
    )

    if start_month and end_month:
        base = base.filter(
            func.to_char(ShiftAllowances.duration_month, "YYYY-MM") >= start_month,
            func.to_char(ShiftAllowances.duration_month, "YYYY-MM") <= end_month,
        )
    elif start_month:
        base = base.filter(
            func.to_char(ShiftAllowances.duration_month, "YYYY-MM") == start_month
        )

    if emp_id:
        base = base.filter(func.upper(ShiftAllowances.emp_id).like(f"%{emp_id.upper()}%"))
    if account_manager:
        base = base.filter(func.upper(ShiftAllowances.account_manager).like(f"%{account_manager.upper()}%"))
    if department:
        base = base.filter(func.upper(ShiftAllowances.department).like(f"%{department.upper()}%"))
    if client:
        base = base.filter(func.upper(ShiftAllowances.client).like(f"%{client.upper()}%"))

   
    overall_records = base.count()

   
    query = base.order_by(
        ShiftAllowances.duration_month.desc(),
        ShiftAllowances.emp_id.asc(),
    )

    if start is not None and limit is not None:
        query = query.offset(start).limit(limit)

    rows = query.all()

    if not rows:
        raise HTTPException(404, "No data found")

    SHIFT_LABELS = {
        "A": "A(9PM to 6AM)",
        "B": "B(4PM to 1AM)",
        "C": "C(6AM to 3PM)",
        "PRIME": "PRIME(12AM to 9AM)",
    }

    total_shift_details = {
        "A(9PM to 6AM)": 0.0,
        "B(4PM to 1AM)": 0.0,
        "C(6AM to 3PM)": 0.0,
        "PRIME(12AM to 9AM)": 0.0,
    }

    overall_total_allowance = 0.0
    result = []

   
    for row in rows:
        d = row._asdict()
        sid = d.pop("id")

        mappings = db.query(
            ShiftMapping.shift_type,
            ShiftMapping.days,
        ).filter(ShiftMapping.shiftallowance_id == sid).all()

        shift_details = {}
        total_allowance = 0.0

        for m in mappings:
            st = (m.shift_type or "").upper()
            days = float(m.days or 0)

            rate = rates.get(st, 0.0)
            allowance = days * rate

            total_allowance += allowance
            overall_total_allowance += allowance

            label = SHIFT_LABELS.get(st, st)
            if days > 0:
                shift_details[label] = days
                total_shift_details[label] += days

        d["shift_details"] = shift_details
        d["total_allowance"] = round(total_allowance, 2)

        
        client_value = d.get("client")
        abbr = next((c.name for c in Company if c.value == client_value), None)
        if abbr:
            d["client"] = abbr

        result.append(d)

 
    result.append({
        "shift_details": {
            k: int(v) if v.is_integer() else v
            for k, v in total_shift_details.items()
            if v > 0
        },
        "total_allowance": round(overall_total_allowance, 2)
    })

    if start is not None and limit is not None:
        total_records = len(rows)       
    else:
        total_records = overall_records 

    return total_records, {
    "employees": result
}

