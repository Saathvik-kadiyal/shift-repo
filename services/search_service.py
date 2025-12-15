from datetime import datetime
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func
from models.models import ShiftAllowances, ShiftMapping,ShiftsAmount
from utils.client_enums import Company

def export_filtered_excel(
    db: Session,
    emp_id: str | None = None,
    account_manager: str | None = None,
    department: str | None = None,
    client: str | None = None,
    start_month: str | None = None,
    end_month: str | None = None,
    start: int = 0,
    limit: int = 10,
):

    
    def _load_shift_rates():
        rates = {}
        rows = db.query(ShiftsAmount).all()
        for r in rows:
            if r.shift_type:
                rates[r.shift_type.upper()] = float(r.amount)
        return rates

    rates = _load_shift_rates()

    
    if end_month and not start_month:
        raise HTTPException(status_code=400, detail="start_month is required when end_month is provided")

    for m in [start_month, end_month]:
        if m:
            try:
                datetime.strptime(m, "%Y-%m")
            except:
                raise HTTPException(status_code=400, detail="Month must be YYYY-MM")

    current_month = datetime.now().strftime("%Y-%m")

    if start_month and start_month > current_month:
        raise HTTPException(status_code=400, detail="start_month cannot be in future")
    if end_month and end_month > current_month:
        raise HTTPException(status_code=400, detail="end_month cannot be in future")

   
    base = (
        db.query(
            ShiftAllowances.id,
            ShiftAllowances.emp_id,
            ShiftAllowances.emp_name,
            ShiftAllowances.grade,
            ShiftAllowances.department,
            ShiftAllowances.client,
            ShiftAllowances.project,
            ShiftAllowances.account_manager,
            func.to_char(ShiftAllowances.duration_month, "YYYY-MM").label("duration_month"),
            func.to_char(ShiftAllowances.payroll_month, "YYYY-MM").label("payroll_month")
        )
    )

    if start_month and end_month:
        base = base.filter(
            func.to_char(ShiftAllowances.duration_month, "YYYY-MM") >= start_month,
            func.to_char(ShiftAllowances.duration_month, "YYYY-MM") <= end_month
        )
    elif start_month:
        base = base.filter(func.to_char(ShiftAllowances.duration_month, "YYYY-MM") == start_month)

    if emp_id:
        base = base.filter(func.upper(ShiftAllowances.emp_id).like(f"%{emp_id.upper()}%"))
    if account_manager:
        base = base.filter(func.upper(ShiftAllowances.account_manager).like(f"%{account_manager.upper()}%"))
    if department:
        base = base.filter(func.upper(ShiftAllowances.department).like(f"%{department.upper()}%"))
    if client:
        base = base.filter(func.upper(ShiftAllowances.client).like(f"%{client.upper()}%"))

    total_records = base.count()

    rows = base.order_by(
        ShiftAllowances.duration_month.desc(),
        ShiftAllowances.emp_id.asc()
    ).offset(start).limit(limit).all()

    if not rows:
        raise HTTPException(status_code=404, detail="No data found")

    result = []

    SHIFT_LABELS = {
        "A": "A(9PM to 6AM)",
        "B": "B(4PM to 1AM)",
        "C": "C(6AM to 3PM)",
        "PRIME": "PRIME(12AM to 9AM)"
    }

    
    for row in rows:
        d = row._asdict()
        sid = d.pop("id")

       
        mappings = db.query(
            ShiftMapping.shift_type,
            ShiftMapping.days
        ).filter(
            ShiftMapping.shiftallowance_id == sid
        ).all()

        shift_details = {}
        total_allowance = 0.0

        for m in mappings:
            st = m.shift_type.upper()
            days = float(m.days or 0)

            
            rate = rates.get(st, 0.0)
            total_allowance += days * rate

            if days > 0:
                shift_details[SHIFT_LABELS.get(st, st)] = days

       
        d["total_allowance"] = round(total_allowance, 2)
        d["shift_details"] = shift_details

       
        client_value = d.get("client")
        abbr = next((c.name for c in Company if c.value == client_value), None)
        if abbr:
            d["client"] = abbr

        result.append(d)

    return total_records, result
