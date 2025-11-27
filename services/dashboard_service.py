from sqlalchemy.orm import Session
from sqlalchemy import extract
from datetime import datetime
from decimal import Decimal
from fastapi import HTTPException
from models.models import ShiftAllowances, ShiftsAmount
from schemas.dashboardschema import VerticalGraphResponse


def get_horizontal_bar_service(db: Session, duration_month: str):

    if not duration_month:
        raise HTTPException(status_code=400, detail="duration_month is required. Example: 2025-01")

    try:
        month_date = datetime.strptime(duration_month + "-01", "%Y-%m-%d").date()
    except:
        raise HTTPException(status_code=400, detail="Invalid duration_month format. Expected YYYY-MM")

    records = (
        db.query(ShiftAllowances)
        .filter(ShiftAllowances.duration_month == month_date)
        .all()
    )

    if not records:
        raise HTTPException(status_code=404, detail="No records found for this duration_month")

    output = {}

    for row in records:
        client = row.client or "Unknown"
        if client not in output:
            output[client] = {
                "total_unique_employees": set(),
                "A": Decimal(0),
                "B": Decimal(0),
                "C": Decimal(0),
                "PRIME": Decimal(0)
            }

        output[client]["total_unique_employees"].add(row.emp_id)

        for mapping in row.shift_mappings:
            stype = mapping.shift_type.strip().upper()
            if stype in ("A", "B", "C", "PRIME"):
                output[client][stype] += Decimal(mapping.days or 0)

    for client, info in output.items():
        info["total_unique_employees"] = len(info["total_unique_employees"])
        for k in ("A", "B", "C", "PRIME"):
            info[k] = float(info[k])

    return {"horizontal_bar": output}



def get_graph_service(db: Session, client_name: str):

    if not client_name:
        raise HTTPException(status_code=400, detail="client_name is required")

    current_year = datetime.now().year
    monthly_allowances = {}

    # Fetch shift rate table once
    rate_rows = db.query(ShiftsAmount).all()
    rates = {r.shift_type.strip().upper(): Decimal(str(r.amount)) for r in rate_rows}

    for month in range(1, 13):
        records = db.query(ShiftAllowances).filter(
            ShiftAllowances.client == client_name,
            extract("year", ShiftAllowances.duration_month) == current_year,
            extract("month", ShiftAllowances.duration_month) == month
        ).all()

        month_key = datetime(1900, month, 1).strftime("%b")

        if not records:
            monthly_allowances[month_key] = 0.0
            continue

        total_amount = Decimal(0)

        for row in records:
            for mapping in row.shift_mappings:
                stype = mapping.shift_type.strip().upper()
                days = Decimal(mapping.days or 0)
                rate = rates.get(stype, Decimal(0))
                total_amount += days * rate

        monthly_allowances[month_key] = float(total_amount)

    return {"graph": monthly_allowances}



def get_all_clients_service(db: Session):
    clients = db.query(ShiftAllowances.client).distinct().all()
    client_list = [c[0] for c in clients if c[0]]
    return {"clients": client_list}


def get_piechart_shift_summary(db: Session, duration_month: str):

    if " " in duration_month:
        raise HTTPException(status_code=400, detail="Spaces are not allowed in duration_month. Use format YYYY-MM")

    try:
        year, month = map(int, duration_month.split("-"))
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid duration_month format. Use YYYY-MM")

    records = (
        db.query(ShiftAllowances)
        .filter(
            extract("year", ShiftAllowances.duration_month) == year,
            extract("month", ShiftAllowances.duration_month) == month
        )
        .all()
    )

    if not records:
        raise HTTPException(status_code=404, detail=f"No shift data found for duration_month '{duration_month}'")

    rate_rows = db.query(ShiftsAmount).all()
    rates = {r.shift_type.upper(): float(r.amount) for r in rate_rows}

    summary = {}

    for row in records:
        client = row.client or "Unknown"

        if client not in summary:
            summary[client] = {
                "employees": set(),
                "shift_a": 0,
                "shift_b": 0,
                "shift_c": 0,
                "prime": 0,
                "total_allowances": 0
            }

        summary[client]["employees"].add(row.emp_id)

        for mapping in row.shift_mappings:
            stype = mapping.shift_type.upper()
            days = int(mapping.days or 0)

            if stype == "A":
                summary[client]["shift_a"] += days
            elif stype == "B":
                summary[client]["shift_b"] += days
            elif stype == "C":
                summary[client]["shift_c"] += days
            elif stype == "PRIME":
                summary[client]["prime"] += days

            summary[client]["total_allowances"] += days * rates.get(stype, 0)

    result = []
    for client, info in summary.items():
        total_days = info["shift_a"] + info["shift_b"] + info["shift_c"] + info["prime"]
        result.append({
            "client_name": client,
            "total_employees": len(info["employees"]),
            "shift_a": info["shift_a"],
            "shift_b": info["shift_b"],
            "shift_c": info["shift_c"],
            "prime": info["prime"],
            "total_days": total_days,
            "total_allowances": info["total_allowances"]
        })

    return result


def get_client_total_allowance_service(db: Session, duration_month: str):

    try:
        month_date = datetime.strptime(duration_month, "%Y-%m")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid duration_month format. Use YYYY-MM")

    records = (
        db.query(ShiftAllowances)
        .filter(
            extract("year", ShiftAllowances.duration_month) == month_date.year,
            extract("month", ShiftAllowances.duration_month) == month_date.month
        )
        .all()
    )

    if not records:
        raise HTTPException(status_code=404, detail=f"No records found for duration_month '{duration_month}'")

    rate_rows = db.query(ShiftsAmount).all()
    rates = {r.shift_type.upper(): float(r.amount) for r in rate_rows}

    client_totals = {}

    for rec in records:
        client = rec.client or "Unknown"
        if client not in client_totals:
            client_totals[client] = {"total_days": 0.0, "total_allowances": 0.0}

        for mapping in rec.shift_mappings:
            stype = mapping.shift_type.upper()
            days = float(mapping.days or 0)
            client_totals[client]["total_days"] += days
            client_totals[client]["total_allowances"] += days * rates.get(stype, 0)

    return [
        VerticalGraphResponse(
            client_name=client,
            total_days=totals["total_days"],
            total_allowances=totals["total_allowances"]
        )
        for client, totals in client_totals.items()
    ]
