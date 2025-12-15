from datetime import date
from typing import List, Dict
import re

from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_

from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount




def normalize_int(value, field: str) -> int:
    if value is None:
        raise HTTPException(400, f"{field} is required")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    raise HTTPException(400, f"Invalid {field} (expected numeric)")


def validate_year(year: int):
    current_year = date.today().year
    if year <= 0:
        raise HTTPException(400, "selected_year must be greater than 0")
    if year > current_year:
        raise HTTPException(400, "selected_year cannot be in the future")


def parse_year_month(value: str, field: str) -> date:
    if not isinstance(value, str) or not re.match(r"^\d{4}-\d{2}$", value):
        raise HTTPException(400, f"{field} must be in YYYY-MM format")
    year, month = map(int, value.split("-"))
    if not 1 <= month <= 12:
        raise HTTPException(400, f"{field} month must be between 01 and 12")
    return date(year, month, 1)


def quarter_to_months(q: str) -> List[int]:
    mapping = {
        "Q1": [1, 2, 3],
        "Q2": [4, 5, 6],
        "Q3": [7, 8, 9],
        "Q4": [10, 11, 12],
    }
    q = q.upper().strip()
    if q not in mapping:
        raise HTTPException(400, "Invalid quarter (expected Q1–Q4)")
    return mapping[q]


def empty_shift_totals():
    return {"A": 0, "B": 0, "C": 0, "PRIME": 0}




def client_summary_service(db: Session, payload: dict):

    payload = payload or {}
    clients_payload = payload.get("clients")

    selected_quarters = payload.get("selected_quarters", [])
    is_quarter_selection = bool(selected_quarters)

    requested_quarters = set(q.upper() for q in selected_quarters)
    quarter_month_map: Dict[int, str] = {}
    quarter_label_map: Dict[str, str] = {}

    no_date_filters_selected = (
        not payload.get("selected_year")
        and not payload.get("selected_months")
        and not payload.get("selected_quarters")
        and not payload.get("start_month")
        and not payload.get("end_month")
    )

    
    if no_date_filters_selected:

        latest_date = db.query(func.max(ShiftAllowances.duration_month)).scalar()
        if not latest_date:
            return {"message": "No records found"}

        selected_year = latest_date.year
        months = [latest_date.month]

   
    else:

        start_month_raw = payload.get("start_month")
        end_month_raw = payload.get("end_month")

        if start_month_raw and end_month_raw:
            start_month = parse_year_month(start_month_raw, "start_month")
            end_month = parse_year_month(end_month_raw, "end_month")
            if start_month > end_month:
                raise HTTPException(400, "start_month cannot be greater than end_month")

            selected_year = start_month.year
            months = list(range(start_month.month, end_month.month + 1))

        else:
            selected_year = normalize_int(payload.get("selected_year"), "selected_year")
            validate_year(selected_year)

            months = []

            for q in selected_quarters:
                q = q.upper()
                q_months = quarter_to_months(q)
                months.extend(q_months)

                label = f"{selected_year}-{q_months[0]:02d} - {selected_year}-{q_months[-1]:02d}"
                quarter_label_map[q] = label

                for m in q_months:
                    quarter_month_map[m] = label

            for m in payload.get("selected_months", []):
                mi = normalize_int(m, "selected_months")
                if not 1 <= mi <= 12:
                    raise HTTPException(400, "Month must be between 1 and 12")
                months.append(mi)

        months = sorted(set(months))
        if not months:
            raise HTTPException(400, "No valid months selected")

    
    normalized_clients = {}
    if isinstance(clients_payload, dict):
        for client, depts in clients_payload.items():
            normalized_clients[client.strip().lower()] = [
                d.strip() for d in (depts or []) if d
            ]

  

    rows = (
        db.query(
            ShiftAllowances.duration_month,
            ShiftAllowances.client,
            ShiftAllowances.department,
            ShiftAllowances.emp_id,
            ShiftAllowances.emp_name,
            ShiftAllowances.account_manager,
            ShiftMapping.shift_type,
            ShiftMapping.days,
            ShiftsAmount.amount,
        )
        .join(ShiftMapping, ShiftMapping.shiftallowance_id == ShiftAllowances.id)
        .join(
            ShiftsAmount,
            and_(
                ShiftsAmount.shift_type == ShiftMapping.shift_type,
                ShiftsAmount.payroll_year ==
                func.to_char(ShiftAllowances.payroll_month, "YYYY"),
            ),
        )
        .filter(
            func.extract("year", ShiftAllowances.duration_month) == selected_year,
            func.extract("month", ShiftAllowances.duration_month).in_(months),
        )
        .all()
    )

    if not rows:
        return {"message": "No records found"}

   

    response = {}
    found_quarters = set()

    for dm, client, dept, emp_id, emp_name, acc_mgr, stype, days, amt in rows:

        month_no = dm.month

        if is_quarter_selection and month_no in quarter_month_map:
            month_key = quarter_month_map[month_no]

            for q, label in quarter_label_map.items():
                if label == month_key:
                    found_quarters.add(q)
        else:
            month_key = dm.strftime("%Y-%m")

        client_key = client.strip()
        dept_key = dept.strip()

        if normalized_clients:
            if client_key.lower() not in normalized_clients:
                continue
            if normalized_clients[client_key.lower()] and dept_key not in normalized_clients[client_key.lower()]:
                continue

        total = float(days or 0) * float(amt or 0)

        month_block = response.setdefault(month_key, {
            "clients": {},
            "month_total": {
                "total_head_count": 0,
                **empty_shift_totals(),
                "total_allowance": 0
            }
        })

        client_block = month_block["clients"].setdefault(client_key, {
            **{f"client_{k}": 0 for k in ["A", "B", "C", "PRIME"]},
            "departments": {},
            "client_head_count": 0,
            "client_total": 0
        })

        dept_block = client_block["departments"].setdefault(dept_key, {
            **{f"dept_{k}": 0 for k in ["A", "B", "C", "PRIME"]},
            "dept_total": 0,
            "employees": [],
            "dept_head_count": 0
        })

        emp = next((e for e in dept_block["employees"] if e["emp_id"] == emp_id), None)
        if not emp:
            emp = {
                "emp_id": emp_id,
                "emp_name": emp_name,
                "account_manager": acc_mgr,
                **empty_shift_totals(),
                "total": 0
            }
            dept_block["employees"].append(emp)
            dept_block["dept_head_count"] += 1
            client_block["client_head_count"] += 1
            month_block["month_total"]["total_head_count"] += 1

        emp[stype] += total
        emp["total"] += total
        dept_block[f"dept_{stype}"] += total
        dept_block["dept_total"] += total
        client_block[f"client_{stype}"] += total
        client_block["client_total"] += total
        month_block["month_total"][stype] += total
        month_block["month_total"]["total_allowance"] += total

    if is_quarter_selection:
        missing_quarters = requested_quarters - found_quarters

        for q in sorted(missing_quarters):
            quarter_label = quarter_label_map.get(q)
            if quarter_label and quarter_label not in response:
                response[quarter_label] = {}

        if missing_quarters:
            response["message"] = (
                f"No data found for quarters: {', '.join(sorted(missing_quarters))}"
            )

    return response
