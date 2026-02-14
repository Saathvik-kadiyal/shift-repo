"""
Client summary service with multi-year, multi-month, shift, and headcount support.

Features:
- Filters by years, months, clients, departments, employees, client partners, shifts, headcount ranges.
- Headcount range applied at dept level if departments selected, else at client level.
- Validations for years, months, shifts, and headcount formats.
- Caching for latest-month requests.
"""

from __future__ import annotations
from datetime import date, datetime
from typing import List, Dict, Optional, Any, Tuple

from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_, cast, Integer, extract

from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount
from diskcache import Cache
from utils.shift_config import get_all_shift_keys
from dateutil.relativedelta import relativedelta

cache = Cache("./diskcache/latest_month")

CLIENT_SUMMARY_VERSION = "v3"
LATEST_MONTH_KEY = f"client_summary_latest:{CLIENT_SUMMARY_VERSION}"
CACHE_TTL = 24 * 60 * 60  # 24 hours


def clean_str(value: Any) -> str:
    """Normalize strings from DB"""
    if value is None:
        return ""
    s = value.strip() if isinstance(value, str) else str(value).strip()
    s = s.replace("\u200b", "").replace("\u00a0", "").strip()
    for _ in range(2):
        if len(s) >= 2 and ((s[0] == s[-1]) and s[0] in ("'", '"')):
            s = s[1:-1].strip()
    if s in ("'", "''", '"', '""'):
        return ""
    if s.upper() in ("NULL", "NONE", "NAN"):
        return ""
    return s


def get_shift_keys() -> List[str]:
    """Get configured shift keys (uppercase)"""
    return [clean_str(k).upper() for k in get_all_shift_keys()]


def empty_shift_totals(shift_keys: List[str]) -> Dict[str, float]:
    """Zero-initialized shift totals"""
    return {k: 0.0 for k in shift_keys}


def is_default_latest_month_request(payload: dict) -> bool:
    """Check if this is default latest-month summary request"""
    return (
        not payload
        or (
            payload.get("clients") in (None, "ALL")
            and not payload.get("years")
            and not payload.get("months")
            and not payload.get("emp_id")
            and not payload.get("client_partner")
        )
    )


def validate_year(year: int) -> None:
    """Validate year is not in future or invalid"""
    current_year = date.today().year
    if year <= 0:
        raise HTTPException(400, "Year must be greater than 0")
    if year > current_year:
        raise HTTPException(400, "Year cannot be in the future")


def validate_months(months: List[int]) -> None:
    """Validate month integers"""
    for m in months:
        if not 1 <= int(m) <= 12:
            raise HTTPException(400, f"Invalid month: {m}")


def parse_headcount_ranges(headcounts_payload):
    """
    Parses headcount ranges.
    Returns:
      - None  -> means ALL (no filtering)
      - List[(start, end)]
    """
    if headcounts_payload == "ALL":
        return None

    if isinstance(headcounts_payload, str):
        headcounts_payload = [headcounts_payload]

    ranges = []

    for h in headcounts_payload:

        if "-" in h:
            parts = h.split("-")

            if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid headcount range format: {h}. Use '1-5'"
                )

            start = int(parts[0])
            end = int(parts[1])

            if start > end:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid headcount range: {h}"
                )

        elif h.isdigit():
            start = end = int(h)

        else:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid headcount range format: {h}"
            )

        ranges.append((start, end))

    return ranges


def normalize_clients(clients_payload: Optional[Any], depts_payload: Optional[Any]) -> Tuple[Dict[str, list], Dict[str, str], dict]:
    """Normalize clients and departments"""
    normalized_clients: dict = {}
    client_name_map: dict = {}
    dept_name_map: dict = {}

    if not clients_payload or clients_payload == "ALL":
        return normalized_clients, client_name_map, dept_name_map

    if isinstance(clients_payload, str):
        clients_payload = [clients_payload]

    for client in clients_payload:
        client_clean = clean_str(client)
        client_lc = client_clean.lower()
        client_name_map[client_lc] = client_clean

        depts_list = []
        if depts_payload not in (None, "ALL"):
            if isinstance(depts_payload, str):
                depts_payload = [depts_payload]
            for d in depts_payload:
                d_clean = clean_str(d)
                dept_name_map[(client_lc, d_clean.lower())] = d_clean
                depts_list.append(d_clean.lower())
        normalized_clients[client_lc] = depts_list

    return normalized_clients, client_name_map, dept_name_map


def get_latest_month(db: Session) -> date:
    """Fetch latest month from DB"""
    latest = db.query(func.max(ShiftAllowances.duration_month)).scalar()
    if not latest:
        raise HTTPException(404, "No data available in database")
    return date(latest.year, latest.month, 1)


def build_base_query(db: Session):
    """Base SQLAlchemy query"""
    return (
        db.query(
            ShiftAllowances.duration_month,
            ShiftAllowances.client,
            ShiftAllowances.department,
            ShiftAllowances.emp_id,
            ShiftAllowances.emp_name,
            ShiftAllowances.client_partner,
            ShiftMapping.shift_type,
            ShiftMapping.days,
            ShiftsAmount.amount,
        )
        .join(ShiftMapping, ShiftMapping.shiftallowance_id == ShiftAllowances.id)
        .outerjoin(
            ShiftsAmount,
            and_(
                ShiftMapping.shift_type == ShiftsAmount.shift_type,
                cast(ShiftsAmount.payroll_year, Integer)
                == extract("year", ShiftAllowances.duration_month),
            ),
        )
    )


def client_summary_service(db: Session, payload: dict):
    """Return client summary with multi-year, multi-month, shift, and headcount filters with sorting"""

    payload = payload or {}
    shift_keys = get_shift_keys()
    shift_key_set = set(shift_keys)

   
    clients_raw = payload.get("clients", "ALL")
    departments_raw = payload.get("departments", "ALL")
    emp_id = payload.get("emp_id")
    client_partner = payload.get("client_partner")
    selected_years = payload.get("years", [])
    selected_months = payload.get("months", [])
    shifts = payload.get("shifts", "ALL")
    headcounts_payload = payload.get("headcounts", "ALL")
    sort_by = payload.get("sort_by", "total_allowance")
    sort_order = payload.get("sort_order", "default")

 
    if isinstance(clients_raw, str):
        clients_list = [c.strip() for c in clients_raw.split(",") if c.strip()]
    elif isinstance(clients_raw, list):
        clients_list = [c.strip() for c in clients_raw if c]
    else:
        clients_list = []

    if isinstance(departments_raw, str):
        departments_list = [departments_raw.strip()] if departments_raw != "ALL" else []
    elif isinstance(departments_raw, list):
        departments_list = [d.strip() for d in departments_raw if d]
    else:
        departments_list = []

    departments_selected = bool(departments_list)

    if selected_years:
        for y in selected_years:
            validate_year(int(y))
    if selected_months:
        validate_months(selected_months)


    if shifts != "ALL":
        if isinstance(shifts, str):
            shifts = [shifts]
        shifts_upper = [clean_str(s).upper() for s in shifts]
        invalid_shifts = [s for s in shifts_upper if s not in shift_key_set]
        if invalid_shifts:
            raise HTTPException(400, f"Invalid shift(s): {invalid_shifts}")
        shift_key_set = set(shifts_upper)

   
    headcount_ranges = parse_headcount_ranges(headcounts_payload)

    months_to_use: List[date] = []

    if not selected_years and not selected_months:
        # No year/month -> try current month
        current_month_date = date.today().replace(day=1)
        query_current = db.query(ShiftAllowances)
        if clients_list:
            query_current = query_current.filter(
                or_(*[ShiftAllowances.client.ilike(f"%{c}%") for c in clients_list])
            )
        current_exists = query_current.filter(ShiftAllowances.duration_month == current_month_date).first()
        if current_exists:
            months_to_use = [current_month_date]
        else:
            # fallback: latest 12 months in DB
            latest_dm = db.query(func.max(ShiftAllowances.duration_month)).scalar()
            if latest_dm:
                latest_start = latest_dm.replace(day=1) - relativedelta(months=11)
                months_to_use = [
                    (latest_start + relativedelta(months=i)) for i in range(12)
                ]
            else:
                months_to_use = [current_month_date]
    elif selected_months and not selected_years:
        current_year = date.today().year
        months_to_use = [date(current_year, int(m), 1) for m in selected_months]
    elif selected_years and not selected_months:
        # full year -> months 1-12
        months_to_use = [date(int(y), m, 1) for y in selected_years for m in range(1, 13)]
    else:
        # both year and month selected
        months_to_use = [date(int(y), int(m), 1) for y in selected_years for m in selected_months]

 
    response: dict = {m.strftime("%Y-%m"): {"message": f"No data found for {m.strftime('%Y-%m')}"} for m in months_to_use}

    query = build_base_query(db)

    # Filter clients
    if clients_list:
        client_filters = []
        for c in clients_list:
            c_lower = c.lower()
            if departments_selected:
                depts_lower = [d.lower() for d in departments_list]
                client_filters.append(
                    and_(
                        func.lower(ShiftAllowances.client) == c_lower,
                        func.lower(ShiftAllowances.department).in_(depts_lower)
                    )
                )
            else:
                client_filters.append(func.lower(ShiftAllowances.client) == c_lower)
        query = query.filter(or_(*client_filters))

    # Filter emp_id
    if emp_id:
        if isinstance(emp_id, str):
            emp_id = [emp_id]
        query = query.filter(func.lower(ShiftAllowances.emp_id).in_([clean_str(e).lower() for e in emp_id]))

    # Filter client_partner
    if client_partner:
        col = ShiftAllowances.client_partner
        if isinstance(client_partner, list):
            filters = [
                func.lower(col).like(f"%{clean_str(cp).lower()}%")
                for cp in client_partner if clean_str(cp)
            ]
            if filters:
                query = query.filter(or_(*filters))
        else:
            query = query.filter(func.lower(col).like(f"%{clean_str(client_partner).lower()}%"))

    # Filter months
    query = query.filter(
        or_(*[and_(extract("year", ShiftAllowances.duration_month) == m.year,
                   extract("month", ShiftAllowances.duration_month) == m.month) for m in months_to_use])
    )

    rows = query.all()

    for dm, client, dept, eid, ename, cp, stype, days, amt in rows:
        stype_norm = clean_str(stype).upper()
        if stype_norm not in shift_key_set:
            continue

        period_key = dm.strftime("%Y-%m")
        if "message" in response.get(period_key, {}):
            response[period_key] = {
                "clients": {},
                "month_total": {
                    "total_head_count": 0,
                    **{k: 0.0 for k in shift_keys},
                    "total_allowance": 0.0,
                }
            }

        month_block = response[period_key]
        client_safe = clean_str(client)
        dept_safe = clean_str(dept)
        cp_safe = clean_str(cp)

        client_block = month_block["clients"].setdefault(
            client_safe,
            {
                **{k: 0.0 for k in shift_keys},
                "departments": {},
                "client_head_count": 0,
                "client_total": 0.0,
                "client_partner": cp_safe or "UNKNOWN",
            }
        )

        dept_block = client_block["departments"].setdefault(
            dept_safe,
            {
                **{k: 0.0 for k in shift_keys},
                "dept_total": 0.0,
                "employees": [],
                "dept_head_count": 0,
            }
        )

        employee = next((e for e in dept_block["employees"] if e["emp_id"] == eid), None)
        if not employee:
            prospective_dept_headcount = dept_block["dept_head_count"] + 1
            prospective_client_headcount = client_block["client_head_count"] + 1
            total_headcount_for_check = prospective_dept_headcount if departments_selected else prospective_client_headcount

            passes_headcount = True if headcount_ranges is None else any(
                start <= total_headcount_for_check <= end for start, end in headcount_ranges
            )

            if not passes_headcount:
                continue

            employee = {
                "emp_id": eid,
                "emp_name": ename,
                "client_partner": cp_safe or "UNKNOWN",
                **{k: 0.0 for k in shift_keys},
                "total": 0.0,
            }
            dept_block["employees"].append(employee)
            dept_block["dept_head_count"] += 1
            client_block["client_head_count"] += 1
            month_block["month_total"]["total_head_count"] += 1

        employee[stype_norm] += float(days or 0) * float(amt or 0)
        employee["total"] += float(days or 0) * float(amt or 0)
        dept_block[stype_norm] += float(days or 0) * float(amt or 0)
        dept_block["dept_total"] += float(days or 0) * float(amt or 0)
        client_block[stype_norm] += float(days or 0) * float(amt or 0)
        client_block["client_total"] += float(days or 0) * float(amt or 0)
        month_block["month_total"][stype_norm] += float(days or 0) * float(amt or 0)
        month_block["month_total"]["total_allowance"] += float(days or 0) * float(amt or 0)

   
    for period, pdata in response.items():
        clients_data = list(pdata.get("clients", {}).values())
        if sort_by.lower() == "head_count":
            clients_data.sort(key=lambda x: x.get("client_head_count", 0), reverse=(sort_order=="desc"))
        elif sort_by.lower() == "client":
            clients_data.sort(key=lambda x: x.get("client") or "", reverse=(sort_order=="desc"))
        elif sort_by.lower() == "client_partner":
            clients_data.sort(key=lambda x: x.get("client_partner") or "", reverse=(sort_order=="desc"))
        elif sort_by.lower() == "departments":
            clients_data.sort(key=lambda x: len(x.get("departments", {})), reverse=(sort_order=="desc"))
        elif sort_by.lower() == "total_allowance":
            clients_data.sort(key=lambda x: x.get("client_total", 0.0), reverse=(sort_order=="desc"))
        pdata["clients"] = {c.get("emp_id", c.get("client")): c for c in clients_data}

    return response
