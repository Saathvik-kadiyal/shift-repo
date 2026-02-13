"""
Client summary service for month and year-based analytics.

This module aggregates shift allowance data across clients, departments,
employees, and time periods with caching for latest-month queries.

Cache:
- Versioned cache key ensures new code invalidates old cached results.
- Validates cached month vs latest DB month and refreshes automatically.
"""

from __future__ import annotations
from datetime import date, datetime
from typing import List, Dict, Optional, Any

from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_, cast, Integer, extract

from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount
from diskcache import Cache
from utils.shift_config import get_all_shift_keys

cache = Cache("./diskcache/latest_month")

CLIENT_SUMMARY_VERSION = "v2"
LATEST_MONTH_KEY = f"client_summary_latest:{CLIENT_SUMMARY_VERSION}"
CACHE_TTL = 24 * 60 * 60  # 24 hours


# -----------------------------
# Helpers
# -----------------------------
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
            and not payload.get("selected_year")
            and not payload.get("selected_months")
            and not payload.get("emp_id")
            and not payload.get("client_partner")
        )
    )


def validate_year(year: int) -> None:
    """Validate year is not in future or invalid"""
    current_year = date.today().year
    if year <= 0:
        raise HTTPException(400, "selected_year must be greater than 0")
    if year > current_year:
        raise HTTPException(400, "selected_year cannot be in the future")


def parse_yyyy_mm(value: str) -> date:
    """Parse YYYY-MM string to date"""
    try:
        return datetime.strptime(value, "%Y-%m").date().replace(day=1)
    except Exception as exc:
        raise HTTPException(400, "Invalid month format. Expected YYYY-MM") from exc


def normalize_clients(clients_payload: Optional[dict]) -> tuple[Dict[str, list], Dict[str, str], dict]:
    """Normalize clients and departments"""
    normalized_clients: dict = {}
    client_name_map: dict = {}
    dept_name_map: dict = {}

    if not clients_payload or clients_payload == "ALL":
        return normalized_clients, client_name_map, dept_name_map

    if not isinstance(clients_payload, dict):
        raise HTTPException(400, "clients must be 'ALL' or {client: [departments]}")

    for client, depts in clients_payload.items():
        client_clean = clean_str(client)
        client_lc = client_clean.lower()
        client_name_map[client_lc] = client_clean
        normalized_clients[client_lc] = []

        for dept in depts or []:
            dept_clean = clean_str(dept)
            dept_lc = dept_clean.lower()
            dept_name_map[(client_lc, dept_lc)] = dept_clean
            normalized_clients[client_lc].append(dept_lc)

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


# -----------------------------
# Main Service
# -----------------------------
def client_summary_service(db: Session, payload: dict):
    """Return client summary with monthly/year filters"""

    payload = payload or {}
    shift_keys = get_shift_keys()
    shift_key_set = set(shift_keys)

    emp_id = payload.get("emp_id")
    client_partner = payload.get("client_partner")
    selected_year = payload.get("selected_year")
    selected_months = payload.get("selected_months", [])

    use_cache = is_default_latest_month_request(payload)
    latest_ym: Optional[str] = None

    if use_cache:
        try:
            latest_ym = get_latest_month(db).strftime("%Y-%m")
        except HTTPException:
            latest_ym = None

        if latest_ym:
            cached = cache.get(LATEST_MONTH_KEY)
            if cached and isinstance(cached, dict) and "data" in cached:
                if cached.get("_cached_month") == latest_ym:
                    return cached["data"]

    normalized_clients, client_name_map, dept_name_map = normalize_clients(payload.get("clients"))

    months: list[date] = []
    if selected_year:
        validate_year(int(selected_year))
        if selected_months:
            months = [date(int(selected_year), int(m), 1) for m in selected_months]
        else:
            months = [date(int(selected_year), m, 1) for m in range(1, 13)]
    else:
        months = [get_latest_month(db)]

    response: dict = {}
    periods = [m.strftime("%Y-%m") for m in months]
    for period in periods:
        response[period] = {"message": f"No data found for {period}"}

    query = build_base_query(db)

    # Client/Department filters
    if normalized_clients:
        filters = []
        for client_lc, depts_lc in normalized_clients.items():
            if depts_lc:
                filters.append(
                    and_(
                        func.lower(ShiftAllowances.client) == client_lc,
                        func.lower(ShiftAllowances.department).in_(depts_lc),
                    )
                )
            else:
                filters.append(func.lower(ShiftAllowances.client) == client_lc)
        query = query.filter(or_(*filters))

    if emp_id:
        query = query.filter(func.lower(ShiftAllowances.emp_id) == clean_str(emp_id).lower())

    if client_partner:
        col = ShiftAllowances.client_partner
        if isinstance(client_partner, list):
            filters = [
                func.lower(col).like(f"%{clean_str(cp).lower()}%")
                for cp in client_partner
                if isinstance(cp, str) and clean_str(cp)
            ]
            if filters:
                query = query.filter(or_(*filters))
        else:
            query = query.filter(func.lower(col).like(f"%{clean_str(client_partner).lower()}%"))

    query = query.filter(
        or_(
            *[
                and_(
                    extract("year", ShiftAllowances.duration_month) == m.year,
                    extract("month", ShiftAllowances.duration_month) == m.month,
                )
                for m in months
            ]
        )
    )

    rows = query.all()

    for dm, client, dept, eid, ename, cp, stype, days, amt in rows:
        period_key = dm.strftime("%Y-%m")
        if "message" in response.get(period_key, {}):
            response[period_key] = {
                "clients": {},
                "month_total": {
                    "total_head_count": 0,
                    **{k: 0.0 for k in shift_keys},
                    "total_allowance": 0.0,
                },
            }

        month_block = response[period_key]

        client_safe = clean_str(client)
        dept_safe = clean_str(dept)
        cp_safe = clean_str(cp)

        client_name = client_name_map.get(client_safe.lower(), client_safe or "UNKNOWN")
        dept_name = dept_name_map.get((client_safe.lower(), dept_safe.lower()), dept_safe or "UNKNOWN")
        cp_display = cp_safe or "UNKNOWN"

        stype_norm = clean_str(stype).upper()
        if stype_norm not in shift_key_set:
            continue

        total = float(days or 0) * float(amt or 0)

        client_block = month_block["clients"].setdefault(
            client_name,
            {
                **{f"client_{k}": 0.0 for k in shift_keys},
                "departments": {},
                "client_head_count": 0,
                "client_total": 0.0,
                "client_partner": cp_display,
            },
        )

        dept_block = client_block["departments"].setdefault(
            dept_name,
            {
                **{f"dept_{k}": 0.0 for k in shift_keys},
                "dept_total": 0.0,
                "employees": [],
                "dept_head_count": 0,
            },
        )

        employee = next((e for e in dept_block["employees"] if e["emp_id"] == eid), None)
        if not employee:
            employee = {
                "emp_id": eid,
                "emp_name": ename,
                "client_partner": cp_display,
                **{k: 0.0 for k in shift_keys},
                "total": 0.0,
            }
            dept_block["employees"].append(employee)
            dept_block["dept_head_count"] += 1
            client_block["client_head_count"] += 1
            month_block["month_total"]["total_head_count"] += 1

        employee[stype_norm] += total
        employee["total"] += total
        dept_block[f"dept_{stype_norm}"] += total
        dept_block["dept_total"] += total
        client_block[f"client_{stype_norm}"] += total
        client_block["client_total"] += total
        month_block["month_total"][stype_norm] += total
        month_block["month_total"]["total_allowance"] += total

    if use_cache and latest_ym:
        cache.set(
            LATEST_MONTH_KEY,
            {"_cached_month": latest_ym, "data": response},
            expire=CACHE_TTL,
        )

    return response
