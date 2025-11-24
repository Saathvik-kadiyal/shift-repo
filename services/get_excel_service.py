import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import func
from models.models import ShiftAllowances, ShiftMapping

# Allowance mapping
ALLOWANCE_MAP = {
    "A": 500,
    "B": 350,
    "C": 100,
    "PRIME": 700
}

def export_filtered_excel(db: Session, emp_id: str | None, account_manager: str | None):
    """
    1. If both filters empty → return full Excel
    2. If filters applied and no result → return full Excel
    3. Else → return filtered data
    """

    query = (
        db.query(
            ShiftAllowances.emp_id,
            ShiftAllowances.emp_name,
            ShiftAllowances.grade,
            ShiftAllowances.department,
            ShiftAllowances.client,
            ShiftAllowances.project,
            ShiftAllowances.project_code,
            ShiftAllowances.account_manager,
            func.array_agg(ShiftMapping.shift_type).label("shift_type"),
            ShiftAllowances.delivery_manager,
            ShiftAllowances.practice_lead,
            ShiftAllowances.billability_status,
            ShiftAllowances.practice_remarks,
            ShiftAllowances.rmg_comments,
            ShiftAllowances.duration_month,
            ShiftAllowances.payroll_month
        )
        .outerjoin(ShiftMapping, ShiftAllowances.id == ShiftMapping.shiftallowance_id)
        .group_by(ShiftAllowances.id)
    )

    # No filters → full data
    if not emp_id and not account_manager:
        rows = query.all()
    else:
        if emp_id:
            query = query.filter(ShiftAllowances.emp_id == emp_id)
        if account_manager:
            query = query.filter(ShiftAllowances.account_manager == account_manager)

        rows = query.all()

        # If filters return no records → full data again
        if not rows:
            rows = query.all()

    # Convert rows to dict & calculate allowances
    final_data = []

    for row in rows:
        shift_list = row.shift_type if row.shift_type else []

        total_allowances = sum(
            ALLOWANCE_MAP.get(s.upper(), 0) for s in shift_list
        )

        row_data = row._asdict()
        row_data["total_allowances"] = total_allowances

        final_data.append(row_data)

    return pd.DataFrame(final_data)
