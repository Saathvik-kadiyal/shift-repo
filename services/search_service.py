from sqlalchemy.orm import Session
from sqlalchemy import func
from models.models import ShiftAllowances, ShiftMapping

ALLOWANCE_MAP = {
    "A": 500,
    "B": 350,
    "C": 100,
    "PRIME": 700
}

def get_employee_details(db: Session, emp_id: str | None, account_manager: str | None):
    """
    Fetch all employee details with shift types and total allowances.
    """

    query = (
        db.query(
            ShiftAllowances.id,
            ShiftAllowances.emp_id,
            ShiftAllowances.emp_name,
            ShiftAllowances.grade,
            ShiftAllowances.department,
            ShiftAllowances.client,
            ShiftAllowances.project,
            ShiftAllowances.project_code,
            ShiftAllowances.account_manager,
            ShiftAllowances.practice_lead,
            ShiftAllowances.delivery_manager,
            ShiftAllowances.duration_month,
            ShiftAllowances.payroll_month,
            ShiftAllowances.billability_status,
            ShiftAllowances.practice_remarks,
            ShiftAllowances.rmg_comments,
            func.array_agg(ShiftMapping.shift_type).label("shift_types")
        )
        .outerjoin(ShiftMapping, ShiftAllowances.id == ShiftMapping.shiftallowance_id)
        .group_by(ShiftAllowances.id)
    )

    # Filters applied if provided
    if emp_id:
        query = query.filter(ShiftAllowances.emp_id == emp_id)

    if account_manager:
        query = query.filter(ShiftAllowances.account_manager == account_manager)

    results = query.all()

    final_output = []

    for r in results:
        shift_list = r.shift_types if r.shift_types else []

        total_allowances = sum(
            ALLOWANCE_MAP.get(s.upper(), 0) for s in shift_list
        )

        final_output.append({
            "id": r.id,
            "emp_id": r.emp_id,
            "emp_name": r.emp_name,
            "grade": r.grade,
            "department": r.department,
            "client": r.client,
            "project": r.project,
            "project_code": r.project_code,
            "account_manager": r.account_manager,
            "practice_lead": r.practice_lead,
            "delivery_manager": r.delivery_manager,
            "duration_month": r.duration_month,
            "payroll_month": r.payroll_month,
            "billability_status": r.billability_status,
            "practice_remarks": r.practice_remarks,
            "rmg_comments": r.rmg_comments,
            "shift_types": shift_list,
            "total_allowances": total_allowances
        })

    return final_output
