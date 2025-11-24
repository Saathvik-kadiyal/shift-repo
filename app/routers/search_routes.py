from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from db import get_db
from services.search_service import get_employee_details

router = APIRouter(prefix="/employee-details", tags=["Search Details"])


@router.get("/Search")
def fetch_employee_details(
    emp_id: str | None = Query(None, description="Search by Employee ID"),
    account_manager: str | None = Query(None, description="Search by Account Manager"),
    db: Session = Depends(get_db)
):
    """
    Fetch ALL employee details.
    Filters allowed: emp_id, account_manager.
    If both empty → return all employees.
    """

    data = get_employee_details(db, emp_id, account_manager)

    return {
        "total": len(data),
        "data": data
    }
