"""
Employee search API routes.

This module defines API endpoints for searching and retrieving employee
shift and allowance details with optional filtering by employee ID,
account manager, department, client, and month range. The endpoints
support pagination and are secured using authentication dependencies.
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from db import get_db
from services.search_service import export_filtered_excel
from utils.dependencies import get_current_user

router = APIRouter(
    prefix="/employee-details",
    tags=["Search Details"]
)

# pylint: disable=too-many-arguments,too-many-positional-arguments
@router.get("/search")
def fetch_employee_details(
    emp_id: str | None = Query(None),
    account_manager: str | None = Query(None),
    department: str | None = Query(None),
    client: str | None = Query(None),
    start_month: str | None = Query(None, description="YYYY-MM"),
    end_month: str | None = Query(None, description="YYYY-MM"),
    start: int | None = Query(0, ge=0),
    limit: int | None = Query(10, gt=0),
    db: Session = Depends(get_db),
    _current_user=Depends(get_current_user),
):
    """
    Fetch employee shift details with flexible search filters.

    This endpoint returns employee-wise shift and allowance information
    based on the provided query parameters. All filters are optional and
    can be combined as needed. Pagination is supported using `start`
    and `limit`.

    Args:
        emp_id (str | None): Filter by employee ID (partial match).
        account_manager (str | None): Filter by account manager name.
        department (str | None): Filter by department name.
        client (str | None): Filter by client name.
        start_month (str | None): Start month in YYYY-MM format.
        end_month (str | None): End month in YYYY-MM format.
        start (int): Pagination offset (default 0).
        limit (int): Maximum number of records to return (default 10).
        db (Session): Database session dependency.
        current_user: Authenticated user context.

    Returns:
        dict: Paginated employee shift details with overall summaries.

    Raises:
        HTTPException:
            - 400 for invalid query parameters.
            - 404 if no matching records are found.
    """

    return export_filtered_excel(
        db=db,
        emp_id=emp_id,
        account_manager=account_manager,
        department=department,
        client=client,
        start_month=start_month,
        end_month=end_month,
        start=start,
        limit=limit,
    )
