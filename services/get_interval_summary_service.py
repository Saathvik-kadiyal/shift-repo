"""
Interval summary service.

This module provides functionality to generate month-wise interval summaries
for client shift data. It supports flexible date ranges, optional account
manager filtering, and graceful handling of missing data by returning
descriptive messages instead of failing requests.

The service is designed for reporting and analytics use cases where
continuous month-to-month visibility is required, even when data for
certain months is unavailable.
"""

import re
from datetime import datetime
from dateutil.relativedelta import relativedelta
from fastapi import HTTPException
from sqlalchemy.orm import Session
from models.models import ShiftAllowances
from services.summary_service import get_client_shift_summary


def get_interval_summary_service(
    db: Session,
    start_month: str | None = None,
    end_month: str | None = None,
    account_manager: str | None = None
):
    """
    Build an interval-based client shift summary between two months.

    This service returns a month-wise summary for the given interval.
    Missing months are handled gracefully and returned with
    descriptive messages instead of failing the request.

    Args:
        db (Session): Active SQLAlchemy database session.
        start_month (str | None): Start month in YYYY-MM format.
        end_month (str | None): End month in YYYY-MM format.
        account_manager (str | None): Optional account manager filter.

    Returns:
        dict: Mapping of YYYY-MM -> summary data or error messages.
    """


    # ACCOUNT MANAGER VALIDATION

    if account_manager:
        if account_manager != account_manager.strip():
            raise HTTPException(status_code=400,
                                detail="Spaces are not allowed at start/end of account_manager")

        if not all(x.isalpha() or x.isspace() for x in account_manager):
            raise HTTPException(status_code=400,
                                detail="Account manager must contain only letters and spaces")


        manager_exists = db.query(ShiftAllowances).filter(
            ShiftAllowances.account_manager.ilike(f"%{account_manager}%")
        ).first()

        if not manager_exists:
            raise HTTPException(
                status_code=404,
                detail=f"Account manager '{account_manager}' not found"
            )


    # START MONTH VALIDATION

    if start_month:
        if " " in start_month:
            raise HTTPException(status_code=400, detail="Spaces are not allowed in start_month")

        if not re.match(r"^\d{4}-\d{2}$", start_month):
            raise HTTPException(status_code=400, detail="Invalid start_month format. Use YYYY-MM")

        # DO NOT CHECK IF MONTH EXISTS — interval will handle missing months
        year, month = map(int, start_month.split("-"))


    # END MONTH VALIDATION

    if end_month:
        if not re.match(r"^\d{4}-\d{2}$", end_month):
            raise HTTPException(status_code=400, detail="Invalid end_month format. Use YYYY-MM")


    # FUNCTION TO PICK NEAREST MONTH IF BOTH ARE EMPTY

    def get_nearest_month(before: datetime):
        """
        Fetch the most recent available duration_month
        less than or equal to the given date.

        Args:
            before (datetime): Upper bound date.

        Returns:
            date | None: Nearest available month or None if no data exists.
        """
        query = db.query(ShiftAllowances.duration_month)
        if account_manager:
            query = query.filter(ShiftAllowances.account_manager.ilike(f"%{account_manager}%"))

        month = query.filter(
            ShiftAllowances.duration_month <= before
        ).order_by(ShiftAllowances.duration_month.desc()).first()

        return month[0] if month else None


    # DETERMINE START & END

    if not start_month and not end_month:
        current_month = datetime.today().replace(day=1).date()
        nearest = get_nearest_month(current_month)
        if not nearest:
            raise HTTPException(status_code=404,
                                detail="No records found for current or previous months")
        start = end = nearest

    elif start_month and not end_month:
        start = end = datetime.strptime(start_month + "-01", "%Y-%m-%d").date()

    elif end_month and not start_month:
        start = end = datetime.strptime(end_month + "-01", "%Y-%m-%d").date()

    else:
        start = datetime.strptime(start_month + "-01", "%Y-%m-%d").date()
        end = datetime.strptime(end_month + "-01", "%Y-%m-%d").date()

        if start > end:
            raise HTTPException(status_code=400,
                                detail="start_month cannot be after end_month")

    # BUILD INTERVAL SUMMARY

    interval_summary = {}
    current = start

    while current <= end:
        month_str = current.strftime("%Y-%m")

        try:
            month_summary = get_client_shift_summary(
                db,
                duration_month=month_str,
                account_manager=account_manager
            )

            # Unwrap {"YYYY-MM": [...]}
            if isinstance(month_summary, dict) and month_str in month_summary:
                month_summary = month_summary[month_str]

            if not month_summary:
                month_summary = [f"No records found for month '{month_str}'"]

        except HTTPException as e:
            # Instead of stopping, add error message inside output
            month_summary = [str(e.detail)]

        interval_summary[month_str] = month_summary
        current += relativedelta(months=1)

    return interval_summary
