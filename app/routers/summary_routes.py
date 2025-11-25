from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from db import get_db
from utils.dependencies import get_current_user
from services.summary_service import get_client_shift_summary
from schemas.displayschema import ClientSummary

router = APIRouter(prefix="/summary", tags=["Summary"])


@router.get("/client-shift-summary", response_model=list[ClientSummary])
def client_shift_summary(
    payroll_month: str,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):

    if not payroll_month:
        raise HTTPException(
            status_code=400,
            detail="payroll_month is required. Example: 2025-01"
        )

    summary = get_client_shift_summary(db, payroll_month)

    if not summary:
        raise HTTPException(
            status_code=404,
            detail=f"No records found for payroll month {payroll_month}"
        )

    return summary
