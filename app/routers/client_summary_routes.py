from fastapi import APIRouter, Depends, Body
from sqlalchemy.orm import Session
from utils.dependencies import get_current_user

from db import get_db
from services.client_summary_service import client_summary_service

router = APIRouter(
    prefix="/client-summary",
    tags=["Client Summary"]
)

@router.post("")
def client_summary(
    payload: dict = Body(
        ...,
        example={
            "clients": "ALL",
            "start_month": "YYYY-MM",
            "end_month": "YYYY-MM",
            "selected_year": "YYYY",
            "selected_months": ["01", "02"],
            "selected_quarters": ["Q1"]
        }
    ),
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user)
):
    return client_summary_service(db=db, payload=payload)
