
from fastapi import APIRouter, Depends, Body
from sqlalchemy.orm import Session
from db import get_db
from utils.dependencies import get_current_user
from services.client_summary_service import client_summary_service

router = APIRouter(
    prefix="/client-summary",
    tags=["Client Summary"],
)

@router.post("/")
def client_summary(
    payload: dict = Body(
        ...,
        example={
            "years": [2025, 2026],         # Multi-year
            "months": [1, 2, 3],           # Multi-month (Cartesian years × months; future months in current year excluded)
            "clients": "ALL",              # or "Foo", or ["Foo","Bar"]
            "departments": "ALL",          # or "Dept A", or ["Dept A","Dept B"]
            "emp_id": ["IN01804611"],      # optional: string or list[str]
            "client_partner": ["John Doe"],# optional: string or list[str]
            "shifts": "US_INDIA",          # "ALL", string, or list[str]; validated against configured keys
            "headcounts": "1-10"           # "ALL", "n", "a-b", or list like ["1-10","11-20","25"]
        },
    ),
    db: Session = Depends(get_db),
    _current_user=Depends(get_current_user)
):
    return client_summary_service(db=db, payload=payload)
