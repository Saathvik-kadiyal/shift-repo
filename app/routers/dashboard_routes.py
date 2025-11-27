from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from schemas.dashboardschema import VerticalGraphResponse, PieChartClientShift
from db import get_db
from utils.dependencies import get_current_user
from services.dashboard_service import (
    get_horizontal_bar_service, 
    get_graph_service,
    get_all_clients_service,
    get_client_total_allowance_service,
    get_piechart_shift_summary
)
from typing import List

router = APIRouter(prefix="/dashboard")

@router.get("/horizontal-bar")
def horizontal_bar(
    duration_month: str,
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user)
):
    return get_horizontal_bar_service(db, duration_month)


@router.get("/graph")
def graph(
    client_name: str,
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user)
):
    return get_graph_service(db, client_name)


@router.get("/clients")
def get_clients(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user)
):
    return get_all_clients_service(db)


@router.get("/piechart", response_model=list[PieChartClientShift])
def piechart(
    duration_month: str,
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user)
):
    return get_piechart_shift_summary(db, duration_month)


@router.get("/vertical-graph", response_model=List[VerticalGraphResponse])
def vertical_graph(
    duration_month: str,
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user)
):
    return get_client_total_allowance_service(db, duration_month)
