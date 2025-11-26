import pandas as pd
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func
from models.models import ShiftAllowances, ShiftsAmount
 
 
def export_filtered_excel(db: Session, emp_id: str | None, account_manager: str | None):
 
    # Base query
    query = (
        db.query(
            ShiftAllowances.emp_id,
            func.min(ShiftAllowances.emp_name).label("emp_name"),
            func.min(ShiftAllowances.grade).label("grade"),
            func.min(ShiftAllowances.department).label("department"),
            func.min(ShiftAllowances.client).label("client"),
            func.min(ShiftAllowances.project).label("project"),
            func.min(ShiftAllowances.project_code).label("project_code"),
            func.min(ShiftAllowances.account_manager).label("account_manager")
        )
        .group_by(ShiftAllowances.emp_id)
    )
 
    if emp_id:
        query = query.filter(ShiftAllowances.emp_id.ilike(f"%{emp_id}%"))
   
 
    if account_manager:
        query = query.filter(ShiftAllowances.account_manager.ilike(f"%{account_manager}%"))
 
 
    rows = query.all()
 
    if not rows:
        raise HTTPException(
            status_code=404,
            detail="No data found for the given emp_id or account_manager"
        )
 
    # Prepare final response
    final_data = []
 
    for row in rows:
        row_dict = row._asdict()
 
        # Convert numpy types → python
        clean_row = {
            k: (v.item() if hasattr(v, "item") else v)
            for k, v in row_dict.items()
        }
 
        final_data.append(clean_row)
 
    df = pd.DataFrame(final_data)
    return df.to_dict(orient="records")