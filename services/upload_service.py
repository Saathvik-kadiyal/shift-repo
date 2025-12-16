import os
import uuid
import io
import pandas as pd
import re
from datetime import datetime, date
from fastapi import HTTPException
from sqlalchemy.orm import Session
from models.models import (
    UploadedFiles,
    ShiftAllowances,
    ShiftMapping,
    ShiftsAmount,
)
from utils.enums import ExcelColumnMap


TEMP_FOLDER = "media/error_excels"
os.makedirs(TEMP_FOLDER, exist_ok=True)

MONTH_MAP = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
    "May": 5, "Jun": 6, "Jul": 7, "Aug": 8,
    "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
}


def make_json_safe(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [make_json_safe(i) for i in obj]
    return obj


def parse_month_format(value: str):
    if not isinstance(value, str):
        return None
    try:
        m, y = value.split("'")
        return datetime(2000 + int(y), MONTH_MAP[m.title()], 1).date()
    except Exception:
        return None


def load_shift_rates(db: Session) -> dict:
    
    rates = {}
    rows = db.query(ShiftsAmount).all()
    for r in rows:
        if r.shift_type:
            rates[r.shift_type.upper()] = float(r.amount or 0)
    return rates


def delete_existing_emp_month(db: Session, emp_id, duration_month, payroll_month):
    existing = (
        db.query(ShiftAllowances)
        .filter(
            ShiftAllowances.emp_id == emp_id,
            ShiftAllowances.duration_month == duration_month,
            ShiftAllowances.payroll_month == payroll_month,
        )
        .all()
    )

    for rec in existing:
        db.query(ShiftMapping).filter(
            ShiftMapping.shiftallowance_id == rec.id
        ).delete()
        db.delete(rec)

    db.flush()



def validate_excel_data(df: pd.DataFrame):
    errors = []
    error_rows = []

    month_pattern = re.compile(
        r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'[0-9]{2}$"
    )

    for idx, row in df.iterrows():
        row_errors = []

        for col in [
            "shift_a_days", "shift_b_days",
            "shift_c_days", "prime_days", "total_days"
        ]:
            try:
                df.at[idx, col] = float(row.get(col, 0))
            except Exception:
                row_errors.append(f"Invalid numeric value in '{col}'")

        for col in ["duration_month", "payroll_month"]:
            val = str(row.get(col, "")).strip()
            if val and not month_pattern.match(val):
                row_errors.append(f"Invalid month format in '{col}'")

        try:
            total = (
                df.at[idx, "shift_a_days"]
                + df.at[idx, "shift_b_days"]
                + df.at[idx, "shift_c_days"]
                + df.at[idx, "prime_days"]
            )
            if total != df.at[idx, "total_days"]:
                row_errors.append("Total days do not match sum of shifts")
        except Exception:
            pass

        if row_errors:
            r = row.to_dict()
            r["error"] = "; ".join(row_errors)
            error_rows.append(r)
            errors.append(idx)

    clean_df = df.drop(index=errors).reset_index(drop=True)
    error_df = pd.DataFrame(error_rows) if error_rows else None

    return clean_df, error_df


def normalize_error_rows(error_rows):
    normalized = []

    for row in error_rows:
        r = dict(row)
        err_text = r.pop("error", "")
        reason = {}

        for err in err_text.split(";"):
            err = err.strip()
            if "Invalid numeric value" in err:
                col = err.split("'")[1]
                reason[col] = "Expected numeric value"
            elif "Invalid month format" in err:
                if "duration_month" in err:
                    reason["duration_month"] = "Expected Jan'24"
                elif "payroll_month" in err:
                    reason["payroll_month"] = "Expected Jan'24"
            elif "Total days do not match" in err:
                reason["total_days"] = "Shift days mismatch"

        r["reason"] = reason
        normalized.append(r)

    return normalized



async def process_excel_upload(file, db: Session, user, base_url: str):

    if not file.filename.endswith((".xls", ".xlsx")):
        raise HTTPException(status_code=400, detail="Only Excel files allowed")

    uploaded_file = UploadedFiles(
        filename=file.filename,
        uploaded_by=user.id,
        status="processing"
    )
    db.add(uploaded_file)
    db.commit()
    db.refresh(uploaded_file)

    try:
        df = pd.read_excel(io.BytesIO(await file.read()))
        df.rename(columns={e.value: e.name for e in ExcelColumnMap}, inplace=True)
        df = df.where(pd.notnull(df), 0)

        clean_df, error_df = validate_excel_data(df)

        error_rows = []
        fname = None

        if error_df is not None and not error_df.empty:
            error_rows = normalize_error_rows(error_df.to_dict(orient="records"))
            fname = f"mixed_validation_errors_{uuid.uuid4().hex}.xlsx"
            error_df.to_excel(os.path.join(TEMP_FOLDER, fname), index=False)

        if clean_df.empty:
            raise HTTPException(
                status_code=400,
                detail=make_json_safe({
                    "message": "File processed with errors",
                    "records_inserted": 0,
                    "skipped_records": len(error_rows),
                    "error_file": fname,
                    "error_rows": error_rows,
                })
            )

        clean_df["duration_month"] = clean_df["duration_month"].apply(parse_month_format)
        clean_df["payroll_month"] = clean_df["payroll_month"].apply(parse_month_format)

        shift_rates = load_shift_rates(db)
        inserted = 0

        allowed_fields = {
            "emp_id", "emp_name", "grade", "department",
            "client", "project", "project_code",
            "account_manager", "practice_lead", "delivery_manager",
            "duration_month", "payroll_month",
            "billability_status", "practice_remarks", "rmg_comments",
            "month_year",
        }

        for row in clean_df.to_dict(orient="records"):

            delete_existing_emp_month(
                db,
                row.get("emp_id"),
                row.get("duration_month"),
                row.get("payroll_month"),
            )

            sa = ShiftAllowances(**{k: row[k] for k in allowed_fields if k in row})
            db.add(sa)
            db.flush()

            for shift, col in [
                ("A", "shift_a_days"),
                ("B", "shift_b_days"),
                ("C", "shift_c_days"),
                ("PRIME", "prime_days"),
            ]:
                days = float(row.get(col, 0) or 0)
                if days > 0:
                    rate = shift_rates.get(shift, 0)
                    db.add(
                        ShiftMapping(
                            shiftallowance_id=sa.id,
                            shift_type=shift,
                            days=days,
                            total_allowance=days * rate
                        )
                    )

            inserted += 1

        db.commit()

        if error_rows:
            raise HTTPException(
                status_code=400,
                detail=make_json_safe({
                    "message": "File processed with errors",
                    "records_inserted": inserted,
                    "skipped_records": len(error_rows),
                    "error_file": fname,
                    "error_rows": error_rows,
                })
            )

        return {
            "message": "File processed successfully",
            "records_inserted": inserted
        }

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
