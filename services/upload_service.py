import os
import uuid
import io
import pandas as pd
import re
from datetime import datetime
from fastapi import HTTPException
from sqlalchemy.orm import Session
from models.models import UploadedFiles, ShiftAllowances, ShiftMapping
from utils.enums import ExcelColumnMap
from sqlalchemy.exc import IntegrityError

TEMP_FOLDER = "media/error_excels"
os.makedirs(TEMP_FOLDER, exist_ok=True)

MONTH_MAP = {
    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,
    'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8,
    'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
}


def parse_month_format(value: str):
    if not isinstance(value, str):
        return None
    try:
        month_abbr, year_suffix = value.split("'")
        month_num = MONTH_MAP.get(month_abbr.strip().title())
        year_full = 2000 + int(year_suffix)
        if month_num:
            return datetime(year_full, month_num, 1).date()
    except Exception:
        pass
    return None


def validate_excel_data(df: pd.DataFrame):
    errors = []
    error_rows = []

    # NEW: separate int_validation from other errors
    error_tracker = {
        "dup_internal": False,
        "total_days_mismatch": False,
        "int_validation": False,
        "other": False,
    }

    month_pattern = re.compile(r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'[0-9]{2}$")

    for idx, row in df.iterrows():
        row_errors = []

        # numeric validation
        for col in ["shift_a_days", "shift_b_days", "shift_c_days", "prime_days", "total_days"]:
            value = row.get(col)
            try:
                if value is None or str(value).strip() == "":
                    df.at[idx, col] = 0.0
                else:
                    df.at[idx, col] = float(value)
            except Exception:
                row_errors.append(f"Invalid numeric value in '{col}' → '{value}'")
                error_tracker["int_validation"] = True  # mark as int validation failure

        # month format validation
        for month_col in ["duration_month", "payroll_month"]:
            value = str(row.get(month_col, "")).strip()
            if value and not month_pattern.match(value):
                row_errors.append(f"Invalid month format in '{month_col}' → '{value}'")
                error_tracker["other"] = True

        # total days mismatch
        # IMPORTANT: guard with try/except to avoid 500 on bad numeric data
        try:
            total = (
                float(df.at[idx, "shift_a_days"])
                + float(df.at[idx, "shift_b_days"])
                + float(df.at[idx, "shift_c_days"])
                + float(df.at[idx, "prime_days"])
            )
            if total != float(df.at[idx, "total_days"]):
                row_errors.append(
                    f"SUM MISMATCH: A+B+C+PRIME = {total} but TOTAL_DAYS = {df.at[idx, 'total_days']}"
                )
                error_tracker["total_days_mismatch"] = True
        except Exception:
            # numeric conversion already flagged; don't add another error here
            pass

        if row_errors:
            row_dict = row.to_dict()
            row_dict["error"] = "; ".join(row_errors)
            error_rows.append(row_dict)
            errors.append(idx)

    clean_df = df.drop(index=errors).reset_index(drop=True)
    error_df = pd.DataFrame(error_rows) if error_rows else None

    # internal duplicate detection on clean rows only
    if not clean_df.empty:
        dup_cols = ["emp_id", "duration_month", "payroll_month"]
        duplicate_mask = clean_df[dup_cols].duplicated(keep=False)

        if duplicate_mask.any():
            dup_df = clean_df[duplicate_mask].copy()
            dup_df["error"] = "Duplicate record inside file"
            clean_df = clean_df.drop(clean_df[duplicate_mask].index).reset_index(drop=True)
            error_tracker["dup_internal"] = True

            if error_df is not None:
                error_df = pd.concat([error_df, dup_df], ignore_index=True)
            else:
                error_df = dup_df

    return clean_df, error_df, error_tracker


async def process_excel_upload(file, db: Session, user, base_url: str):
    uploaded_by = user.id

    if not file.filename.endswith((".xls", ".xlsx")):
        raise HTTPException(status_code=400, detail="Only Excel files are allowed")

    uploaded_file = UploadedFiles(
        filename=file.filename,
        uploaded_by=uploaded_by,
        status="processing",
        payroll_month=None,
    )
    db.add(uploaded_file)
    db.commit()
    db.refresh(uploaded_file)

    try:
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))

        column_mapping = {e.value: e.name for e in ExcelColumnMap}
        df.rename(columns=column_mapping, inplace=True)

        required_cols = [e.name for e in ExcelColumnMap]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing columns: {missing}")

        df = df.where(pd.notnull(df), 0)

        clean_df, error_df, error_tracker = validate_excel_data(df)

        error_file = None
        if error_df is not None and not error_df.empty:
            uid = uuid.uuid4().hex

            # filename selection now respects integer-only case
            if (
                error_tracker["dup_internal"]
                and not error_tracker["total_days_mismatch"]
                and not error_tracker["int_validation"]
                and not error_tracker["other"]
            ):
                duration_value = str(error_df["duration_month"].iloc[0]).replace(" ", "_")
                fname = f"dup_error_{duration_value}_{uid}.xlsx"

            elif (
                error_tracker["total_days_mismatch"]
                and not error_tracker["dup_internal"]
                and not error_tracker["int_validation"]
                and not error_tracker["other"]
            ):
                fname = f"error_total_days_mismatch_{uid}.xlsx"

            elif (
                error_tracker["int_validation"]
                and not error_tracker["dup_internal"]
                and not error_tracker["total_days_mismatch"]
                and not error_tracker["other"]
            ):
                fname = f"integer_validation_failed_{uid}.xlsx"

            else:
                fname = f"mixed_validation_errors_{uid}.xlsx"

            path = os.path.join(TEMP_FOLDER, fname)
            reverse_mapping = {e.name: e.value for e in ExcelColumnMap}
            error_df.rename(columns=reverse_mapping, inplace=True)
            error_df.to_excel(path, index=False)
            error_file = f"{base_url}/upload/error-files/{fname}"

        if clean_df.empty:
            uploaded_file.status = "failed"
            db.commit()
            raise HTTPException(
                status_code=400,
                detail={"message": "No valid rows found in file", "error_file": error_file}
            )

        # parse months to dates only for valid rows going into DB
        for col in ["duration_month", "payroll_month"]:
            clean_df[col] = clean_df[col].apply(parse_month_format)

        uploaded_file.payroll_month = clean_df["payroll_month"].iloc[0]
        db.commit()

        inserted_count = 0

        shift_fields = {"shift_a_days", "shift_b_days", "shift_c_days", "prime_days"}
        allowed_fields = {
            "emp_id", "emp_name", "grade", "department",
            "client", "project", "project_code",
            "account_manager", "practice_lead", "delivery_manager",
            "duration_month", "payroll_month",
            "billability_status", "practice_remarks", "rmg_comments",
            "month_year",
        }

        for row in clean_df.to_dict(orient="records"):
            shift_data = {k: float(row.get(k, 0)) for k in shift_fields}
            sa_payload = {k: row[k] for k in allowed_fields if k in row}

            sa = ShiftAllowances(**sa_payload)
            db.add(sa)
            db.flush()

            for shift_type, days in [
                ("A", shift_data["shift_a_days"]),
                ("B", shift_data["shift_b_days"]),
                ("C", shift_data["shift_c_days"]),
                ("PRIME", shift_data["prime_days"]),
            ]:
                if days > 0:
                    db.add(ShiftMapping(shiftallowance_id=sa.id, shift_type=shift_type, days=days))

            inserted_count += 1

        db.commit()
        uploaded_file.status = "processed"
        uploaded_file.record_count = inserted_count
        db.commit()

        # always return 400 if error file exists, even if some rows inserted
        if error_file:
            raise HTTPException(
                status_code=400,
                detail={
                    "message": "File processed with errors",
                    "records_inserted": inserted_count,
                    "skipped_records": len(error_df),
                    "error_file": error_file,
                },
            )

        return {
            "message": "File processed successfully",
            "records_inserted": inserted_count,
        }

    except HTTPException as http_err:
        db.rollback()
        uploaded_file.status = "failed"
        db.commit()
        raise http_err

    except Exception as error:
        db.rollback()
        uploaded_file.status = "failed"
        db.commit()

        # keep your duplicate-key special handling
        if "duplicate key value violates unique constraint" in str(error):
            pm = str(df["payroll_month"].iloc[0]).replace(" ", "_")
            dm = str(df["duration_month"].iloc[0]).replace(" ", "_")
            fname = f"error_{pm}_{dm}_exists_in_database_{uuid.uuid4().hex}.xlsx"
            path = os.path.join(TEMP_FOLDER, fname)
            df.to_excel(path, index=False)
            raise HTTPException(
                status_code=400,
                detail=f"Duplicate data exists in DB. Download: {base_url}/upload/error-files/{fname}",
            )

        raise HTTPException(status_code=500, detail=f"Processing failed: {str(error)}")
