import os
import uuid
import io
import pandas as pd
from datetime import datetime
from fastapi import HTTPException
from sqlalchemy.orm import Session
from models.models import UploadedFiles, ShiftAllowances
from utils.enums import ExcelColumnMap

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


def validate_excel_data(df: pd.DataFrame, numeric_columns: list):
    errors = []
    error_rows = []

    for idx, row in df.iterrows():
        row_errors = []
        for col in numeric_columns:
            value = row[col]
            if not pd.api.types.is_numeric_dtype(type(value)):
                try:
                    df.at[idx, col] = pd.to_numeric(value)
                except (ValueError, TypeError):
                    row_errors.append(f"Invalid value in '{col}' → '{value}' (expected numeric)")
        if row_errors:
            row_data = row.to_dict()
            row_data["error"] = "; ".join(row_errors)
            error_rows.append(row_data)
            errors.append(idx)

    clean_df = df.drop(index=errors).reset_index(drop=True)
    error_df = pd.DataFrame(error_rows) if error_rows else None
    return clean_df, error_df


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

        # Rename Excel columns using Enum
        column_mapping = {e.value: e.name for e in ExcelColumnMap}
        df.rename(columns=column_mapping, inplace=True)

        required_columns = [e.name for e in ExcelColumnMap]
        missing = [c for c in required_columns if c not in df.columns]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing columns in Excel: {missing}")

        df = df.where(pd.notnull(df), 0)

        int_columns = ["shift_a_days", "shift_b_days", "shift_c_days", "prime_days", "total_days"]
        numeric_columns = int_columns

        clean_df, error_df = validate_excel_data(df, numeric_columns)

        inserted_count = 0
        if not clean_df.empty:
            clean_df[int_columns] = (
                clean_df[int_columns]
                .apply(pd.to_numeric, errors="coerce")
                .round(0)
                .astype("Int64")
            )

            for col in ["duration_month", "payroll_month"]:
                clean_df[col] = clean_df[col].apply(parse_month_format)

            payroll_date = clean_df["payroll_month"].iloc[0]
            uploaded_file.payroll_month = payroll_date if payroll_date else None

            records = clean_df[required_columns].to_dict(orient="records")

            for row in records:
                if not row.get("month_year"):
                    row["month_year"] = datetime.now().date()

            shift_records = [ShiftAllowances(**row) for row in records]
            db.bulk_save_objects(shift_records)
            db.commit()
            inserted_count = len(shift_records)

        if error_df is not None and not error_df.empty:
            error_filename = f"error_{uuid.uuid4().hex}.xlsx"
            error_path = os.path.join(TEMP_FOLDER, error_filename)
            error_df.to_excel(error_path, index=False)

            uploaded_file.status = "partially_processed"
            uploaded_file.record_count = inserted_count
            db.commit()

            return {
                "message": "File partially processed. Some rows contained invalid data.",
                "records_inserted": inserted_count,
                "records_skipped": len(error_df),
                "download_link": f"{base_url}/upload/error-files/{error_filename}",
                "file_name": error_filename,
            }

        uploaded_file.status = "processed"
        uploaded_file.record_count = inserted_count
        db.commit()

        return {
            "message": "File processed successfully",
            "file_id": uploaded_file.id,
            "records": inserted_count,
        }

    except HTTPException:
        db.rollback()
        uploaded_file.status = "failed"
        db.commit()
        raise

    except Exception as e:
        db.rollback()
        uploaded_file.status = "failed"
        db.commit()
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")
