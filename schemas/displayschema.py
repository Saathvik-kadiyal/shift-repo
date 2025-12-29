"""
Pydantic schemas for shift allowance, employee, and client summary responses.

This module defines request and response models used across shift allowance,
employee details, summaries, dashboards, and Excel correction workflows.
All schemas are designed for FastAPI response validation and serialization.
"""
# pylint: disable=too-few-public-methods,missing-class-docstring
from typing import Optional, List,Dict,Union
from datetime import datetime,date
from pydantic import BaseModel,Field

class ShiftAllowancesResponse(BaseModel):
    """
    Response model for shift allowance summary per employee.
    """
    id: int
    emp_id: str
    emp_name: str
    department: str
    payroll_month: str
    client: str
    account_manager: str
    duration_month: str
    shift_types: List
    shift_days: Dict

    class Config:
        from_attributes = True

class ClientSummary(BaseModel):
    """
    Aggregated shift summary per client and account manager.
    """
    account_manager: str
    client: str
    total_employees: int
    shift_a_days: float
    shift_b_days: float
    shift_c_days: float
    prime_days: float
    total_allowances: float

    class Config:
        from_attributes = True


class ShiftMappingResponse(BaseModel):
    """
    Individual shift mapping details.
    """
    shift_type: str
    days: int
    total_allowance:Optional[str]

    class Config:
        from_attributes = True


class EmployeeResponse(BaseModel):
    """
    Detailed employee information including shift mappings.
    """
    id: int
    emp_id: Optional[str]
    emp_name: Optional[str]
    grade: Optional[str]
    department: Optional[str]
    client: Optional[str]
    project: Optional[str]
    project_code: Optional[str]
    account_manager: Optional[str]
    practice_lead: Optional[str]
    delivery_manager: Optional[str]

    duration_month: Optional[date]
    payroll_month: Optional[date]

    billability_status: Optional[str]
    practice_remarks: Optional[str]
    rmg_comments: Optional[str]


    created_at: datetime
    updated_at: datetime

    shift_mappings: List[ShiftMappingResponse] = []

    class Config:
        from_attributes = True



class PaginatedShiftResponse(BaseModel):
    """
    Paginated shift response with selected month context.
    """
    total_records: int
    selected_month: str
    data: List[ShiftAllowancesResponse]

    class Config:
        from_attributes = True

class ShiftUpdateRequest(BaseModel):
    """
    Request payload for updating shift days.
    """
    shift_a: Optional[str] = None
    shift_b: Optional[str] = None
    shift_c: Optional[str] = None
    prime: Optional[str] = None

class ShiftDetail(BaseModel):
    """
    Shift-wise breakdown after update.
    """
    shift: str
    days: float

class ShiftUpdateResponse(BaseModel):
    """
    Response returned after successful shift update.
    """
    message: str
    updated_fields: List[str]
    total_days: float
    total_allowance: float
    shift_details: List[ShiftDetail]

class ClientAllowance(BaseModel):
    """
    Client-wise allowance total.
    """
    client: str
    total_allowances: float

    class Config:
        from_attributes = True


class ClientAllowanceList(BaseModel):
    """
    Wrapper for client allowance list responses.
    """
    data: List[ClientAllowance]


class ClientDeptResponse(BaseModel):
    """
    Mapping of client to departments.
    """
    client: str
    departments: List[str]

    class Config:
        from_attributes = True


class CorrectedRow(BaseModel):
    """
    Represents a corrected employee row from Excel upload.
    """

    emp_id: str
    project:str
    duration_month: Optional[str] = Field(
    None,
    description="Format: Mon'YY (e.g. Jan'25)"
)

    payroll_month: Optional[str] = Field(
    None,
    description="Format: Mon'YY (e.g. Jan'25)"
)


    shift_a_days: Optional[Union[int, float]] = 0
    shift_b_days: Optional[Union[int, float]] = 0
    shift_c_days: Optional[Union[int, float]] = 0
    prime_days: Optional[Union[int, float]] = 0

    class Config:
        extra = "ignore"


class CorrectedRowsRequest(BaseModel):
    """
    Request payload containing multiple corrected rows.
    """
    corrected_rows: List[CorrectedRow]

    class Config:
        extra = "ignore"
