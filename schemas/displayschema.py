from pydantic import BaseModel,Field,validator
from typing import Optional, List,Dict,Union
from datetime import datetime,date


class ShiftAllowancesResponse(BaseModel):
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
    shift_type: str
    days: int
    total_allowance:Optional[str]

    class Config:
        from_attributes = True


class EmployeeResponse(BaseModel):
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
    total_records: int
    selected_month: str
    data: List[ShiftAllowancesResponse]

    class Config:
        from_attributes = True

class ShiftUpdateRequest(BaseModel):
    shift_a: Optional[str] = None
    shift_b: Optional[str] = None
    shift_c: Optional[str] = None
    prime: Optional[str] = None
 
class ShiftDetail(BaseModel):
    shift: str
    days: float
 
class ShiftUpdateResponse(BaseModel):
    message: str
    updated_fields: List[str]
    total_days: float
    total_allowance: float
    shift_details: List[ShiftDetail]

class ClientAllowance(BaseModel):
    client: str
    total_allowances: float
 
    class Config:
        from_attributes = True
 
 
class ClientAllowanceList(BaseModel):
    data: List[ClientAllowance]
 
 
class ClientDeptResponse(BaseModel):
    client: str
    departments: List[str]
 
    class Config:
        from_attributes = True


class CorrectedRow(BaseModel):

    emp_id: str
    project: Optional[str] = None

    duration_month: Optional[str] = Field(
        None,
        description="YYYY-MM (used as filter)"
    )

   
    payroll_month: Optional[str] = Field(
        None,
        description="YYYY-MM (used for update)"
    )

    shift_a_days: Optional[Union[int, float]] = 0
    shift_b_days: Optional[Union[int, float]] = 0
    shift_c_days: Optional[Union[int, float]] = 0
    prime_days: Optional[Union[int, float]] = 0

    
    emp_name: Optional[str] = None
    grade: Optional[str] = None

    current_status: Optional[str] = Field(
        None, alias="Current Status(e)"
    )

    department: Optional[str] = None
    client: Optional[str] = None
    project_code: Optional[str] = None
    account_manager: Optional[str] = None
    practice_lead: Optional[str] = None
    delivery_manager: Optional[Union[str, int]] = None

    shift_types: Optional[int] = Field(
        None, alias="# Shift Types(e)"
    )

    total_days: Optional[Union[int, float]] = None

    timesheet_billable_days: Optional[int] = Field(
        None, alias="Timesheet Billable Days"
    )
    timesheet_non_billable_days: Optional[int] = Field(
        None, alias="Timesheet Non Billable Days"
    )

    diff: Optional[int] = Field(None, alias="Diff")
    final_total_days: Optional[int] = Field(
        None, alias="Final Total Days"
    )

    billability_status: Optional[str] = None
    practice_remarks: Optional[Union[str, int]] = None
    rmg_comments: Optional[str] = None

    amar_approval: Optional[int] = Field(
        None, alias="Amar Approval"
    )

    shift_a_allowances: Optional[Union[int, float]] = Field(
        None, alias="Shift A Allowances"
    )
    shift_b_allowances: Optional[Union[int, float]] = Field(
        None, alias="Shift B Allowances"
    )
    shift_c_allowances: Optional[Union[int, float]] = Field(
        None, alias="Shift C Allowances"
    )
    prime_allowances: Optional[Union[int, float]] = Field(
        None, alias="Prime Allowances"
    )

    total_day_allowances: Optional[Union[int, float]] = Field(
        None, alias="TOTAL DAYS Allowances"
    )

    am_email_attempt: Optional[str] = Field(
        None, alias="AM Email Attempt(e)"
    )

    am_approval_status: Optional[Union[str, int]] = Field(
        None, alias="AM Approval Status(e)"
    )

   
    class Config:
        allow_population_by_field_name = True
        extra = "ignore"


class CorrectedRowsRequest(BaseModel):
    corrected_rows: List[CorrectedRow]

    class Config:
        extra = "ignore"
