"""
Dashboard and analytics request/response schemas.

This module defines Pydantic models used by dashboard-related APIs,
including pie charts, bar charts, graphs, client lists, and dashboard
filter payloads. These schemas standardize request validation and
response structures for analytics endpoints.
"""

from typing import List,Optional, Literal,Union,Dict
from pydantic import BaseModel,Field,field_validator


class PieChartClientShift(BaseModel):
    """
    Pie chart data structure for client-wise shift distribution.
    """
    client_full_name: str
    client_enum: str
    total_employees: int
    shift_a: int
    shift_b: int
    shift_c: int
    prime: int
    total_days: int
    total_allowances: float



class HorizontalBarResponse(BaseModel):
    """
    Horizontal bar chart response model.
    """
    Name: str
    total_no_of_days: float



class GraphResponse(BaseModel):
    """
    Line or bar graph response model.
    """
    Name: str
    total_allowances: float



class VerticalGraphResponse(BaseModel):
    """
    Vertical bar chart response model.
    """
    client_full_name: str
    client_enum: str
    total_days: float
    total_allowances: float


class ClientList(BaseModel):
    """
    Client list response model.
    """
    clients: List[str]



class DashboardFilterRequest(BaseModel):
    """
    Dashboard filter request payload.

    Supports client-wise filtering, date-based filtering (month, quarter,
    year), and result limiting using the `top` parameter.
    """

    clients: Union[
        Literal["ALL"],
        Dict[str, List[str]]
    ]


    top: str = Field(
        default="ALL",
        description="ALL or a numeric string like '2', '5', '10'"
    )

    start_month: Optional[str] = None
    end_month: Optional[str] = None

    selected_year: Optional[int] = None
    selected_months: Optional[List[str]] = None
    selected_quarters: Optional[List[Literal["Q1","Q2","Q3","Q4"]]] = None

    @field_validator("top")
    def validate_top(cls, v): # pylint: disable=no-self-argument
        """
        Validate the `top` field to allow only 'ALL' or a positive number string.
        """
        if v == "ALL":
            return v
        if not v.isdigit() or int(v) <= 0:
            raise ValueError("top must be 'ALL' or a positive number as string")
        return v
