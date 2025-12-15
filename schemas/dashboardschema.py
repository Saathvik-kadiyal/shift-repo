from pydantic import BaseModel
from typing import List


class PieChartClientShift(BaseModel):
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
    Name: str
    total_no_of_days: float



class GraphResponse(BaseModel):
    Name: str
    total_allowances: float



class VerticalGraphResponse(BaseModel):
    client_full_name: str
    client_enum: str
    total_days: float
    total_allowances: float


class ClientList(BaseModel):
    clients: List[str]

