from datetime import date, datetime
from typing import Optional
from pydantic import BaseModel


class UserLogin(BaseModel):
    username: str
    password: str


class UserOut(BaseModel):
    id: int
    username: str
    full_name: str
    role: str

    class Config:
        from_attributes = True


# Fields a Team Lead may set (A-D)
class TeamLeadUpdate(BaseModel):
    received_date: Optional[date] = None
    assigned_date: Optional[date] = None
    employee_id: Optional[str] = None
    employee_name: Optional[str] = None
    assigned_to_id: Optional[int] = None


# Fields used for bulk inventory import (E-O)
class InventoryImportRow(BaseModel):
    edm: Optional[str] = None
    status: Optional[str] = None
    created: Optional[datetime] = None
    image_count: Optional[int] = None
    doc_count: Optional[int] = None
    def_doc_type: Optional[str] = None
    amount: Optional[float] = None
    description: Optional[str] = None
    division: Optional[str] = None
    deposit_date: Optional[date] = None


# Fields a Colleague may set (P-Z)
class ColleagueUpdate(BaseModel):
    posted_amount: Optional[float] = None
    pending_amount: Optional[float] = None
    bar_batch: Optional[str] = None
    trans_count: Optional[int] = None
    posting_status: Optional[str] = None
    poster_comment: Optional[str] = None
    ventra_comment: Optional[str] = None
    escalation_category: Optional[str] = None
    issue_raised_date: Optional[date] = None
    issue_closed_date: Optional[date] = None
    posted_date: Optional[date] = None


class WorkOrderOut(BaseModel):
    id: int
    received_date: Optional[date]
    assigned_date: Optional[date]
    employee_id: Optional[str]
    employee_name: Optional[str]
    edm: Optional[str]
    status: Optional[str]
    created: Optional[datetime]
    image_count: Optional[int]
    doc_count: Optional[int]
    def_doc_type: Optional[str]
    amount: Optional[float]
    last_edited_by: Optional[str]
    description: Optional[str]
    division: Optional[str]
    deposit_date: Optional[date]
    posted_amount: Optional[float]
    pending_amount: Optional[float]
    bar_batch: Optional[str]
    trans_count: Optional[int]
    posting_status: Optional[str]
    poster_comment: Optional[str]
    ventra_comment: Optional[str]
    escalation_category: Optional[str]
    issue_raised_date: Optional[date]
    issue_closed_date: Optional[date]
    posted_date: Optional[date]
    tat_days: Optional[int]
    assigned_to_id: Optional[int]

    class Config:
        from_attributes = True
