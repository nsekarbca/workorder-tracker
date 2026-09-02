from datetime import date, datetime
from typing import Optional, List
from pydantic import BaseModel


class UserLogin(BaseModel):
    username: str
    password: str


class ProcessOut(BaseModel):
    id: int
    name: str

    class Config:
        from_attributes = True


class UserOut(BaseModel):
    id: int
    username: str
    full_name: str
    role: str
    email: Optional[str] = None
    dob: Optional[date] = None
    doj: Optional[date] = None
    anniversary_date: Optional[date] = None
    designation: Optional[str] = None
    reporting_manager: Optional[str] = None
    employment_status: str = "Active"
    must_change_password: bool = True
    processes: List[ProcessOut] = []

    class Config:
        from_attributes = True


class LoginResponse(BaseModel):
    access_token: str
    token_type: str
    role: str
    full_name: str
    must_change_password: bool
    processes: List[ProcessOut] = []


class ChangePasswordRequest(BaseModel):
    old_password: str
    new_password: str

class SessionTimeoutSetting(BaseModel):
    minutes: int
    
class ForgotUsernameRequest(BaseModel):
    email: str


class ForgotPasswordRequest(BaseModel):
    username_or_email: str


class ResetPasswordWithTokenRequest(BaseModel):
    token: str
    new_password: str


# Super-Admin-only: create a full user profile in one step.
class CreateUserRequest(BaseModel):
    username: str
    full_name: str
    role: str  # "colleague", "team_lead", or "super_admin"
    email: Optional[str] = None
    dob: Optional[date] = None
    doj: Optional[date] = None
    anniversary_date: Optional[date] = None
    designation: Optional[str] = None
    reporting_manager: Optional[str] = None
    employment_status: str = "Active"
    employee_id: Optional[str] = None
    process_ids: List[int] = []


class CreateUserResponse(BaseModel):
    user: UserOut
    temporary_password: str


class ResetPasswordResponse(BaseModel):
    username: str
    temporary_password: str


class ReassignRequest(BaseModel):
    assigned_to_id: int


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
    escalation_category: Optional[str] = None
    issue_raised_date: Optional[date] = None
    posted_date: Optional[date] = None


# Fields a Team Lead may correct on a row that's Completed but not yet
# submitted to Production — deliberately permissive, since this is only
# reachable by a trusted Team Lead for fixing a colleague's mistake before
# end-of-day submission locks it for good.
class TeamLeadCorrection(BaseModel):
    posted_amount: Optional[float] = None
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
    process_id: Optional[int]
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
    submitted: bool
    submitted_at: Optional[datetime]

    class Config:
        from_attributes = True
