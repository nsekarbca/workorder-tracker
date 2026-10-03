from datetime import date, datetime
from typing import Optional, List, Dict
from pydantic import BaseModel


class UserLogin(BaseModel):
    username: str
    password: str


class CelebrationCommentOut(BaseModel):
    id: int
    target_user_id: int
    message: str
    posted_by_name: str
    created_at: datetime
    reaction_counts: dict = {}  # e.g. {"like": 2, "heart": 1}
    my_reactions: List[str] = []  # reaction types the current user already gave this comment

    class Config:
        from_attributes = True


class CelebrationCommentCreate(BaseModel):
    message: str


class CelebrationReactRequest(BaseModel):
    reaction: str  # "like" | "heart"


class CelebrationOccasion(BaseModel):
    kind: str  # "birthday" or "anniversary"
    years: Optional[int] = None  # anniversaries only, if resolvable


class CelebrationPerson(BaseModel):
    user_id: int
    full_name: str
    occasions: List[CelebrationOccasion] = []  # usually one, but a birthday and
    # work anniversary can legitimately fall on the same day for one person
    comments: List[CelebrationCommentOut] = []


class ProcessUpdateAttachmentOut(BaseModel):
    id: int
    file_name: str
    content_type: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True


class ProcessUpdateOut(BaseModel):
    id: int
    process_id: int
    received_date: Optional[date] = None
    mode: Optional[str] = None
    received_from: Optional[str] = None
    category: Optional[str] = None
    status: str
    message: str
    verified_by: Optional[str] = None
    posted_by_name: str
    posted_by_role: str
    created_at: datetime
    updated_at: Optional[datetime] = None
    updated_by_name: Optional[str] = None
    updated_by_role: Optional[str] = None
    attachments: List[ProcessUpdateAttachmentOut] = []

    class Config:
        from_attributes = True


class ProcessUpdateCreate(BaseModel):
    process_ids: List[int]  # post this update to one or more processes at once
    received_date: Optional[date] = None
    mode: Optional[str] = None
    received_from: Optional[str] = None
    category: Optional[str] = None
    status: str = "Active"
    message: str
    verified_by: Optional[str] = None


# Editing an existing update — same fields, minus process_ids since which
# process(es) it was posted to isn't changeable after the fact (each
# process got its own independent row at creation time).
class ProcessUpdateEdit(BaseModel):
    received_date: Optional[date] = None
    mode: Optional[str] = None
    received_from: Optional[str] = None
    category: Optional[str] = None
    status: str = "Active"
    message: str
    verified_by: Optional[str] = None


class ProcessOut(BaseModel):
    id: int
    name: str
    daily_target: Optional[int] = None

    class Config:
        from_attributes = True


# Super-Admin-only: create a new process, with an optional daily target.
class ProcessCreate(BaseModel):
    name: str
    daily_target: Optional[int] = None


# Super-Admin-only: edit an existing process's name and/or daily target.
class ProcessUpdate(BaseModel):
    name: str
    daily_target: Optional[int] = None


class UserOut(BaseModel):
    id: int
    username: str
    full_name: str
    role: str
    email: Optional[str] = None
    employee_id: Optional[str] = None
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
    id: int
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
    role: str  # "colleague", "team_lead", "admin", "quality" or "super_admin"
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


class ClarificationDetailOut(BaseModel):
    order_id: int
    deposit_type: Optional[str] = None
    exchange: Optional[str] = None
    era_check: Optional[str] = None
    edm_batch_number: Optional[str] = None
    bar_batch_number: Optional[str] = None
    batch_description: Optional[str] = None
    escalation_type: Optional[str] = None
    clarification_details: Optional[str] = None
    team: Optional[str] = None
    poster_login: Optional[str] = None
    amount_posted: Optional[str] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class ClarificationDetailSave(BaseModel):
    escalation_type: Optional[str] = None


class EscalationDetailOut(BaseModel):
    order_id: int
    category: str
    data: dict
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class EscalationDetailSave(BaseModel):
    category: str  # which category's form this is — sent explicitly since the
    # row's own escalation_category is often still an unsaved draft in the
    # browser at the point the popup is used, not yet persisted to the order
    data: dict = {}  # only the category's manual fields — auto fields are always server-derived


class ReassignRequest(BaseModel):
    assigned_to_id: int


# Team Lead-initiated (or Super Admin, on a Team Lead's behalf) transfer of
# a batch of not-yet-completed, not-yet-submitted orders to another Team
# Lead's queue. Omitting order_ids transfers the whole eligible queue.
class TransferOrdersRequest(BaseModel):
    to_team_lead_id: int
    from_team_lead_id: Optional[int] = None
    order_ids: Optional[List[int]] = None


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
    team_lead_id: Optional[int]
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
    escalated: bool = False
    clarification_detail: Optional[ClarificationDetailOut] = None
    escalation_detail: Optional[EscalationDetailOut] = None
    timer_status: str = "running"
    timer_started_at: Optional[datetime] = None
    time_taken_seconds: int = 0

    # Filled in for Admin / Super Admin on the Active Queue, Escalation queue
    # and Production lists so those screens can be shown Team Lead-wise.
    team_lead_name: Optional[str] = None

    class Config:
        from_attributes = True


class EscalationResolve(BaseModel):
    ventra_comment: str


# Batch Count Dashboard — one calculated row per colleague / process / date.
class BatchDashboardRow(BaseModel):
    user_id: int
    employee_name: str
    process_id: int
    process_name: str
    work_date: date
    batches_worked: int
    total_trans_count: int
    daily_target: Optional[int] = None
    hours_worked: Optional[float] = None
    production_pct: Optional[float] = None
    accounts_audited: Optional[int] = None
    errors: Optional[int] = None
    quality_pct: Optional[float] = None
    team_lead_id: Optional[int] = None      # 0 = no Team Lead
    team_lead_name: Optional[str] = None
    # True for the "Overall" row added when a colleague worked more than one
    # process on the same date (process_id is 0 on those rows).
    is_summary: bool = False


# Only the fields actually sent are changed. Colleagues may send
# hours_worked only; Team Leads / Super Admins may send accounts_audited
# and errors only (enforced in the endpoint).
class BatchStatUpdate(BaseModel):
    user_id: Optional[int] = None  # ignored for colleagues (always themselves)
    process_id: int
    work_date: date
    hours_worked: Optional[float] = None
    accounts_audited: Optional[int] = None
    errors: Optional[int] = None


# Team Lead "Orders Dashboard" — one row per date.
# received / pending / in_process / clarification / completed all count the
# orders RECEIVED on that date, by their current status (so they add up:
# received = pending + in_process + clarification + completed).
# completed_on_date counts orders marked Completed on that date (Posted
# Date), whichever day they were received.
class OrdersDashboardRow(BaseModel):
    work_date: date
    received: int
    pending: int
    in_process: int
    clarification: int
    completed: int
    completed_on_date: int


# Same five counts as OrdersDashboardRow, grouped by Team Lead instead of date
# (Admin / Super Admin only). team_lead_id 0 = orders with no Team Lead.
class OrdersDashboardTLRow(BaseModel):
    team_lead_id: int
    team_lead_name: str
    received: int
    pending: int
    in_process: int
    clarification: int
    completed: int
    completed_on_date: int


# Raw-data drill-down: a normal order plus the Team Lead it belongs to.
class OrdersDashboardDetail(WorkOrderOut):
    team_lead_name: Optional[str] = None


# Batch Count Dashboard grouped by Team Lead over the whole date range
# (Admin / Super Admin). team_lead_id 0 = colleagues with no Team Lead,
# -1 = the "All Team Leads" total row.
class BatchTeamLeadRow(BaseModel):
    team_lead_id: int
    team_lead_name: str
    colleagues: int
    batches_worked: int
    total_trans_count: int
    hours_worked: Optional[float] = None
    production_pct: Optional[float] = None
    accounts_audited: Optional[int] = None
    errors: Optional[int] = None
    quality_pct: Optional[float] = None


# Admin / Super Admin: what Team Leads changed on locked / completed orders.
class OrderChangeLogOut(BaseModel):
    id: int
    created_at: datetime
    process_id: Optional[int] = None
    process_name: Optional[str] = None
    order_id: int
    edm: Optional[str] = None
    employee_name: Optional[str] = None
    actor_id: Optional[int] = None
    actor_username: Optional[str] = None
    actor_name: Optional[str] = None
    actor_role: Optional[str] = None
    action: str
    order_state: Optional[str] = None
    changes: List[dict] = []

    class Config:
        from_attributes = True


# ---- Clients (Super Admin -> Admin -> Clients) ----
class ClientAssignment(BaseModel):
    process_id: int
    team_lead_id: int


class ClientOut(BaseModel):
    id: int
    facility_no: str
    client_name: str
    status: str
    inactive_date: Optional[date] = None
    assignments: List[ClientAssignment] = []


class ClientTeamLeadOut(BaseModel):
    id: int
    full_name: str
    active: bool
    process_ids: List[int] = []


class ClientProcessOut(BaseModel):
    id: int
    name: str


class ClientListResponse(BaseModel):
    clients: List[ClientOut]
    team_leads: List[ClientTeamLeadOut]
    processes: List[ClientProcessOut]


class ClientSave(BaseModel):
    facility_no: str
    client_name: str
    status: str = "Active"
    inactive_date: Optional[date] = None   # Inactive with no date -> today (IST)


# process_id -> Team Lead id (null clears that process). Only the processes
# sent are changed.
class ClientAssignmentsSave(BaseModel):
    assignments: Dict[int, Optional[int]]


class ClientBulkAssign(BaseModel):
    client_ids: List[int]
    process_ids: Optional[List[int]] = None   # null / empty = every process
    team_lead_id: Optional[int] = None        # null = clear the assignment


class ClientBulkResult(BaseModel):
    updated: int                              # client x process assignments set / cleared
    skipped_processes: List[str] = []         # processes the Team Lead doesn't work on


# ---- Import Exceptions (Team Lead / Admin / Super Admin) ----
class ImportExceptionOut(BaseModel):
    id: int
    created_at: datetime
    process_id: Optional[int] = None
    process_name: Optional[str] = None
    kind: str
    edm: Optional[str] = None
    division_raw: Optional[str] = None
    facility_no: Optional[str] = None
    client_name: Optional[str] = None
    def_doc_type: Optional[str] = None
    amount: Optional[float] = None
    reason: Optional[str] = None
    imported_by_name: Optional[str] = None
    imported_by_role: Optional[str] = None
    team_lead_id: Optional[int] = None
    team_lead_name: Optional[str] = None
    existing_order_id: Optional[int] = None
    existing_posted_date: Optional[date] = None
    existing_employee_name: Optional[str] = None

    class Config:
        from_attributes = True
