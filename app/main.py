from datetime import date, datetime, timedelta
from typing import List, Optional

from fastapi import Depends, FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
import csv
import io

import os

from . import models, schemas, auth
from .database import Base, engine, get_db, SessionLocal

Base.metadata.create_all(bind=engine)

app = FastAPI(title="Work Order Allocation Tracker")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def bootstrap_first_team_lead():
    """
    Creates the very first Team Lead login automatically, from environment
    variables, if no users exist yet. This means no local script and no
    Python install is needed on your machine — just set BOOTSTRAP_USERNAME
    and BOOTSTRAP_PASSWORD as env vars on Render and the account is ready
    the moment the app deploys.
    """
    db = SessionLocal()
    try:
        if db.query(models.User).first() is not None:
            return
        username = os.getenv("BOOTSTRAP_USERNAME")
        password = os.getenv("BOOTSTRAP_PASSWORD")
        if not username or not password:
            return
        db.add(models.User(
            username=username,
            full_name="Team Lead",
            role="team_lead",
            password_hash=auth.hash_password(password),
        ))
        db.commit()
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

@app.post("/auth/login")
def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = db.query(models.User).filter(models.User.username == form_data.username).first()
    if not user or not auth.verify_password(form_data.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Incorrect username or password")
    token = auth.create_access_token({"sub": user.username})
    return {"access_token": token, "token_type": "bearer", "role": user.role, "full_name": user.full_name}


@app.post("/auth/register", response_model=schemas.UserOut)
def register_user(
    username: str,
    full_name: str,
    password: str,
    role: str,
    employee_id: Optional[str] = None,
    current_user: models.User = Depends(auth.require_role("team_lead")),
    db: Session = Depends(get_db),
):
    """Only a team lead can create new logins (colleagues or other leads)."""
    if role not in ("team_lead", "colleague"):
        raise HTTPException(status_code=400, detail="role must be 'team_lead' or 'colleague'")
    if db.query(models.User).filter(models.User.username == username).first():
        raise HTTPException(status_code=400, detail="username already exists")
    user = models.User(
        username=username,
        full_name=full_name,
        password_hash=auth.hash_password(password),
        role=role,
        employee_id=employee_id,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    if role == "colleague":
        _auto_assign_open_slots(db)
    return user


@app.get("/auth/me", response_model=schemas.UserOut)
def read_me(current_user: models.User = Depends(auth.get_current_user)):
    return current_user


@app.post("/orders/run-assignment")
def run_assignment(
    current_user: models.User = Depends(auth.require_role("team_lead")),
    db: Session = Depends(get_db),
):
    """
    Manually re-runs the auto-assignment pass. Use this any time a colleague
    ends up with no open order and needs to be caught up — e.g. right after
    creating a new colleague account, in case it wasn't picked up automatically.
    """
    _auto_assign_open_slots(db)
    return {"status": "assignment pass complete"}


@app.delete("/orders/all")
def delete_all_orders(
    current_user: models.User = Depends(auth.require_role("team_lead")),
    db: Session = Depends(get_db),
):
    """
    Deletes every work order — used to clear test/imported data for a fresh
    run. Does NOT touch user accounts (Team Lead / colleague logins stay).
    """
    deleted_count = db.query(models.WorkOrder).delete()
    db.commit()
    return {"deleted": deleted_count}


# ---------------------------------------------------------------------------
# Inventory import (E-O) — Team Lead bulk-loads from the existing Excel export
# ---------------------------------------------------------------------------

@app.post("/orders/import")
def import_inventory(
    file: UploadFile = File(...),
    current_user: models.User = Depends(auth.require_role("team_lead")),
    db: Session = Depends(get_db),
):
    """
    Accepts a CSV with columns matching E-O:
    edm,status,created,image_count,doc_count,def_doc_type,amount,description,division,deposit_date
    Creates one unassigned WorkOrder row per line.
    """
    content = file.file.read().decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content))
    created_count = 0
    today = date.today()
    for row in reader:
        order = models.WorkOrder(
            received_date=today,
            edm=row.get("edm") or None,
            status=row.get("status") or None,
            created=_parse_dt(row.get("created")),
            image_count=_parse_int(row.get("image_count")),
            doc_count=_parse_int(row.get("doc_count")),
            def_doc_type=row.get("def_doc_type") or None,
            amount=_parse_float(row.get("amount")),
            description=row.get("description") or None,
            division=row.get("division") or None,
            deposit_date=_parse_date(row.get("deposit_date")),
            last_edited_by=current_user.username,
        )
        db.add(order)
        created_count += 1
    db.commit()
    _auto_assign_open_slots(db)
    return {"imported": created_count}


def _parse_date(v: Optional[str]) -> Optional[date]:
    if not v:
        return None
    return datetime.strptime(v.strip(), "%Y-%m-%d").date()


def _parse_dt(v: Optional[str]) -> Optional[datetime]:
    if not v:
        return None
    return datetime.strptime(v.strip(), "%Y-%m-%d %H:%M:%S")


def _parse_int(v: Optional[str]) -> Optional[int]:
    return int(v) if v not in (None, "") else None


def _parse_float(v: Optional[str]) -> Optional[float]:
    return float(v) if v not in (None, "") else None


# ---------------------------------------------------------------------------
# Assignment logic — one open file per colleague at a time (round-robin)
# ---------------------------------------------------------------------------

def _auto_assign_open_slots(db: Session):
    """
    For every colleague who currently has no open (non-completed) order,
    hand them the oldest unassigned order. Mirrors the 'application-based'
    model: one open file per user; finishing it pulls the next one in.
    """
    colleagues = db.query(models.User).filter(models.User.role == "colleague").all()
    for colleague in colleagues:
        has_open = (
            db.query(models.WorkOrder)
            .filter(
                models.WorkOrder.assigned_to_id == colleague.id,
                models.WorkOrder.posting_status != "Completed",
            )
            .first()
        )
        if has_open:
            continue
        next_order = (
            db.query(models.WorkOrder)
            .filter(models.WorkOrder.assigned_to_id.is_(None))
            .order_by(models.WorkOrder.id.asc())
            .first()
        )
        if next_order:
            next_order.assigned_to_id = colleague.id
            next_order.assigned_date = date.today()
            next_order.employee_id = colleague.employee_id or colleague.username
            next_order.employee_name = colleague.full_name
            next_order.posting_status = "In-Process"
            db.add(next_order)
            # Autoflush is off for this session, so without this the next
            # colleague's "find an unassigned order" query would still see
            # this one as unassigned and both would grab the same row.
            db.flush()
    db.commit()


# ---------------------------------------------------------------------------
# Read endpoints
# ---------------------------------------------------------------------------

@app.get("/orders", response_model=List[schemas.WorkOrderOut])
def list_orders(current_user: models.User = Depends(auth.get_current_user), db: Session = Depends(get_db)):
    """Active queue only — orders already submitted to Production are hidden here for everyone."""
    query = db.query(models.WorkOrder).filter(models.WorkOrder.submitted == False)  # noqa: E712
    if current_user.role == "colleague":
        query = query.filter(models.WorkOrder.assigned_to_id == current_user.id)
    return query.order_by(models.WorkOrder.id.asc()).all()


@app.get("/orders/production", response_model=List[schemas.WorkOrderOut])
def list_production_orders(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    received_start: Optional[date] = None,
    received_end: Optional[date] = None,
    assigned_start: Optional[date] = None,
    assigned_end: Optional[date] = None,
    created_start: Optional[date] = None,
    created_end: Optional[date] = None,
    employee_name: Optional[str] = None,
    def_doc_type: Optional[str] = None,
    division: Optional[str] = None,
    escalation_category: Optional[str] = None,
    posting_status: Optional[str] = None,
    current_user: models.User = Depends(auth.require_role("team_lead")),
    db: Session = Depends(get_db),
):
    """
    Team-Lead-only view of everything already submitted to Production.
    All filters are optional and combine with AND. start_date/end_date filter
    by Posted Date (inclusive) — the day the work was actually completed.
    """
    query = db.query(models.WorkOrder).filter(models.WorkOrder.submitted == True)  # noqa: E712

    if start_date:
        query = query.filter(models.WorkOrder.posted_date >= start_date)
    if end_date:
        query = query.filter(models.WorkOrder.posted_date <= end_date)

    if received_start:
        query = query.filter(models.WorkOrder.received_date >= received_start)
    if received_end:
        query = query.filter(models.WorkOrder.received_date <= received_end)

    if assigned_start:
        query = query.filter(models.WorkOrder.assigned_date >= assigned_start)
    if assigned_end:
        query = query.filter(models.WorkOrder.assigned_date <= assigned_end)

    if created_start:
        query = query.filter(models.WorkOrder.created >= datetime.combine(created_start, datetime.min.time()))
    if created_end:
        query = query.filter(models.WorkOrder.created <= datetime.combine(created_end, datetime.max.time()))

    # Each of these accepts a comma-separated list of exact values, matching
    # a multi-select dropdown built from the distinct values actually present
    # in the data (rather than free-text partial matching).
    def _in_filter(column, raw: Optional[str]):
        if not raw:
            return None
        values = [v.strip() for v in raw.split(",") if v.strip()]
        return column.in_(values) if values else None

    for column, raw in [
        (models.WorkOrder.employee_name, employee_name),
        (models.WorkOrder.def_doc_type, def_doc_type),
        (models.WorkOrder.division, division),
        (models.WorkOrder.escalation_category, escalation_category),
        (models.WorkOrder.posting_status, posting_status),
    ]:
        cond = _in_filter(column, raw)
        if cond is not None:
            query = query.filter(cond)

    return query.order_by(models.WorkOrder.posted_date.asc(), models.WorkOrder.id.asc()).all()


@app.get("/orders/{order_id}", response_model=schemas.WorkOrderOut)
def get_order(order_id: int, current_user: models.User = Depends(auth.get_current_user), db: Session = Depends(get_db)):
    order = db.query(models.WorkOrder).filter(models.WorkOrder.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    if current_user.role == "colleague" and order.assigned_to_id != current_user.id:
        raise HTTPException(status_code=403, detail="Not your assigned order")
    return order


# ---------------------------------------------------------------------------
# Write endpoints — split by role, matching column ownership A-D / P-Z
# ---------------------------------------------------------------------------

@app.patch("/orders/{order_id}/team-lead", response_model=schemas.WorkOrderOut)
def update_team_lead_fields(
    order_id: int,
    payload: schemas.TeamLeadUpdate,
    current_user: models.User = Depends(auth.require_role("team_lead")),
    db: Session = Depends(get_db),
):
    order = db.query(models.WorkOrder).filter(models.WorkOrder.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    for field, value in payload.dict(exclude_unset=True).items():
        setattr(order, field, value)
    order.last_edited_by = current_user.username
    db.commit()
    db.refresh(order)
    return order


@app.patch("/orders/{order_id}/colleague", response_model=schemas.WorkOrderOut)
def update_colleague_fields(
    order_id: int,
    payload: schemas.ColleagueUpdate,
    current_user: models.User = Depends(auth.require_role("colleague")),
    db: Session = Depends(get_db),
):
    order = db.query(models.WorkOrder).filter(models.WorkOrder.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    if order.assigned_to_id != current_user.id:
        raise HTTPException(status_code=403, detail="This order is not assigned to you")
    if order.posting_status == "Completed":
        raise HTTPException(status_code=403, detail="This order is completed and locked for further edits")

    payload_data = payload.dict(exclude_unset=True)
    if "posting_status" in payload_data and payload_data["posting_status"] not in (
        "Completed", "In-Process", "Clarification"
    ):
        raise HTTPException(
            status_code=400,
            detail="posting_status must be 'Completed', 'In-Process', or 'Clarification'",
        )

    previous_status = order.posting_status

    # Pending $ and Posted Date are always system-derived, never accepted
    # directly from the client.
    payload_data.pop("pending_amount", None)
    payload_data.pop("posted_date", None)

    # Final posting_status after this update is applied (may be unchanged).
    final_status = payload_data.get("posting_status", order.posting_status)

    # Posted $, BAR Batch, and Trans Count are the core figures — nothing
    # else can be saved until all three are filled in (existing value or
    # part of this update).
    final_posted_amount = payload_data.get("posted_amount", order.posted_amount)
    final_bar_batch = payload_data.get("bar_batch", order.bar_batch)
    final_trans_count = payload_data.get("trans_count", order.trans_count)
    missing_core = []
    if final_posted_amount is None:
        missing_core.append("Posted $")
    if not final_bar_batch:
        missing_core.append("BAR Batch")
    if final_trans_count is None:
        missing_core.append("Trans Count")
    if missing_core:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot save — missing required: {', '.join(missing_core)}",
        )

    # Poster Comment, VENTRA Comment, Escalation Category, Issue Raised/Closed
    # Date only ever make sense while a Clarification is open — locked both
    # during In-Process and once Completed.
    CLARIFICATION_ONLY_FIELDS = [
        "poster_comment", "ventra_comment", "escalation_category",
        "issue_raised_date", "issue_closed_date",
    ]
    if final_status != "Clarification":
        attempted = [
            f for f in CLARIFICATION_ONLY_FIELDS
            if f in payload_data and payload_data[f] not in (None, "")
        ]
        if attempted:
            raise HTTPException(
                status_code=400,
                detail=f"{', '.join(attempted)} can only be edited while Posting Status is Clarification",
            )

    for field, value in payload_data.items():
        setattr(order, field, value)
    order.last_edited_by = current_user.username

    # Auto-set Issue Raised Date the moment a colleague flags Clarification,
    # if it isn't already set.
    if order.posting_status == "Clarification" and not order.issue_raised_date:
        order.issue_raised_date = date.today()

    # Auto-set Posted Date the moment a colleague marks Completed — no manual
    # entry needed, and it guarantees TAT can always be calculated below.
    if order.posting_status == "Completed" and not order.posted_date:
        order.posted_date = date.today()

    # Leaving Clarification for any other status clears the fields that only
    # make sense while a clarification is open.
    if previous_status == "Clarification" and order.posting_status != "Clarification":
        order.escalation_category = None
        order.issue_raised_date = None

    # Pending $ = Amount - Posted $, recalculated any time either changes.
    if order.amount is not None:
        order.pending_amount = order.amount - (order.posted_amount or 0)

    # Completing an order requires the core figures to already be filled in
    # (redundant with the check above, kept as a final guard), plus a Posted
    # Date — otherwise TAT can never be calculated and the row locks with it
    # permanently missing.
    if order.posting_status == "Completed":
        missing = []
        if order.posted_amount is None:
            missing.append("Posted $")
        if not order.bar_batch:
            missing.append("BAR Batch")
        if order.trans_count is None:
            missing.append("Trans Count")
        if not order.posted_date:
            missing.append("Posted Date")
        if missing:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot mark Completed — missing: {', '.join(missing)}",
            )

    # TAT = business days (Mon-Fri) between Received Date and Posted Date,
    # minus business days spent in an open Clarification pause window.
    if order.posted_date and order.received_date:
        total_bdays = _count_business_days(order.received_date, order.posted_date)
        pause_bdays = 0
        if order.issue_raised_date and order.issue_closed_date:
            pause_bdays = _count_business_days(order.issue_raised_date, order.issue_closed_date)
        order.tat_days = total_bdays - pause_bdays

    db.commit()
    db.refresh(order)

    if order.posting_status == "Completed":
        _auto_assign_open_slots(db)

    return order


@app.post("/orders/submit-day")
def submit_end_of_day(
    current_user: models.User = Depends(auth.require_role("colleague")),
    db: Session = Depends(get_db),
):
    """
    Moves all of this colleague's Completed orders to Production — hides
    them from both the colleague's and Team Lead's active queue for good.
    Only Completed orders are eligible; anything still In-Process or in
    Clarification is left untouched.
    """
    orders = (
        db.query(models.WorkOrder)
        .filter(
            models.WorkOrder.assigned_to_id == current_user.id,
            models.WorkOrder.posting_status == "Completed",
            models.WorkOrder.submitted == False,  # noqa: E712
        )
        .all()
    )
    now = datetime.utcnow()
    for order in orders:
        order.submitted = True
        order.submitted_at = now
    db.commit()
    return {"submitted": len(orders)}


@app.patch("/orders/{order_id}/team-lead-correction", response_model=schemas.WorkOrderOut)
def correct_completed_order(
    order_id: int,
    payload: schemas.TeamLeadCorrection,
    current_user: models.User = Depends(auth.require_role("team_lead")),
    db: Session = Depends(get_db),
):
    """
    Lets a Team Lead fix a colleague's mistake on a row that's Completed but
    not yet submitted to Production. Deliberately skips the Clarification-only
    and In-Process-locking rules that apply to colleagues — a Team Lead
    correction is trusted oversight, not routine data entry. Once the row is
    submitted, this endpoint refuses to touch it.
    """
    order = db.query(models.WorkOrder).filter(models.WorkOrder.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    if order.submitted:
        raise HTTPException(status_code=403, detail="This order has been submitted to Production and is locked")

    payload_data = payload.dict(exclude_unset=True)
    if "posting_status" in payload_data and payload_data["posting_status"] not in (
        "Completed", "In-Process", "Clarification"
    ):
        raise HTTPException(
            status_code=400,
            detail="posting_status must be 'Completed', 'In-Process', or 'Clarification'",
        )

    for field, value in payload_data.items():
        setattr(order, field, value)
    order.last_edited_by = current_user.username

    # Keep derived figures consistent with whatever the Team Lead just fixed.
    if order.amount is not None:
        order.pending_amount = order.amount - (order.posted_amount or 0)
    if order.posted_date and order.received_date:
        total_bdays = _count_business_days(order.received_date, order.posted_date)
        pause_bdays = 0
        if order.issue_raised_date and order.issue_closed_date:
            pause_bdays = _count_business_days(order.issue_raised_date, order.issue_closed_date)
        order.tat_days = total_bdays - pause_bdays

    db.commit()
    db.refresh(order)
    return order


def _count_business_days(start: date, end: date) -> int:
    """Counts weekdays (Mon-Fri) strictly after `start` up to and including `end`."""
    if not start or not end or end <= start:
        return 0
    count = 0
    current = start + timedelta(days=1)
    while current <= end:
        if current.weekday() < 5:  # 0=Mon ... 4=Fri
            count += 1
        current += timedelta(days=1)
    return count


# Serve the single-file frontend prototype
app.mount("/", StaticFiles(directory="static", html=True), name="static")
