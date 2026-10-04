from datetime import date, datetime, timedelta, timezone
from typing import List, Optional
from dateutil import parser as date_parser
from dateutil.relativedelta import relativedelta
import secrets

from fastapi import Depends, FastAPI, HTTPException, UploadFile, File, Form, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session, selectinload
from sqlalchemy import or_, and_, false as sa_false, func, inspect as sa_inspect, text as sa_text
import csv
import io
import re
import base64

import os

from . import models, schemas, auth, email_utils
from .database import Base, engine, get_db, SessionLocal

Base.metadata.create_all(bind=engine)


# Columns added to tables that already exist. create_all() only creates NEW
# tables, so each of these is added here if it's missing — safe to run on every
# start, which means a deploy no longer depends on someone running the ALTER by hand.
_ADDED_COLUMNS = [
    ("users", "last_login_date", "DATE"),
    ("daily_batch_stats", "pending_hours", "DOUBLE PRECISION"),
    ("daily_batch_stats", "hours_status", "VARCHAR"),
    ("daily_batch_stats", "hours_decided_by", "VARCHAR"),
    ("daily_batch_stats", "hours_decided_at", "TIMESTAMP"),
]


def _ensure_added_columns():
    try:
        insp = sa_inspect(engine)
        for table, column, ddl in _ADDED_COLUMNS:
            if table not in insp.get_table_names():
                continue
            if column in {c["name"] for c in insp.get_columns(table)}:
                continue
            with engine.begin() as conn:
                conn.execute(sa_text(f'ALTER TABLE {table} ADD COLUMN {column} {ddl}'))
    except Exception as exc:       # never block startup over this
        print(f"[startup] could not add missing columns automatically: {exc}")


_ensure_added_columns()

app = FastAPI(title="Work Order Allocation Tracker")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def no_cache_for_app_shell(request, call_next):
    """
    The whole frontend is one HTML file with inline JS/CSS, so if the browser
    caches it, deployed changes silently don't show up until a hard refresh.
    Force revalidation on the root/app shell every time; let genuinely
    static assets (the logo) cache normally.
    """
    response = await call_next(request)
    path = request.url.path
    if path == "/" or path == "/index.html":
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


@app.on_event("startup")
def bootstrap_and_seed():
    """
    Creates the very first login automatically, from environment variables,
    if no users exist yet — as super_admin, since only a super_admin can
    create further accounts. Also seeds the fixed process list (idempotent —
    safe to run on every startup).
    """
    db = SessionLocal()
    try:
        for name in models.PROCESS_NAMES:
            if not db.query(models.Process).filter(models.Process.name == name).first():
                db.add(models.Process(name=name))
        db.commit()

        # One-time starting client list (only when the clients table is empty).
        if db.query(models.Client).first() is None:
            from .client_seed import SEED_CLIENTS
            for facility_no, client_name in SEED_CLIENTS:
                db.add(models.Client(facility_no=facility_no, client_name=client_name, status="Active"))
            db.commit()

        if db.query(models.User).first() is None:
            username = os.getenv("BOOTSTRAP_USERNAME")
            password = os.getenv("BOOTSTRAP_PASSWORD")
            if username and password:
                admin = models.User(
                    username=username,
                    full_name="Super Admin",
                    role="super_admin",
                    password_hash=auth.hash_password(password),
                    must_change_password=True,
                )
                admin.processes = db.query(models.Process).all()
                db.add(admin)
                db.commit()
    finally:
        db.close()


def _user_has_process(user: models.User, process_id: int) -> bool:
    if user.role == "super_admin":
        return True
    return any(p.id == process_id for p in user.processes)


def _require_process_access(user: models.User, process_id: int):
    if not _user_has_process(user, process_id):
        raise HTTPException(status_code=403, detail="You don't have access to this process")


# ---------------------------------------------------------------------------
# Audit trail: Team Lead changes to locked / completed orders
# ---------------------------------------------------------------------------

# Columns that change as a side effect (or are pure noise) and would only
# clutter the "what changed" list.
AUDIT_SKIP_FIELDS = {"id", "last_edited_by", "updated_at", "timer_status", "timer_started_at", "time_taken_seconds"}


def _audit_value(v):
    return v.isoformat() if isinstance(v, (date, datetime)) else v


def _order_snapshot(order: models.WorkOrder) -> dict:
    return {
        c.name: _audit_value(getattr(order, c.name))
        for c in models.WorkOrder.__table__.columns
        if c.name not in AUDIT_SKIP_FIELDS
    }


def _order_lock_reasons(order: models.WorkOrder) -> list:
    """Why an order counts as locked/completed ([] = an ordinary open order)."""
    reasons = []
    if order.posting_status == "Completed":
        reasons.append("Completed")
    if order.escalated:
        reasons.append("Escalated (locked)")
    if order.submitted:
        reasons.append("Submitted to Production")
    return reasons


def _audit_applies(user: models.User) -> bool:
    """Whose changes are logged. Today: Team Leads (extend here to include others)."""
    return user.role == "team_lead"


def _log_order_change(db: Session, user: models.User, order: models.WorkOrder, action: str,
                      reasons: list, before: dict):
    """
    Adds one audit row. `before` is the snapshot taken BEFORE the change.
    For "delete" the key fields are recorded; otherwise only the fields that
    actually changed (a save that changes nothing logs nothing).
    """
    if action == "delete":
        changes = [
            {"field": k, "old": before.get(k), "new": None}
            for k in ("edm", "posting_status", "employee_name", "amount", "posted_amount", "posted_date")
            if before.get(k) not in (None, "")
        ]
    else:
        after = _order_snapshot(order)
        changes = [{"field": k, "old": before.get(k), "new": after.get(k)} for k in after if before.get(k) != after.get(k)]
        if not changes:
            return
    db.add(models.OrderChangeLog(
        created_at=datetime.now(IST).replace(tzinfo=None),
        process_id=order.process_id,
        order_id=order.id,
        edm=order.edm,
        employee_name=before.get("employee_name"),
        actor_id=user.id,
        actor_username=user.username,
        actor_name=user.full_name,
        actor_role=user.role,
        action=action,
        order_state=", ".join(reasons),
        changes=changes,
    ))


def _team_colleague_ids(db: Session, team_lead: models.User) -> list:
    """Ids of the colleagues who report to this Team Lead (matched on Reporting Manager)."""
    return [
        u.id for u in db.query(models.User.id).filter(
            models.User.role == "colleague",
            models.User.reporting_manager == team_lead.full_name,
        ).all()
    ]


def _team_orders_condition(db: Session, user: models.User, include_unowned: bool = False):
    """
    SQL condition limiting work orders to those a Team Lead may see data for:
    orders assigned to one of THEIR colleagues, plus not-yet-assigned orders
    that are theirs (team_lead_id = them; with include_unowned also the
    shared queue of orders no Team Lead owns yet). Super Admin: no limit
    (returns None). Other Team Leads' colleagues' orders never match.
    """
    if user.role != "team_lead":
        return None
    ids = _team_colleague_ids(db, user)
    owned = [models.WorkOrder.team_lead_id == user.id]
    if include_unowned:
        owned.append(models.WorkOrder.team_lead_id.is_(None))
    return or_(
        models.WorkOrder.assigned_to_id.in_(ids) if ids else sa_false(),
        and_(models.WorkOrder.assigned_to_id.is_(None), or_(*owned)),
    )


def _finalize_timer(order: models.WorkOrder, new_status: str):
    """
    Rolls the current running session's elapsed time into
    time_taken_seconds and stops the clock. Used whenever a row leaves
    active colleague control — Pause, Complete (new_status='stopped'),
    or getting locked by an escalation (new_status='paused', so it
    resumes cleanly once handed back). No-op if the timer isn't running.
    """
    if order.timer_status == "running" and order.timer_started_at:
        now = datetime.now(IST).replace(tzinfo=None)
        elapsed = (now - order.timer_started_at).total_seconds()
        if elapsed > 0:
            order.time_taken_seconds = (order.time_taken_seconds or 0) + int(elapsed)
    order.timer_status = new_status
    order.timer_started_at = None


def _start_timer(order: models.WorkOrder):
    """Called whenever an order is freshly (re)assigned — resets the clock to zero and starts it running."""
    order.timer_status = "running"
    order.timer_started_at = datetime.now(IST).replace(tzinfo=None)
    order.time_taken_seconds = 0


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

@app.post("/auth/login", response_model=schemas.LoginResponse)
def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    process_id: Optional[int] = Form(None),
    db: Session = Depends(get_db),
):
    user = db.query(models.User).filter(models.User.username == form_data.username).first()
    if not user or not auth.verify_password(form_data.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Incorrect username or password")
    if user.employment_status == "Inactive":
        raise HTTPException(status_code=403, detail="This account is inactive")
    # Quality (view/export Production only) can open any process; everyone else needs it assigned.
    if process_id is not None and user.role != "quality" and not _user_has_process(user, process_id):
        raise HTTPException(status_code=403, detail="You don't have access to that process")
    token = auth.create_access_token({"sub": user.username})
    processes = db.query(models.Process).all() if user.role in ("super_admin", "quality") else user.processes
    if user.role == "colleague":
        user.last_login_date = datetime.now(IST).date()     # logged in today -> eligible for auto-assignment
        db.commit()
        for p in processes:
            _auto_assign_open_slots(db, p.id)
    return {
        "access_token": token,
        "token_type": "bearer",
        "id": user.id,
        "role": user.role,
        "full_name": user.full_name,
        "must_change_password": user.must_change_password,
        "processes": processes,
    }

@app.post("/auth/change-password")
def change_password(
    payload: schemas.ChangePasswordRequest,
    current_user: models.User = Depends(auth.get_current_user),
    db: Session = Depends(get_db),
):
    if not auth.verify_password(payload.old_password, current_user.password_hash):
        raise HTTPException(status_code=401, detail="Current password is incorrect")
    if len(payload.new_password) < 8:
        raise HTTPException(status_code=400, detail="New password must be at least 8 characters")
    current_user.password_hash = auth.hash_password(payload.new_password)
    current_user.must_change_password = False
    db.commit()
    return {"status": "password changed"}


@app.get("/auth/me", response_model=schemas.UserOut)
def read_me(current_user: models.User = Depends(auth.get_current_user)):
    return current_user


@app.put("/auth/me/profile", response_model=schemas.UserOut)
def update_my_profile(
    payload: schemas.ProfileDatesUpdate,
    current_user: models.User = Depends(auth.require_role("colleague", "team_lead", "admin", "quality")),
    db: Session = Depends(get_db),
):
    """
    Self-service profile edit for Colleague, Team Lead, Admin and Quality:
    ONLY Date of Birth and Anniversary Date. Everything else on the profile
    (name, role, email, designation, reporting manager, DOJ, processes ...)
    stays Super Admin-only, and no other field is read from this request.
    """
    today = datetime.now(IST).date()
    data = payload.dict(exclude_unset=True)
    for field, label in (("dob", "Date of Birth"), ("anniversary_date", "Anniversary Date")):
        v = data.get(field)
        if v is not None and v > today:
            raise HTTPException(status_code=400, detail=f"{label} can't be in the future")
    if "dob" in data:
        current_user.dob = data["dob"]
    if "anniversary_date" in data:
        current_user.anniversary_date = data["anniversary_date"]
    db.add(current_user)
    db.commit()
    db.refresh(current_user)
    return current_user


DEFAULT_SESSION_TIMEOUT_MINUTES = 60
SESSION_TIMEOUT_SETTING_KEY = "session_timeout_minutes"


@app.get("/settings/session-timeout", response_model=schemas.SessionTimeoutSetting)
def get_session_timeout(
    current_user: models.User = Depends(auth.get_current_user),
    db: Session = Depends(get_db),
):
    """Any logged-in user can read this — the frontend uses it to run the
    inactivity auto-logout timer."""
    setting = db.query(models.AppSetting).filter(models.AppSetting.key == SESSION_TIMEOUT_SETTING_KEY).first()
    return {"minutes": int(setting.value) if setting else DEFAULT_SESSION_TIMEOUT_MINUTES}


@app.put("/settings/session-timeout", response_model=schemas.SessionTimeoutSetting)
def set_session_timeout(
    payload: schemas.SessionTimeoutSetting,
    current_user: models.User = Depends(auth.require_role("super_admin")),
    db: Session = Depends(get_db),
):
    if payload.minutes < 1:
        raise HTTPException(status_code=400, detail="Timeout must be at least 1 minute")
    setting = db.query(models.AppSetting).filter(models.AppSetting.key == SESSION_TIMEOUT_SETTING_KEY).first()
    if setting:
        setting.value = str(payload.minutes)
    else:
        setting = models.AppSetting(key=SESSION_TIMEOUT_SETTING_KEY, value=str(payload.minutes))
        db.add(setting)
    db.commit()
    return {"minutes": payload.minutes}


@app.post("/auth/forgot-username")
def forgot_username(payload: schemas.ForgotUsernameRequest, db: Session = Depends(get_db)):
    """
    Always returns the same generic message regardless of whether the email
    matches an account — avoids leaking which addresses are registered.
    """
    user = db.query(models.User).filter(models.User.email == payload.email).first()
    if user:
        email_utils.send_username_reminder_email(user.email, user.username)
    return {"message": "If that email is on file, we've sent the username to it."}


@app.post("/auth/forgot-password")
def forgot_password(payload: schemas.ForgotPasswordRequest, db: Session = Depends(get_db)):
    """Same generic-response principle as forgot-username, for the same reason."""
    identifier = payload.username_or_email
    user = (
        db.query(models.User)
        .filter((models.User.username == identifier) | (models.User.email == identifier))
        .first()
    )
    if user and user.email:
        user.reset_token = secrets.token_urlsafe(32)
        user.reset_token_expires = datetime.utcnow() + timedelta(hours=1)
        db.commit()
        email_utils.send_password_reset_email(user.email, user.username, user.reset_token)
    return {"message": "If that account exists and has an email on file, we've sent reset instructions."}


@app.post("/auth/reset-password-with-token")
def reset_password_with_token(payload: schemas.ResetPasswordWithTokenRequest, db: Session = Depends(get_db)):
    user = db.query(models.User).filter(models.User.reset_token == payload.token).first()
    if not user or not user.reset_token_expires or user.reset_token_expires < datetime.utcnow():
        raise HTTPException(status_code=400, detail="This reset link is invalid or has expired")
    if len(payload.new_password) < 8:
        raise HTTPException(status_code=400, detail="New password must be at least 8 characters")
    user.password_hash = auth.hash_password(payload.new_password)
    user.must_change_password = False
    user.reset_token = None
    user.reset_token_expires = None
    db.commit()
    return {"status": "password reset — you can now log in with your new password"}


# ---------------------------------------------------------------------------
# User management — Super Admin only
# ---------------------------------------------------------------------------

@app.get("/users/team-leads", response_model=List[schemas.UserOut])
def list_team_leads(
    process_id: int,
    current_user: models.User = Depends(auth.require_role("admin", "team_lead", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    Team Leads who have access to this process. Originally Admin-only (so an
    Admin can import inventory on a specific absent Team Lead's behalf) —
    now also used by a Team Lead to pick a destination when transferring
    orders to another Team Lead, and by a Super Admin doing the same on a
    Team Lead's behalf.
    """
    _require_process_access(current_user, process_id)
    return (
        db.query(models.User)
        .filter(models.User.role == "team_lead", models.User.processes.any(models.Process.id == process_id))
        .order_by(models.User.full_name.asc())
        .all()
    )
    
@app.get("/users", response_model=List[schemas.UserOut])
def list_users(
    current_user: models.User = Depends(auth.require_role("super_admin")),
    db: Session = Depends(get_db),
):
    return db.query(models.User).order_by(models.User.full_name.asc()).all()


@app.post("/users", response_model=schemas.CreateUserResponse)
def create_user(
    payload: schemas.CreateUserRequest,
    current_user: models.User = Depends(auth.require_role("super_admin")),
    db: Session = Depends(get_db),
):
    """
    Only a Super Admin can create profiles. No email is sent (not configured
    for this deployment) — the temporary password is returned in this
    response so the Super Admin can relay it to the new user directly.
    """
    if payload.role not in ("colleague", "team_lead", "admin", "quality", "super_admin"):
        raise HTTPException(status_code=400, detail="role must be 'colleague', 'team_lead', 'admin', 'quality', or 'super_admin'")
    if db.query(models.User).filter(models.User.username == payload.username).first():
        raise HTTPException(status_code=400, detail="username already exists")

    temp_password = auth.generate_temp_password()
    user = models.User(
        username=payload.username,
        full_name=payload.full_name,
        role=payload.role,
        password_hash=auth.hash_password(temp_password),
        must_change_password=True,
        email=payload.email,
        dob=payload.dob,
        doj=payload.doj,
        anniversary_date=payload.anniversary_date,
        designation=payload.designation,
        reporting_manager=payload.reporting_manager,
        employment_status=payload.employment_status,
        employee_id=payload.employee_id,
    )
    if payload.process_ids:
        user.processes = db.query(models.Process).filter(models.Process.id.in_(payload.process_ids)).all()
    db.add(user)
    db.commit()
    db.refresh(user)
    if user.email:
        email_utils.send_new_account_email(user.email, user.full_name, user.username, temp_password)
    return {"user": user, "temporary_password": temp_password}


@app.patch("/users/{user_id}", response_model=schemas.UserOut)
def update_user(
    user_id: int,
    payload: schemas.CreateUserRequest,
    current_user: models.User = Depends(auth.require_role("super_admin")),
    db: Session = Depends(get_db),
):
    """Edit an existing profile — role, employment status, processes, etc."""
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if payload.role not in ("colleague", "team_lead", "admin", "quality", "super_admin"):
        raise HTTPException(status_code=400, detail="role must be 'colleague', 'team_lead', 'admin', 'quality', or 'super_admin'")

    user.full_name = payload.full_name
    user.role = payload.role
    user.email = payload.email
    user.dob = payload.dob
    user.doj = payload.doj
    user.anniversary_date = payload.anniversary_date
    user.designation = payload.designation
    user.reporting_manager = payload.reporting_manager
    user.employment_status = payload.employment_status
    user.employee_id = payload.employee_id
    user.processes = db.query(models.Process).filter(models.Process.id.in_(payload.process_ids)).all()
    db.commit()
    db.refresh(user)
    return user


@app.post("/users/{user_id}/reset-password", response_model=schemas.ResetPasswordResponse)
def reset_password(
    user_id: int,
    current_user: models.User = Depends(auth.require_role("super_admin")),
    db: Session = Depends(get_db),
):
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    temp_password = auth.generate_temp_password()
    user.password_hash = auth.hash_password(temp_password)
    user.must_change_password = True
    db.commit()
    if user.email:
        email_utils.send_new_account_email(user.email, user.full_name, user.username, temp_password)
    return {"username": user.username, "temporary_password": temp_password}

@app.get("/processes/public", response_model=List[schemas.ProcessOut])
def list_processes_public(db: Session = Depends(get_db)):
    """Unauthenticated — only exposes process names, used to populate the login page's process dropdown."""
    return db.query(models.Process).order_by(models.Process.name.asc()).all()

@app.get("/processes", response_model=List[schemas.ProcessOut])
def list_all_processes(
    current_user: models.User = Depends(auth.require_role("super_admin")),
    db: Session = Depends(get_db),
):
    """Full process list — for the Super Admin's user-creation form."""
    return db.query(models.Process).order_by(models.Process.name.asc()).all()


@app.post("/processes", response_model=schemas.ProcessOut)
def create_process(
    payload: schemas.ProcessCreate,
    current_user: models.User = Depends(auth.require_role("super_admin")),
    db: Session = Depends(get_db),
):
    """
    Adds a new process beyond the original fixed nine — lets a Super Admin
    grow the list as the org's work expands, with an optional daily
    production target attached.
    """
    name = payload.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Process name is required")
    if db.query(models.Process).filter(models.Process.name == name).first():
        raise HTTPException(status_code=400, detail="A process with that name already exists")
    process = models.Process(name=name, daily_target=payload.daily_target)
    db.add(process)
    db.commit()
    db.refresh(process)
    return process


@app.patch("/processes/{process_id}", response_model=schemas.ProcessOut)
def update_process(
    process_id: int,
    payload: schemas.ProcessUpdate,
    current_user: models.User = Depends(auth.require_role("super_admin")),
    db: Session = Depends(get_db),
):
    """Rename a process and/or change its daily target."""
    process = db.query(models.Process).filter(models.Process.id == process_id).first()
    if not process:
        raise HTTPException(status_code=404, detail="Process not found")
    name = payload.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Process name is required")
    duplicate = (
        db.query(models.Process)
        .filter(models.Process.name == name, models.Process.id != process_id)
        .first()
    )
    if duplicate:
        raise HTTPException(status_code=400, detail="A process with that name already exists")
    process.name = name
    process.daily_target = payload.daily_target
    db.commit()
    db.refresh(process)
    return process


# ---------------------------------------------------------------------------
# Today's Celebrations (org-wide birthdays/anniversaries) + Process Updates
# ---------------------------------------------------------------------------

IST = timezone(timedelta(hours=5, minutes=30))
REACTION_TYPES = ("like", "heart")
UPDATE_MODES = ("Team message", "Email", "Smartsheet", "Call")
UPDATE_CATEGORIES = ("Payer", "Adjustment", "Generic")
UPDATE_STATUSES = ("Active", "Inactive")


@app.get("/celebrations/today", response_model=List[schemas.CelebrationPerson])
def todays_celebrations(
    current_user: models.User = Depends(auth.get_current_user),
    db: Session = Depends(get_db),
):
    """
    Every active user, across the whole org (any role, any process), whose
    birthday or work anniversary falls on today's calendar date. Open to
    anyone logged in — this isn't process-scoped. Returns an empty list on
    any day nobody's celebrating, so the frontend can hide the section
    entirely rather than show an empty state.
    """
    # "Today" has to mean today in India, not on the server — the server
    # (Render) runs in UTC, so for roughly the first 5.5 hours of every
    # IST day, date.today() would still report yesterday's date, either
    # delaying a real celebration or holding yesterday's over too long.
    today = datetime.now(IST).date()
    users = db.query(models.User).filter(models.User.employment_status == "Active").all()
    people_by_id = {}
    for u in users:
        occasions = []
        if u.dob and u.dob.month == today.month and u.dob.day == today.day:
            occasions.append({"kind": "birthday", "years": None})
        # Work anniversary (Date of Joining) and Anniversary Date are two
        # independent fields — check each on its own rather than one
        # falling back to the other, so both can show (as separate badges
        # on the same merged card) if they land on the same day.
        if u.doj and u.doj.month == today.month and u.doj.day == today.day:
            years = today.year - u.doj.year
            occasions.append({"kind": "work_anniversary", "years": years if years > 0 else None})
        if u.anniversary_date and u.anniversary_date.month == today.month and u.anniversary_date.day == today.day:
            years = today.year - u.anniversary_date.year
            occasions.append({"kind": "anniversary", "years": years if years > 0 else None})
        if occasions:
            # A birthday and one or more anniversaries can legitimately
            # land on the same day for one person — one card, all
            # occasion tags together, one shared comment thread, instead
            # of duplicate cards with the same comments/reactions
            # showing under each.
            people_by_id[u.id] = {"user_id": u.id, "full_name": u.full_name, "occasions": occasions}

    if not people_by_id:
        return []

    people = list(people_by_id.values())
    target_ids = list(people_by_id.keys())
    comments = (
        db.query(models.CelebrationComment)
        .filter(
            models.CelebrationComment.target_user_id.in_(target_ids),
            models.CelebrationComment.occasion_date == today,
        )
        .order_by(models.CelebrationComment.created_at.asc())
        .all()
    )
    comments_by_target = {}
    for c in comments:
        comments_by_target.setdefault(c.target_user_id, []).append(c)

    comment_ids = [c.id for c in comments]
    reactions_by_comment = {}
    if comment_ids:
        reactions = (
            db.query(models.CelebrationReaction)
            .filter(models.CelebrationReaction.comment_id.in_(comment_ids))
            .all()
        )
        for r in reactions:
            reactions_by_comment.setdefault(r.comment_id, []).append(r)

    for p in people:
        target_comments = comments_by_target.get(p["user_id"], [])
        enriched = []
        for c in target_comments:
            c_reactions = reactions_by_comment.get(c.id, [])
            counts = {t: 0 for t in REACTION_TYPES}
            my_reactions = []
            for r in c_reactions:
                counts[r.reaction] = counts.get(r.reaction, 0) + 1
                if r.posted_by_id == current_user.id:
                    my_reactions.append(r.reaction)
            enriched.append({
                "id": c.id, "target_user_id": c.target_user_id, "message": c.message,
                "posted_by_name": c.posted_by_name, "created_at": c.created_at,
                "reaction_counts": counts, "my_reactions": my_reactions,
            })
        p["comments"] = enriched
    return people


@app.post("/celebrations/{target_user_id}/comments", response_model=schemas.CelebrationCommentOut)
def add_celebration_comment(
    target_user_id: int,
    payload: schemas.CelebrationCommentCreate,
    current_user: models.User = Depends(auth.get_current_user),
    db: Session = Depends(get_db),
):
    message = payload.message.strip()
    if not message:
        raise HTTPException(status_code=400, detail="Comment can't be empty")
    target = db.query(models.User).filter(models.User.id == target_user_id).first()
    if not target:
        raise HTTPException(status_code=404, detail="User not found")
    comment = models.CelebrationComment(
        target_user_id=target_user_id,
        occasion_date=datetime.now(IST).date(),
        message=message,
        posted_by_id=current_user.id,
        posted_by_name=current_user.full_name,
    )
    db.add(comment)
    db.commit()
    db.refresh(comment)
    return {
        "id": comment.id, "target_user_id": comment.target_user_id, "message": comment.message,
        "posted_by_name": comment.posted_by_name, "created_at": comment.created_at,
        "reaction_counts": {t: 0 for t in REACTION_TYPES}, "my_reactions": [],
    }


@app.post("/celebrations/comments/{comment_id}/react")
def react_to_celebration_comment(
    comment_id: int,
    payload: schemas.CelebrationReactRequest,
    current_user: models.User = Depends(auth.get_current_user),
    db: Session = Depends(get_db),
):
    """Toggles a like/heart on a specific comment — clicking it again removes it."""
    if payload.reaction not in REACTION_TYPES:
        raise HTTPException(status_code=400, detail=f"reaction must be one of {REACTION_TYPES}")
    comment = db.query(models.CelebrationComment).filter(models.CelebrationComment.id == comment_id).first()
    if not comment:
        raise HTTPException(status_code=404, detail="Comment not found")

    existing = (
        db.query(models.CelebrationReaction)
        .filter(
            models.CelebrationReaction.comment_id == comment_id,
            models.CelebrationReaction.posted_by_id == current_user.id,
            models.CelebrationReaction.reaction == payload.reaction,
        )
        .first()
    )
    if existing:
        db.delete(existing)
        db.commit()
        return {"status": "removed"}

    db.add(models.CelebrationReaction(
        comment_id=comment_id,
        reaction=payload.reaction,
        posted_by_id=current_user.id,
        posted_by_name=current_user.full_name,
    ))
    db.commit()
    return {"status": "added"}


@app.get("/process-updates", response_model=List[schemas.ProcessUpdateOut])
def list_process_updates(
    process_id: int,
    current_user: models.User = Depends(auth.get_current_user),
    db: Session = Depends(get_db),
):
    """Newest first — the process screen shows the latest one most prominently."""
    _require_process_access(current_user, process_id)
    return (
        db.query(models.ProcessUpdate)
        .filter(models.ProcessUpdate.process_id == process_id)
        .order_by(models.ProcessUpdate.created_at.desc())
        .limit(20)
        .all()
    )


@app.post("/process-updates", response_model=List[schemas.ProcessUpdateOut])
def create_process_update(
    payload: schemas.ProcessUpdateCreate,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "super_admin")),
    db: Session = Depends(get_db),
):
    if not payload.process_ids:
        raise HTTPException(status_code=400, detail="Select at least one process")
    for pid in payload.process_ids:
        _require_process_access(current_user, pid)

    message = payload.message.strip()
    if not message:
        raise HTTPException(status_code=400, detail="Update comment can't be empty")
    if payload.mode and payload.mode not in UPDATE_MODES:
        raise HTTPException(status_code=400, detail=f"mode must be one of {UPDATE_MODES}")
    if payload.category and payload.category not in UPDATE_CATEGORIES:
        raise HTTPException(status_code=400, detail=f"category must be one of {UPDATE_CATEGORIES}")
    if payload.status not in UPDATE_STATUSES:
        raise HTTPException(status_code=400, detail=f"status must be one of {UPDATE_STATUSES}")

    created = []
    for pid in payload.process_ids:
        update = models.ProcessUpdate(
            process_id=pid,
            received_date=payload.received_date,
            mode=payload.mode,
            received_from=(payload.received_from.strip() if payload.received_from else None),
            category=payload.category,
            status=payload.status,
            message=message,
            verified_by=(payload.verified_by.strip() if payload.verified_by else None),
            posted_by_id=current_user.id,
            posted_by_name=current_user.full_name,
            posted_by_role=current_user.role,
        )
        db.add(update)
        created.append(update)
    db.commit()
    for u in created:
        db.refresh(u)
    return created


@app.patch("/process-updates/{update_id}", response_model=schemas.ProcessUpdateOut)
def edit_process_update(
    update_id: int,
    payload: schemas.ProcessUpdateEdit,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    Same posting roles can edit, not just the original author — this is
    shared operational logging (like a process log), not private content.
    Which process(es) it's posted to isn't editable; if it was posted to
    several, each process's copy is edited independently.
    """
    update = db.query(models.ProcessUpdate).filter(models.ProcessUpdate.id == update_id).first()
    if not update:
        raise HTTPException(status_code=404, detail="Update not found")
    _require_process_access(current_user, update.process_id)

    message = payload.message.strip()
    if not message:
        raise HTTPException(status_code=400, detail="Update comment can't be empty")
    if payload.mode and payload.mode not in UPDATE_MODES:
        raise HTTPException(status_code=400, detail=f"mode must be one of {UPDATE_MODES}")
    if payload.category and payload.category not in UPDATE_CATEGORIES:
        raise HTTPException(status_code=400, detail=f"category must be one of {UPDATE_CATEGORIES}")
    if payload.status not in UPDATE_STATUSES:
        raise HTTPException(status_code=400, detail=f"status must be one of {UPDATE_STATUSES}")

    update.received_date = payload.received_date
    update.mode = payload.mode
    update.received_from = (payload.received_from.strip() if payload.received_from else None)
    update.category = payload.category
    update.status = payload.status
    update.message = message
    update.verified_by = (payload.verified_by.strip() if payload.verified_by else None)
    update.updated_at = datetime.utcnow()
    update.updated_by_name = current_user.full_name
    update.updated_by_role = current_user.role
    db.commit()
    db.refresh(update)
    return update


MAX_ATTACHMENT_BYTES = 5 * 1024 * 1024  # 5 MB per file
MAX_ATTACHMENTS_PER_UPDATE = 5


@app.post("/process-updates/{update_id}/attachments", response_model=List[schemas.ProcessUpdateAttachmentOut])
def upload_process_update_attachments(
    update_id: int,
    files: List[UploadFile] = File(...),
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    Attaches one or more images/documents to an existing Process Update.
    Stored inline as base64 (see ProcessUpdateAttachment) since there's no
    object storage configured — capped in size and count to keep that
    reasonable.
    """
    update = db.query(models.ProcessUpdate).filter(models.ProcessUpdate.id == update_id).first()
    if not update:
        raise HTTPException(status_code=404, detail="Update not found")
    _require_process_access(current_user, update.process_id)

    existing_count = (
        db.query(models.ProcessUpdateAttachment)
        .filter(models.ProcessUpdateAttachment.process_update_id == update_id)
        .count()
    )
    if existing_count + len(files) > MAX_ATTACHMENTS_PER_UPDATE:
        raise HTTPException(
            status_code=400,
            detail=f"Max {MAX_ATTACHMENTS_PER_UPDATE} attachments per update ({existing_count} already there)",
        )

    created = []
    for f in files:
        content = f.file.read()
        if len(content) > MAX_ATTACHMENT_BYTES:
            raise HTTPException(status_code=400, detail=f"{f.filename} is over the 5 MB limit")
        attachment = models.ProcessUpdateAttachment(
            process_update_id=update_id,
            file_name=f.filename,
            content_type=f.content_type,
            file_data=base64.b64encode(content).decode("ascii"),
            uploaded_by_name=current_user.full_name,
        )
        db.add(attachment)
        created.append(attachment)
    db.commit()
    for a in created:
        db.refresh(a)
    return created


@app.get("/process-updates/attachments/{attachment_id}")
def get_process_update_attachment(
    attachment_id: int,
    current_user: models.User = Depends(auth.get_current_user),
    db: Session = Depends(get_db),
):
    """
    Serves the raw file so the frontend can open/download it. Requires the
    same Bearer auth as everything else, so the frontend fetches this as a
    blob (via authFetch) rather than linking to it directly — a plain
    anchor click wouldn't carry the Authorization header.
    """
    attachment = (
        db.query(models.ProcessUpdateAttachment)
        .filter(models.ProcessUpdateAttachment.id == attachment_id)
        .first()
    )
    if not attachment:
        raise HTTPException(status_code=404, detail="Attachment not found")
    update = db.query(models.ProcessUpdate).filter(models.ProcessUpdate.id == attachment.process_update_id).first()
    if not update:
        raise HTTPException(status_code=404, detail="Update not found")
    _require_process_access(current_user, update.process_id)

    content = base64.b64decode(attachment.file_data)
    return Response(
        content=content,
        media_type=attachment.content_type or "application/octet-stream",
        headers={"Content-Disposition": f'inline; filename="{attachment.file_name}"'},
    )


@app.get("/users/colleagues", response_model=List[schemas.UserOut])
def list_colleagues(
    process_id: int,
    current_user: models.User = Depends(auth.require_role("team_lead", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    Colleagues who have access to this process — populates the Reassign
    dropdown. A Team Lead only ever gets their OWN colleagues (those whose
    Reporting Manager is them); a Super Admin gets everyone.
    """
    _require_process_access(current_user, process_id)
    q = db.query(models.User).filter(
        models.User.role == "colleague", models.User.processes.any(models.Process.id == process_id)
    )
    if current_user.role == "team_lead":
        q = q.filter(models.User.reporting_manager == current_user.full_name)
    return q.order_by(models.User.full_name.asc()).all()


@app.post("/orders/run-assignment")
def run_assignment(
    process_id: int,
    current_user: models.User = Depends(auth.require_role("team_lead", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    Manually re-runs the auto-assignment pass for one process. Use this any
    time a colleague ends up with no open order and needs to be caught up —
    e.g. right after adding a colleague to this process, in case it wasn't
    picked up automatically.
    """
    _require_process_access(current_user, process_id)
    _auto_assign_open_slots(db, process_id)
    return {"status": "assignment pass complete"}


@app.patch("/orders/{order_id}/reassign", response_model=schemas.WorkOrderOut)
def reassign_order(
    order_id: int,
    payload: schemas.ReassignRequest,
    current_user: models.User = Depends(auth.require_role("team_lead", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    Manually reassign an In-Process (or Clarification) order to a different
    colleague. Not available once an order is Completed — use the
    team-lead-correction flow for a completed-but-unsubmitted row instead,
    and reassignment is meaningless once it's been submitted to Production.
    """
    order = db.query(models.WorkOrder).filter(models.WorkOrder.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    _require_process_access(current_user, order.process_id)
    if order.submitted:
        raise HTTPException(status_code=403, detail="This order has been submitted to Production")
    if order.posting_status == "Completed":
        raise HTTPException(status_code=403, detail="Completed orders can't be reassigned this way")

    if current_user.role == "team_lead":
        # Only orders in this Team Lead's own queue (theirs, or not yet owned
        # by any Team Lead) can be reassigned...
        if order.team_lead_id not in (None, current_user.id):
            raise HTTPException(status_code=403, detail="This order belongs to another Team Lead")

    new_colleague_q = db.query(models.User).filter(
        models.User.id == payload.assigned_to_id,
        models.User.role == "colleague",
        models.User.processes.any(models.Process.id == order.process_id),
    )
    if current_user.role == "team_lead":
        # ...and only to colleagues who report to them.
        new_colleague_q = new_colleague_q.filter(models.User.reporting_manager == current_user.full_name)
    new_colleague = new_colleague_q.first()
    if not new_colleague:
        raise HTTPException(status_code=400, detail="Not a valid colleague for this process")

    reasons = _order_lock_reasons(order) if _audit_applies(current_user) else []
    before = _order_snapshot(order) if reasons else None

    order.assigned_to_id = new_colleague.id
    order.assigned_date = datetime.now(IST).date()
    order.employee_id = new_colleague.employee_id or new_colleague.username
    order.employee_name = new_colleague.full_name
    order.last_edited_by = current_user.username
    _start_timer(order)
    if reasons:
        _log_order_change(db, current_user, order, "reassign", reasons, before)
    db.commit()
    db.refresh(order)
    return order


@app.post("/orders/transfer")
def transfer_orders(
    process_id: int,
    payload: schemas.TransferOrdersRequest,
    current_user: models.User = Depends(auth.require_role("team_lead", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    Moves a batch of not-yet-completed, not-yet-submitted orders from one
    Team Lead's queue to another's. A Team Lead transfers their own queue;
    a Super Admin must specify from_team_lead_id. Transferred orders are
    fully unassigned (posting data already entered is kept, but the
    colleague assignment is cleared) and immediately re-offered to the
    destination Team Lead's colleagues via the normal auto-assign pass.
    Omitting order_ids transfers the whole eligible queue; passing it
    transfers just those rows.
    """
    _require_process_access(current_user, process_id)

    if current_user.role == "team_lead":
        from_team_lead_id = current_user.id
    else:
        if not payload.from_team_lead_id:
            raise HTTPException(status_code=400, detail="from_team_lead_id is required for a Super Admin transfer")
        from_team_lead_id = payload.from_team_lead_id

    to_lead = (
        db.query(models.User)
        .filter(
            models.User.id == payload.to_team_lead_id,
            models.User.role == "team_lead",
            models.User.processes.any(models.Process.id == process_id),
        )
        .first()
    )
    if not to_lead:
        raise HTTPException(status_code=400, detail="Not a valid Team Lead for this process")
    if to_lead.id == from_team_lead_id:
        raise HTTPException(status_code=400, detail="Source and destination Team Lead are the same")

    query = db.query(models.WorkOrder).filter(
        models.WorkOrder.process_id == process_id,
        models.WorkOrder.team_lead_id == from_team_lead_id,
        models.WorkOrder.submitted == False,  # noqa: E712
        or_(
            models.WorkOrder.posting_status != "Completed",
            models.WorkOrder.posting_status.is_(None),
        ),
    )
    if payload.order_ids:
        query = query.filter(models.WorkOrder.id.in_(payload.order_ids))
    orders = query.all()

    for order in orders:
        order.team_lead_id = to_lead.id
        order.assigned_to_id = None
        order.assigned_date = None
        order.employee_id = None
        order.employee_name = None
        order.posting_status = None
        order.last_edited_by = current_user.username

    db.commit()
    _auto_assign_open_slots(db, process_id)
    return {"transferred": len(orders)}


@app.delete("/orders/all")
def delete_all_orders(
    process_id: int,
    current_user: models.User = Depends(auth.require_role("team_lead", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    Deletes every work order in this process — used to clear test/imported
    data for a fresh run. Does NOT touch user accounts, and does not affect
    other processes' data. Registered before /orders/{order_id} on purpose:
    a literal path has to come before a dynamic {order_id}: int path, or
    "all" gets swallowed as an attempted (and invalid) order_id — same
    routing gotcha as /orders/escalations earlier.
    """
    _require_process_access(current_user, process_id)
    if _audit_applies(current_user):
        # Record every locked / completed order that is about to disappear.
        for o in db.query(models.WorkOrder).filter(
            models.WorkOrder.process_id == process_id,
            or_(models.WorkOrder.posting_status == "Completed",
                models.WorkOrder.escalated == True,  # noqa: E712
                models.WorkOrder.submitted == True),  # noqa: E712
        ).all():
            _log_order_change(db, current_user, o, "delete", _order_lock_reasons(o), _order_snapshot(o))
    # A bulk .delete() skips the ORM cascade, so the child rows (clarification /
    # escalation details) have to go first or Postgres rejects the delete with a
    # foreign-key error (the "Internal Server Error").
    ids = db.query(models.WorkOrder.id).filter(models.WorkOrder.process_id == process_id)
    db.query(models.ClarificationDetail).filter(models.ClarificationDetail.order_id.in_(ids)).delete(synchronize_session=False)
    db.query(models.EscalationDetail).filter(models.EscalationDetail.order_id.in_(ids)).delete(synchronize_session=False)
    deleted_count = db.query(models.WorkOrder).filter(models.WorkOrder.process_id == process_id).delete(synchronize_session=False)
    db.commit()
    return {"deleted": deleted_count}


@app.delete("/orders/{order_id}")
def delete_one_order(
    order_id: int,
    current_user: models.User = Depends(auth.require_role("team_lead", "super_admin")),
    db: Session = Depends(get_db),
):
    """Deletes a single order — for cleaning up a bad test/import row without wiping the whole queue."""
    order = db.query(models.WorkOrder).filter(models.WorkOrder.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    _require_process_access(current_user, order.process_id)
    reasons = _order_lock_reasons(order) if _audit_applies(current_user) else []
    if reasons:
        _log_order_change(db, current_user, order, "delete", reasons, _order_snapshot(order))
    db.delete(order)
    db.commit()
    return {"deleted": 1}


# ---------------------------------------------------------------------------
# Inventory import (E-O) — Team Lead bulk-loads from the existing Excel export
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Inventory import rules
# ---------------------------------------------------------------------------

# EDM process: only these Def Doc Types are imported (compared ignoring case / extra spaces).
EDM_ALLOWED_DOC_TYPES = {"manual eft eob", "lockbox payment", "insurance credit card pmt only", "insurance credit card pmt"}
# Rows in this Bar Grp are never imported (any process).
SKIP_BAR_GRPS = {"grp - 4 gottlieb-sound phys"}

# Original inventory headers (Batch Manager export) -> internal field names.
_HEADER_ALIASES = {
    "batch": "edm", "edm": "edm", "edm#": "edm",
    "status": "status", "created": "created",
    "images": "image_count", "image count": "image_count", "image_count": "image_count",
    "docs": "doc_count", "doc count": "doc_count", "doc_count": "doc_count",
    "def doc type": "def_doc_type", "def_doc_type": "def_doc_type",
    "amount": "amount", "description": "description",
    "division": "division", "deposit date": "deposit_date", "deposit_date": "deposit_date",
    "bar grp": "bar_grp", "bar_grp": "bar_grp",
    "last edited by": "last_edited_by", "scanned date": "scanned_date", "scanned batch": "scanned_batch",
}


def _cell_text(v) -> str:
    if v is None:
        return ""
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v).strip()


def _read_inventory_rows(raw: bytes, filename: str):
    """
    Reads the uploaded inventory (.xlsx or .csv) into a list of dicts keyed by
    internal field names. Title rows above the header (e.g. "Batch Manager -
    Group 1") and blank rows are ignored. Division and Deposit Date are taken
    from Description (text before the 1st "_" and between the 1st and 2nd "_",
    e.g. 1340_09-30-2026_334.99_...pdf); the Division / Deposit Date columns
    are only a fallback for older CSVs.
    """
    name = (filename or "").lower()
    if name.endswith(".xlsx") or name.endswith(".xlsm") or raw[:2] == b"PK":
        try:
            import openpyxl
        except ImportError:
            raise HTTPException(status_code=500, detail="Excel support (openpyxl) is not installed on the server")
        wb = openpyxl.load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
        matrix = [[_cell_text(c) for c in r] for r in wb.worksheets[0].iter_rows(values_only=True)]
    elif name.endswith(".xls"):
        raise HTTPException(status_code=400, detail="Please save the file as .xlsx or .csv and upload again")
    else:
        text = raw.decode("utf-8-sig", errors="replace")
        matrix = [[c.strip() for c in r] for r in csv.reader(io.StringIO(text))]

    header_idx, mapping = None, {}
    for i, r in enumerate(matrix[:30]):
        m = {j: _HEADER_ALIASES[_norm_text(c)] for j, c in enumerate(r) if _norm_text(c) in _HEADER_ALIASES}
        if "edm" in m.values() and len(m) >= 3:
            header_idx, mapping = i, m
            break
    if header_idx is None:
        raise HTTPException(status_code=400, detail="Could not find the header row (Batch, Status, Created, Images, Docs, Def Doc Type, ...)")

    rows = []
    for r in matrix[header_idx + 1:]:
        if not any(c for c in r):
            continue
        d = {f: (r[j] if j < len(r) else "") for j, f in mapping.items()}
        desc = d.get("description") or ""
        if "_" in desc:
            parts = desc.split("_")
            d["division"] = parts[0].strip()
            d["deposit_date"] = parts[1].strip() if len(parts) > 2 else (d.get("deposit_date") or "")
        rows.append(d)
    return rows
DUPLICATE_LOOKBACK_MONTHS = 1      # an EDM already imported within this window is not imported again
EXCEPTION_DEDUPE_DAYS = 30         # the same EDM isn't logged twice as an exception within this many days


def _norm_text(v) -> str:
    return " ".join((v or "").split()).lower()


def _norm_edm(v) -> str:
    return (v or "").strip().upper()


def _norm_facility(v) -> str:
    t = (v or "").strip()
    return str(int(t)) if t.isdigit() else t


def _client_for_row(division, by_no: dict, by_name: dict):
    """
    Which client an inventory row belongs to, from its Division column:
      1. the value is a Facility No ("120", "0120")
      2. a Facility No appears as a number inside the text ("Facility 120 - ...")
      3. the value is exactly a client name (only if that name is unique)
    Returns the Client or None. This is the one place that decides it.
    """
    text = (division or "").strip()
    if not text:
        return None
    if _norm_facility(text) in by_no:
        return by_no[_norm_facility(text)]
    for tok in re.findall(r"\d+", text):
        if _norm_facility(tok) in by_no:
            return by_no[_norm_facility(tok)]
    return by_name.get(_norm_text(text))


@app.post("/orders/import")
def import_inventory(
    process_id: int,
    file: UploadFile = File(...),
    on_behalf_of_team_lead_id: Optional[int] = Form(None),
    current_user: models.User = Depends(auth.require_role("team_lead", "super_admin", "admin")),
    db: Session = Depends(get_db),
):
    """
    Accepts the original inventory file (.xlsx or .csv): Batch, Status, Created, Images, Docs,
    Def Doc Type, Amount, Description, Bar Grp... Division / Deposit Date come from Description.
    Rows with Bar Grp "Grp - 4 Gottlieb-Sound Phys" are skipped.
    Creates one unassigned WorkOrder row per accepted line, tagged to this process.

    Which rows are accepted:
      * Client: a row is imported only if its client (matched from the
        Division column) is assigned to a Team Lead for this process.
        - A Team Lead (or an Admin importing on a Team Lead's behalf) gets
          only that Team Lead's clients; rows for another Team Lead's
          clients are skipped quietly (that Team Lead imports them).
        - A Super Admin, or an Admin with no Team Lead chosen, imports
          everything that is assigned, each row tagged to ITS client's Team Lead.
        - A client that isn't in the client list, or has no Team Lead for
          this process, is NOT imported and is logged under Import
          Exceptions for Admin / Super Admin.
      * EDM process only:
        - Def Doc Type must be Manual Eft Eob, Lockbox Payment or
          Insurance Credit Card Pmt Only; anything else is skipped.
        - The EDM must not already exist in this process from the last
          month. If it exists and is In-Process / Clarification / blank it
          is skipped quietly; if it is Completed it is skipped AND logged
          under Import Exceptions for the Team Lead, Admin and Super Admin.
          A repeat of an EDM inside the same file is skipped too.
    Orders imported for a Team Lead are tagged to them, so auto-assignment
    only offers them to colleagues reporting to that Team Lead.
    """
    _require_process_access(current_user, process_id)

    fixed_team_lead_id = None       # set when the whole file is imported for one Team Lead
    if current_user.role == "team_lead":
        fixed_team_lead_id = current_user.id
    elif current_user.role == "admin" and on_behalf_of_team_lead_id is not None:
        delegate = (
            db.query(models.User)
            .filter(
                models.User.id == on_behalf_of_team_lead_id,
                models.User.role == "team_lead",
                models.User.processes.any(models.Process.id == process_id),
            )
            .first()
        )
        if not delegate:
            raise HTTPException(status_code=400, detail="Not a valid Team Lead for this process")
        fixed_team_lead_id = delegate.id

    process = db.query(models.Process).filter(models.Process.id == process_id).first()
    is_edm = bool(process and process.name.strip().upper() == "EDM")

    # Client -> Team Lead for this process
    clients = db.query(models.Client).all()
    by_no = {_norm_facility(c.facility_no): c for c in clients}
    name_counts = {}
    for c in clients:
        name_counts[_norm_text(c.client_name)] = name_counts.get(_norm_text(c.client_name), 0) + 1
    by_name = {_norm_text(c.client_name): c for c in clients if name_counts[_norm_text(c.client_name)] == 1}
    client_tl = {
        a.client_id: a.team_lead_id
        for a in db.query(models.ClientProcessTeamLead).filter(models.ClientProcessTeamLead.process_id == process_id).all()
    }

    # EDMs already imported in the lookback window (EDM process)
    # IST, not the server's own (UTC) date — see todays_celebrations for
    # why: near IST midnight the two disagree on which calendar day it is.
    today = datetime.now(IST).date()
    existing = {}
    if is_edm:
        cutoff = today - relativedelta(months=DUPLICATE_LOOKBACK_MONTHS)
        for o in db.query(
            models.WorkOrder.id, models.WorkOrder.edm, models.WorkOrder.posting_status,
            models.WorkOrder.posted_date, models.WorkOrder.employee_name,
        ).filter(
            models.WorkOrder.process_id == process_id,
            models.WorkOrder.received_date >= cutoff,
            models.WorkOrder.edm.isnot(None),
        ).all():
            existing.setdefault(_norm_edm(o.edm), []).append(o)

    reader = _read_inventory_rows(file.file.read(), file.filename)
    counts = {
        "total_rows": 0, "imported": 0, "skipped_bar_grp": 0,
        "skipped_doc_type": 0, "skipped_other_team_lead": 0, "skipped_unassigned_client": 0,
        "skipped_duplicate_open": 0, "skipped_duplicate_in_file": 0, "skipped_duplicate_completed": 0,
    }
    exceptions = []
    seen_in_file = set()
    now_ist = datetime.now(IST).replace(tzinfo=None)

    def log_exception(kind, row, client, reason, team_lead_id, existing_order=None):
        exceptions.append(models.ImportException(
            created_at=now_ist, process_id=process_id, kind=kind,
            edm=(row.get("edm") or "").strip() or None,
            division_raw=(row.get("division") or "").strip() or None,
            facility_no=client.facility_no if client else None,
            client_name=client.client_name if client else None,
            def_doc_type=(row.get("def_doc_type") or "").strip() or None,
            amount=_parse_float(row.get("amount")),
            reason=reason,
            imported_by_id=current_user.id, imported_by_name=current_user.full_name, imported_by_role=current_user.role,
            team_lead_id=team_lead_id,
            existing_order_id=existing_order.id if existing_order else None,
            existing_posted_date=existing_order.posted_date if existing_order else None,
            existing_employee_name=existing_order.employee_name if existing_order else None,
        ))

    for row in reader:
        counts["total_rows"] += 1

        # 0) Bar Grp rows that are never imported
        if _norm_text(row.get("bar_grp")) in SKIP_BAR_GRPS:
            counts["skipped_bar_grp"] += 1
            continue

        # 1) EDM process: only the allowed Def Doc Types
        if is_edm and _norm_text(row.get("def_doc_type")) not in EDM_ALLOWED_DOC_TYPES:
            counts["skipped_doc_type"] += 1
            continue

        # 2) the client must be assigned to a Team Lead for this process
        client = _client_for_row(row.get("division"), by_no, by_name)
        row_tl = client_tl.get(client.id) if client else None
        if row_tl is None:
            counts["skipped_unassigned_client"] += 1
            log_exception(
                "unassigned_client", row, client,
                "No Team Lead assigned for this process" if client else "Client not found in the client list",
                fixed_team_lead_id,
            )
            continue
        if fixed_team_lead_id is not None and row_tl != fixed_team_lead_id:
            counts["skipped_other_team_lead"] += 1      # someone else's client; they import it
            continue

        # 3) EDM process: not already imported in the last month, nor repeated in this file
        edm_key = _norm_edm(row.get("edm"))
        if is_edm and edm_key:
            if edm_key in seen_in_file:
                counts["skipped_duplicate_in_file"] += 1
                continue
            seen_in_file.add(edm_key)
            prior = existing.get(edm_key)
            if prior:
                done = [o for o in prior if o.posting_status == "Completed"]
                if done:
                    counts["skipped_duplicate_completed"] += 1
                    latest = max(done, key=lambda o: (o.posted_date or date.min, o.id))
                    log_exception("duplicate_completed", row, client, "EDM already Completed", row_tl, existing_order=latest)
                else:
                    counts["skipped_duplicate_open"] += 1     # In-Process / Clarification / blank: already in the system
                continue

        db.add(models.WorkOrder(
            process_id=process_id,
            team_lead_id=row_tl,
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
            deposit_date=_safe_date(row.get("deposit_date")),
            last_edited_by=current_user.username,
        ))
        counts["imported"] += 1

    # One exception per document: skip any EDM already logged for this process recently.
    if exceptions:
        recent = {
            (e.kind, _norm_edm(e.edm))
            for e in db.query(models.ImportException.kind, models.ImportException.edm).filter(
                models.ImportException.process_id == process_id,
                models.ImportException.created_at >= now_ist - timedelta(days=EXCEPTION_DEDUPE_DAYS),
                models.ImportException.edm.isnot(None),
            ).all()
        }
        logged = set()
        for e in exceptions:
            key = (e.kind, _norm_edm(e.edm))
            if e.edm and (key in recent or key in logged):
                continue
            logged.add(key)
            db.add(e)
    db.commit()
    _auto_assign_open_slots(db, process_id)
    return counts


# Import Exceptions list: Team Leads see their own "already completed" notices;
# Admin and Super Admin see everything.
@app.get("/import-exceptions", response_model=List[schemas.ImportExceptionOut])
def list_import_exceptions(
    kind: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    process_id: Optional[int] = None,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    Rows the inventory import skipped that someone should know about,
    newest first (at most 5,000). kind = unassigned_client | duplicate_completed.
    Team Lead: only "duplicate_completed" for rows meant for them.
    Admin / Super Admin: both kinds, all Team Leads and processes.
    """
    if kind is not None and kind not in ("unassigned_client", "duplicate_completed"):
        raise HTTPException(status_code=400, detail="Unknown kind")
    q = db.query(models.ImportException)
    if current_user.role == "team_lead":
        q = q.filter(models.ImportException.kind == "duplicate_completed", models.ImportException.team_lead_id == current_user.id)
    if kind:
        q = q.filter(models.ImportException.kind == kind)
    if process_id is not None:
        q = q.filter(models.ImportException.process_id == process_id)
    if start_date:
        q = q.filter(models.ImportException.created_at >= datetime.combine(start_date, datetime.min.time()))
    if end_date:
        q = q.filter(models.ImportException.created_at < datetime.combine(end_date + timedelta(days=1), datetime.min.time()))
    rows = q.order_by(models.ImportException.created_at.desc(), models.ImportException.id.desc()).limit(5000).all()
    pnames = {p.id: p.name for p in db.query(models.Process).all()}
    tlnames = {u.id: u.full_name for u in db.query(models.User.id, models.User.full_name).filter(models.User.role == "team_lead").all()}
    out = []
    for r in rows:
        item = schemas.ImportExceptionOut.model_validate(r)
        item.process_name = pnames.get(r.process_id)
        item.team_lead_name = tlnames.get(r.team_lead_id)
        out.append(item)
    return out


def _parse_date(v: Optional[str]) -> Optional[date]:
    if not v:
        return None
    try:
        return date_parser.parse(v.strip()).date()
    except (ValueError, OverflowError) as e:
        raise ValueError(f"Unrecognized date format: {v!r}") from e


def _safe_date(v):
    try:
        return _parse_date(v)
    except ValueError:
        return None


def _parse_dt(v: Optional[str]) -> Optional[datetime]:
    if not v:
        return None
    try:
        return date_parser.parse(v.strip())
    except (ValueError, OverflowError) as e:
        raise ValueError(f"Unrecognized datetime format: {v!r}") from e

def _parse_int(v: Optional[str]) -> Optional[int]:
    return int(v) if v not in (None, "") else None


def _parse_float(v: Optional[str]) -> Optional[float]:
    return float(v) if v not in (None, "") else None


# ---------------------------------------------------------------------------
# Assignment logic — one open file per colleague at a time (round-robin)
# ---------------------------------------------------------------------------

def _auto_assign_open_slots(db: Session, process_id: int):
    """
    For every colleague who has access to this process, has logged in today (IST), and currently has no
    open (non-completed, non-escalated) order *in this process*, hand them
    the oldest unassigned order they're actually eligible for. An order
    imported by a Team Lead only goes to colleagues whose Reporting Manager
    matches that same Team Lead's name; an order imported by a Super Admin
    (team_lead_id is None) is open to any colleague in the process.

    Written to run in a small, fixed number of queries regardless of how
    many colleagues or candidate orders there are — colleagues needing an
    order, unassigned candidates, and team lead names are each fetched
    once, then matched in memory. The previous version re-queried the
    entire unassigned pool and did a fresh per-candidate team-lead lookup
    for every single colleague, which made a busy queue's Save noticeably
    slow.
    """
    colleagues = (
        db.query(models.User)
        .filter(
            models.User.role == "colleague",
            models.User.processes.any(models.Process.id == process_id),
            # Orders are only handed out to colleagues who have logged in today (IST).
            models.User.last_login_date == datetime.now(IST).date(),
        )
        .all()
    )
    if not colleagues:
        return

    open_colleague_ids = {
        row[0] for row in db.query(models.WorkOrder.assigned_to_id).filter(
            models.WorkOrder.process_id == process_id,
            models.WorkOrder.assigned_to_id.isnot(None),
            models.WorkOrder.posting_status != "Completed",
            models.WorkOrder.escalated == False,  # noqa: E712
        ).all()
    }
    needy = [c for c in colleagues if c.id not in open_colleague_ids]
    if not needy:
        return

    candidates = (
        db.query(models.WorkOrder)
        .filter(models.WorkOrder.process_id == process_id, models.WorkOrder.assigned_to_id.is_(None))
        .order_by(models.WorkOrder.id.asc())
        .all()
    )
    if not candidates:
        return

    team_lead_ids = {c.team_lead_id for c in candidates if c.team_lead_id is not None}
    team_lead_names = {}
    if team_lead_ids:
        team_lead_names = {
            u.id: u.full_name
            for u in db.query(models.User).filter(models.User.id.in_(team_lead_ids)).all()
        }

    remaining = list(candidates)
    for colleague in needy:
        match_index = None
        for i, cand in enumerate(remaining):
            if cand.team_lead_id is None:
                match_index = i
                break
            owner_name = team_lead_names.get(cand.team_lead_id)
            if owner_name and colleague.reporting_manager == owner_name:
                match_index = i
                break
        if match_index is None:
            continue
        next_order = remaining.pop(match_index)
        next_order.assigned_to_id = colleague.id
        next_order.assigned_date = datetime.now(IST).date()
        next_order.employee_id = colleague.employee_id or colleague.username
        next_order.employee_name = colleague.full_name
        next_order.posting_status = "In-Process"
        _start_timer(next_order)
        db.add(next_order)

    db.commit()


# ---------------------------------------------------------------------------
# Read endpoints
# ---------------------------------------------------------------------------

@app.get("/orders", response_model=List[schemas.WorkOrderOut])
def list_orders(
    process_id: int,
    current_user: models.User = Depends(auth.get_current_user),
    db: Session = Depends(get_db),
):
    """Active queue for one process — orders already submitted to Production are hidden here for everyone."""
    _deny_quality(current_user)
    _require_view_access(current_user, process_id)
    query = db.query(models.WorkOrder).options(
        selectinload(models.WorkOrder.clarification_detail), selectinload(models.WorkOrder.escalation_detail)).filter(
        models.WorkOrder.submitted == False,  # noqa: E712
        models.WorkOrder.process_id == process_id,
    )
    if current_user.role == "colleague":
        query = query.filter(models.WorkOrder.assigned_to_id == current_user.id)
    elif current_user.role == "team_lead":
        # A Team Lead's queue is their own orders plus anything not yet
        # tagged to any Team Lead (e.g. a Super Admin import). Orders
        # transferred to another Team Lead (transfer_orders sets
        # team_lead_id to the destination) drop out of view here.
        query = query.filter(
            or_(
                models.WorkOrder.team_lead_id == current_user.id,
                models.WorkOrder.team_lead_id.is_(None),
            )
        )
    orders = query.order_by(models.WorkOrder.id.asc()).all()
    if current_user.role in ("admin", "super_admin"):
        _attach_team_leads(db, orders, prefer="owner")     # so the screen can group by Team Lead
    return orders


@app.get("/orders/production", response_model=List[schemas.WorkOrderOut])
def list_production_orders(
    process_id: int,
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
    current_user: models.User = Depends(auth.get_current_user),
    db: Session = Depends(get_db),
):
    """
    Everything already submitted to Production, within one process. A Team
    Lead sees their own colleagues' (never another Team Lead's); a Super
    Admin sees everyone's; a colleague only ever sees their own — enforced
    server-side regardless of what filters are passed, not just hidden in
    the UI. All filters are optional and combine with AND. start_date/end_date
    filter by Posted Date (inclusive) — the day the work was actually completed.
    Admin (read-only), Quality (read-only) and Super Admin get every row
    tagged with its Team Lead.
    """
    _require_view_access(current_user, process_id)
    query = db.query(models.WorkOrder).options(
        selectinload(models.WorkOrder.clarification_detail), selectinload(models.WorkOrder.escalation_detail)).filter(
        models.WorkOrder.submitted == True,  # noqa: E712
        models.WorkOrder.process_id == process_id,
    )
    if current_user.role == "colleague":
        query = query.filter(models.WorkOrder.assigned_to_id == current_user.id)
    elif current_user.role == "team_lead":
        # A Team Lead only sees production data for THEIR OWN colleagues.
        query = query.filter(_team_orders_condition(db, current_user))

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

    orders = query.order_by(models.WorkOrder.posted_date.asc(), models.WorkOrder.id.asc()).all()
    if current_user.role in ("admin", "quality", "super_admin"):
        _attach_team_leads(db, orders, prefer="colleague")
    return orders


@app.get("/orders/escalations", response_model=List[schemas.WorkOrderOut])
def list_escalations(
    process_id: int,
    resolved: bool = False,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    resolved=False (default): rows currently locked awaiting a Team Lead's
    resolution. resolved=True: the report of past escalations this Team
    Lead has already resolved (any escalation category with a detail form,
    no longer locked, Issue Closed Date stamped) — a history view, so it
    isn't filtered by submitted like the open queue is.
    A Team Lead sees only their own team's rows either way; Admin (read-only)
    and Super Admin see every row in the process, each tagged with its Team
    Lead. Registered before /orders/{order_id}
    on purpose: FastAPI matches routes in registration order, so a
    literal path like this one has to come before a dynamic
    {order_id}: int path or "escalations" gets swallowed as an attempted
    (and invalid) order_id.
    """
    _require_view_access(current_user, process_id)
    query = db.query(models.WorkOrder).filter(models.WorkOrder.process_id == process_id)
    if resolved:
        query = query.filter(
            models.WorkOrder.escalation_category.in_(("Clarification", *ESCALATION_CATEGORY_FIELDS.keys())),
            models.WorkOrder.escalated == False,  # noqa: E712
            models.WorkOrder.issue_closed_date.isnot(None),
        )
    else:
        query = query.filter(
            models.WorkOrder.escalated == True,  # noqa: E712
            models.WorkOrder.submitted == False,  # noqa: E712
        )
    if current_user.role == "team_lead":
        query = query.filter(models.WorkOrder.team_lead_id == current_user.id)
    order_col = models.WorkOrder.issue_raised_date
    orders = query.order_by(order_col.desc() if resolved else order_col.asc()).all()
    if current_user.role in ("admin", "super_admin"):
        _attach_team_leads(db, orders, prefer="owner")
    return orders


@app.get("/orders/{order_id}", response_model=schemas.WorkOrderOut)
def get_order(order_id: int, current_user: models.User = Depends(auth.get_current_user), db: Session = Depends(get_db)):
    order = db.query(models.WorkOrder).filter(models.WorkOrder.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    _deny_quality(current_user)
    _require_process_access(current_user, order.process_id)
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
    current_user: models.User = Depends(auth.require_role("team_lead", "super_admin")),
    db: Session = Depends(get_db),
):
    order = db.query(models.WorkOrder).filter(models.WorkOrder.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    _require_process_access(current_user, order.process_id)
    reasons = _order_lock_reasons(order) if _audit_applies(current_user) else []
    before = _order_snapshot(order) if reasons else None
    for field, value in payload.dict(exclude_unset=True).items():
        setattr(order, field, value)
    order.last_edited_by = current_user.username
    if reasons:
        _log_order_change(db, current_user, order, "edit", reasons, before)
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
    if order.escalated:
        raise HTTPException(
            status_code=403,
            detail="This order is escalated to your Team Lead and locked until they resolve it",
        )

    payload_data = payload.dict(exclude_unset=True)
    if "posting_status" in payload_data and payload_data["posting_status"] not in (
        "Completed", "In-Process", "Clarification"
    ):
        raise HTTPException(
            status_code=400,
            detail="posting_status must be 'Completed', 'In-Process', or 'Clarification'",
        )

    previous_status = order.posting_status

    # Pending $, Posted Date, VENTRA Comment, and Issue Closed Date are all
    # system-derived now — never accepted directly from the colleague.
    # VENTRA Comment is a Team-Lead-only note (set via team-lead-correction);
    # Issue Closed Date auto-stamps the moment a Clarification is resolved.
    payload_data.pop("pending_amount", None)
    payload_data.pop("posted_date", None)
    payload_data.pop("ventra_comment", None)
    payload_data.pop("issue_closed_date", None)

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

    # Posting rules (colleague save):
    #   * Posted $ <> 0  -> BAR Batch must be a 7-digit number
    #     Posted $ = 0   -> BAR Batch must be 0
    #   * BAR Batch > 0  -> Trans Count must be > 0
    #   * Pending $ <> 0 -> Poster Comment is required (checked when the row is
    #     being Completed or sent to Clarification, the only statuses where the
    #     comment can be entered at all)
    bar_text = str(final_bar_batch).strip()
    problems = []
    if abs(float(final_posted_amount)) < 0.005:
        if bar_text not in ("0", "00", "0000000"):
            problems.append("BAR Batch must be 0 when Posted $ is 0")
    else:
        if not (bar_text.isdigit() and len(bar_text) == 7 and int(bar_text) > 0):
            problems.append("BAR Batch must be a 7-digit number when Posted $ is not 0")
    bar_positive = bar_text.isdigit() and int(bar_text) > 0
    if bar_positive and not (final_trans_count and final_trans_count > 0):
        problems.append("Trans Count must be greater than 0 when BAR Batch is greater than 0")
    if order.amount is not None and final_status in ("Completed", "Clarification"):
        pending_now = float(order.amount) - float(final_posted_amount)
        final_comment = payload_data.get("poster_comment", order.poster_comment)
        if abs(pending_now) >= 0.005 and not (final_comment or "").strip():
            problems.append("Poster Comment is required when Pending $ is not 0")
    if problems:
        raise HTTPException(status_code=400, detail="Cannot save \u2014 " + "; ".join(problems))

    # Poster Comment, Escalation Category, and Issue Raised Date only make
    # sense while a Clarification is open — locked both during In-Process
    # and once Completed. (VENTRA Comment and Issue Closed Date are excluded
    # here entirely since they're never colleague-editable — see the pops
    # above.)
    CLARIFICATION_ONLY_FIELDS = [
        "poster_comment", "escalation_category", "issue_raised_date",
    ]
    # Poster Comment is the one exception: it stays editable on the save
    # that marks the row Completed (a closing note).
    if final_status != "Clarification":
        attempted = [
            f for f in CLARIFICATION_ONLY_FIELDS
            if not (f == "poster_comment" and final_status == "Completed")
            and f in payload_data and payload_data[f] not in (None, "")
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
        order.issue_raised_date = datetime.now(IST).date()

    # Auto-set Posted Date the moment a colleague marks Completed — no manual
    # entry needed, and it guarantees TAT can always be calculated below.
    if order.posting_status == "Completed" and not order.posted_date:
        order.posted_date = datetime.now(IST).date()

    # Completing an order freezes its "Time Taken" — stop the clock and
    # roll in whatever time was still running.
    if order.posting_status == "Completed" and order.timer_status == "running":
        _finalize_timer(order, "stopped")

    # Resolving a Clarification (moving to any other status) auto-stamps
    # Issue Closed Date, mirroring how Issue Raised Date auto-stamps on the
    # way in. Escalation Category and Issue Raised Date are deliberately
    # NOT cleared anymore — they're kept as history so the TAT pause-window
    # calculation at Completion stays accurate.
    if previous_status == "Clarification" and order.posting_status != "Clarification" and not order.issue_closed_date:
        order.issue_closed_date = datetime.now(IST).date()

    # Saving with Escalation Category = "Clarification" hands the row off
    # to the Team Lead: it locks for the colleague (still visible, read-
    # only) and, once committed, frees their one-open-order slot so they
    # get handed new work instead of sitting idle waiting on a resolution.
    # Requires that category's detail popup to have actually been
    # completed first — otherwise nothing to hand off. Applies to every
    # escalation category that has a defined detail form (Clarification,
    # plus EOB not found / Invoice Creation / Patient not found / Need to
    # Delete via ESCALATION_CATEGORY_FIELDS); a category with no form
    # (e.g. DUVA Verification) just saves normally, same as always.
    if order.posting_status == "Clarification" and order.escalation_category == "Clarification":
        detail = (
            db.query(models.ClarificationDetail)
            .filter(models.ClarificationDetail.order_id == order.id)
            .first()
        )
        if not detail or not detail.escalation_type:
            raise HTTPException(
                status_code=400,
                detail="Fill in the Clarification Details popup (📋 Details) — choose an Escalation Type — before saving",
            )
        # Re-derive the auto-filled fields from the order as it stands
        # RIGHT NOW, not as it stood whenever the popup was last saved —
        # the popup can be (and often is) saved before the row's own
        # BAR Batch/Posted $/Poster Comment are actually persisted, so
        # without this the Team Lead's Escalations queue would show
        # stale/blank values for those even though the colleague's own
        # popup showed them filled in.
        process = db.query(models.Process).filter(models.Process.id == order.process_id).first()
        detail.deposit_type = process.name if process else None
        detail.exchange = "-"
        detail.era_check = "-"
        detail.edm_batch_number = order.edm
        detail.bar_batch_number = order.bar_batch
        detail.batch_description = order.description
        detail.team = "CBE"
        detail.poster_login = current_user.full_name
        detail.amount_posted = str(order.posted_amount) if order.posted_amount is not None else None
        detail.clarification_details = order.poster_comment
        detail.updated_at = datetime.now(IST)
        order.escalated = True
        if order.timer_status == "running":
            _finalize_timer(order, "paused")
    elif order.posting_status == "Clarification" and order.escalation_category in ESCALATION_CATEGORY_FIELDS:
        category = order.escalation_category
        detail = (
            db.query(models.EscalationDetail)
            .filter(models.EscalationDetail.order_id == order.id)
            .first()
        )
        if not detail or detail.category != category:
            raise HTTPException(
                status_code=400,
                detail=f"Fill in the {category} details popup (📋 Details) before saving",
            )
        # Same re-derive-at-actual-save-time fix as Clarification above:
        # keep whatever was manually typed into the popup, but recompute
        # every auto/fixed/user field fresh from the order as it stands
        # right now.
        manual_keys = {k for k, _l, kind in ESCALATION_CATEGORY_FIELDS[category] if kind in ("manual", "manual_date", "manual_currency", "manual_select")}
        manual_values = {k: detail.data.get(k) for k in manual_keys}
        merged = _compute_escalation_auto_fields(category, order, current_user)
        merged.update(manual_values)
        detail.data = merged
        detail.posted_by_name = current_user.full_name
        detail.updated_at = datetime.now(IST)
        order.escalated = True
        if order.timer_status == "running":
            _finalize_timer(order, "paused")

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

    if order.posting_status == "Completed" or order.escalated:
        _auto_assign_open_slots(db, order.process_id)

    return order


# ---------------------------------------------------------------------------
# Team Lead Orders Dashboard (date-wise: received / pending / in-process /
# clarification / completed)
# ---------------------------------------------------------------------------

AUDIT_LIMIT = 5000


@app.get("/audit/order-changes", response_model=List[schemas.OrderChangeLogOut])
def list_order_changes(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    process_id: Optional[int] = None,
    current_user: models.User = Depends(auth.require_role("admin", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    What Team Leads changed on locked / completed orders (Admin and Super
    Admin only), newest first. start_date / end_date filter on the day the
    change was made (IST). At most 5,000 entries.
    """
    q = db.query(models.OrderChangeLog)
    if process_id is not None:
        q = q.filter(models.OrderChangeLog.process_id == process_id)
    if start_date:
        q = q.filter(models.OrderChangeLog.created_at >= datetime.combine(start_date, datetime.min.time()))
    if end_date:
        q = q.filter(models.OrderChangeLog.created_at < datetime.combine(end_date + timedelta(days=1), datetime.min.time()))
    rows = q.order_by(models.OrderChangeLog.created_at.desc(), models.OrderChangeLog.id.desc()).limit(AUDIT_LIMIT).all()
    names = {p.id: p.name for p in db.query(models.Process).all()}
    out = []
    for r in rows:
        item = schemas.OrderChangeLogOut.model_validate(r)
        item.process_name = names.get(r.process_id)
        out.append(item)
    return out


def _require_view_access(user: models.User, process_id: int):
    """Viewing a process's queues: Admin and Quality may view every process
    (read-only); everyone else needs the process assigned to them."""
    if user.role not in ("admin", "quality"):
        _require_process_access(user, process_id)


def _deny_quality(user: models.User):
    """Quality can only view / export Production — nothing else."""
    if user.role == "quality":
        raise HTTPException(status_code=403, detail="The Quality profile can only view Production data")


def _attach_team_leads(db: Session, orders: list, prefer: str = "owner") -> list:
    """
    Sets order.team_lead_name on each order (Admin / Super Admin lists).
      prefer="owner"     -> the Team Lead who owns the order (team_lead_id); if
                            unowned, its assigned colleague's Team Lead.
                            (How Active Queue / Escalations are scoped.)
      prefer="colleague" -> the assigned colleague's Team Lead; if unassigned,
                            the owner. (How Production is scoped.)
    Anything else is "No Team Lead".
    """
    tls, members, _orphans = _tl_membership(db)
    colleague_tl = {cid: tid for tid, ids in members.items() for cid in ids}
    for o in orders:
        owner = o.team_lead_id if o.team_lead_id in tls else None
        via_colleague = colleague_tl.get(o.assigned_to_id) if o.assigned_to_id is not None else None
        tid = (owner or via_colleague) if prefer == "owner" else (via_colleague or owner)
        o.team_lead_name = tls.get(tid) if tid else None
    return orders


def _tl_membership(db: Session):
    """
    Which Team Lead each colleague belongs to (Reporting Manager = Team Lead's
    full name). Returns (team_leads, members, orphans):
      team_leads  {team_lead_id: full_name}
      members     {team_lead_id: [colleague ids]}
      orphans     [colleague ids whose Reporting Manager matches no Team Lead]
    """
    tls = {u.id: u.full_name for u in db.query(models.User.id, models.User.full_name).filter(models.User.role == "team_lead").all()}
    name_to_id = {name: tid for tid, name in tls.items()}
    members = {tid: [] for tid in tls}
    orphans = []
    for cid, mgr in db.query(models.User.id, models.User.reporting_manager).filter(models.User.role == "colleague").all():
        tid = name_to_id.get(mgr)
        (members[tid] if tid else orphans).append(cid)
    return tls, members, orphans


def _order_team_lead_id(order_assigned_to_id, order_team_lead_id, colleague_tl: dict, tl_ids: set) -> int:
    """The single Team Lead an order counts under (0 = none): its assigned
    colleague's Team Lead, or for unassigned orders the Team Lead who owns it."""
    if order_assigned_to_id is not None:
        return colleague_tl.get(order_assigned_to_id, 0)
    return order_team_lead_id if order_team_lead_id in tl_ids else 0


def _tl_condition(db: Session, team_lead_id: int):
    """SQL condition: orders that count under one Team Lead (0 = no Team Lead)."""
    tls, members, orphans = _tl_membership(db)
    W = models.WorkOrder
    if team_lead_id == 0:
        return or_(
            W.assigned_to_id.in_(orphans) if orphans else sa_false(),
            and_(W.assigned_to_id.is_(None), or_(W.team_lead_id.is_(None), ~W.team_lead_id.in_(list(tls.keys())) if tls else sa_false())),
        )
    ids = members.get(team_lead_id, [])
    return or_(
        W.assigned_to_id.in_(ids) if ids else sa_false(),
        and_(W.assigned_to_id.is_(None), W.team_lead_id == team_lead_id),
    )


def _orders_dash_scope(db: Session, user: models.User, process_id: Optional[int], team_lead_id: Optional[int] = None):
    """
    Returns a function that narrows a WorkOrder query to what this user may
    see on the Orders Dashboard:
      * Team Lead  -> their own team only (colleagues' orders + their unassigned queue)
      * Admin / Super Admin -> every team and every process; optionally one
        Team Lead via team_lead_id
      * Team Lead -> only the processes they have access to
    """
    all_access = user.role in ("admin", "super_admin")
    if process_id is not None and not all_access:
        _require_process_access(user, process_id)
    cond = _team_orders_condition(db, user, include_unowned=True)   # None for admin / super_admin
    tl_cond = _tl_condition(db, team_lead_id) if (team_lead_id is not None and user.role in ("admin", "super_admin")) else None
    my_process_ids = [p.id for p in user.processes]

    def scoped(q):
        if process_id is not None:
            q = q.filter(models.WorkOrder.process_id == process_id)
        elif not all_access:
            q = q.filter(models.WorkOrder.process_id.in_(my_process_ids) if my_process_ids else sa_false())
        if cond is not None:
            q = q.filter(cond)
        if tl_cond is not None:
            q = q.filter(tl_cond)
        return q
    return scoped


_DASH_STATUS_KEY = {"Pending": "pending", "In-Process": "in_process", "Clarification": "clarification", "Completed": "completed"}


def _dash_status_expr():
    # No posting status yet (not assigned / started) is shown as "Pending".
    return func.coalesce(func.nullif(models.WorkOrder.posting_status, ""), "Pending")


@app.get("/dashboard/orders", response_model=List[schemas.OrdersDashboardRow])
def orders_dashboard(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    process_id: Optional[int] = None,
    team_lead_id: Optional[int] = None,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    One row per date. The first five numbers count the orders RECEIVED on
    that date (Received Date) by their CURRENT status:
        pending       = not yet assigned/started (no posting status)
        in_process    = In-Process
        clarification = Clarification
        completed     = Completed
    so received = pending + in_process + clarification + completed.
    completed_on_date counts orders marked Completed ON that date (Posted
    Date). Orders already submitted to Production are included. A Team Lead
    only counts their own team's orders; Admin and Super Admin count every
    team (team_lead_id narrows to one Team Lead).
    """
    scoped = _orders_dash_scope(db, current_user, process_id, team_lead_id)
    status_expr = _dash_status_expr()

    rec = scoped(db.query(
        models.WorkOrder.received_date, status_expr, func.count(models.WorkOrder.id)
    ).filter(models.WorkOrder.received_date.isnot(None)))
    if start_date:
        rec = rec.filter(models.WorkOrder.received_date >= start_date)
    if end_date:
        rec = rec.filter(models.WorkOrder.received_date <= end_date)
    rec = rec.group_by(models.WorkOrder.received_date, status_expr).all()

    done = scoped(db.query(
        models.WorkOrder.posted_date, func.count(models.WorkOrder.id)
    ).filter(models.WorkOrder.posting_status == "Completed", models.WorkOrder.posted_date.isnot(None)))
    if start_date:
        done = done.filter(models.WorkOrder.posted_date >= start_date)
    if end_date:
        done = done.filter(models.WorkOrder.posted_date <= end_date)
    done = done.group_by(models.WorkOrder.posted_date).all()

    days = {}
    def day(d):
        return days.setdefault(d, {
            "work_date": d, "received": 0, "pending": 0, "in_process": 0,
            "clarification": 0, "completed": 0, "completed_on_date": 0,
        })
    for d, st, n in rec:
        row = day(d)
        row["received"] += n
        # Any unexpected status text is shown as pending rather than lost.
        row[_DASH_STATUS_KEY.get(st, "pending")] += n
    for d, n in done:
        day(d)["completed_on_date"] += n

    return sorted(days.values(), key=lambda r: r["work_date"], reverse=True)


@app.get("/dashboard/orders/by-team-lead", response_model=List[schemas.OrdersDashboardTLRow])
def orders_dashboard_by_team_lead(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    process_id: Optional[int] = None,
    current_user: models.User = Depends(auth.require_role("admin", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    The same counts as /dashboard/orders but one row per Team Lead over the
    whole date range (Admin / Super Admin only). An order counts under its
    assigned colleague's Team Lead; an unassigned order under the Team Lead
    who owns it; anything else under "No Team Lead" (id 0). Every order
    lands in exactly one row, so the rows add up to the overall totals.
    """
    scoped = _orders_dash_scope(db, current_user, process_id)
    tls, members, orphans = _tl_membership(db)
    colleague_tl = {cid: tid for tid, ids in members.items() for cid in ids}
    tl_ids = set(tls.keys())
    W = models.WorkOrder
    status_expr = _dash_status_expr()

    rec = scoped(db.query(W.assigned_to_id, W.team_lead_id, status_expr, func.count(W.id)).filter(W.received_date.isnot(None)))
    if start_date:
        rec = rec.filter(W.received_date >= start_date)
    if end_date:
        rec = rec.filter(W.received_date <= end_date)
    rec = rec.group_by(W.assigned_to_id, W.team_lead_id, status_expr).all()

    done = scoped(db.query(W.assigned_to_id, W.team_lead_id, func.count(W.id)).filter(
        W.posting_status == "Completed", W.posted_date.isnot(None)))
    if start_date:
        done = done.filter(W.posted_date >= start_date)
    if end_date:
        done = done.filter(W.posted_date <= end_date)
    done = done.group_by(W.assigned_to_id, W.team_lead_id).all()

    rows = {}
    def row_for(tid):
        return rows.setdefault(tid, {
            "team_lead_id": tid,
            "team_lead_name": tls.get(tid, "No Team Lead (unassigned queue)"),
            "received": 0, "pending": 0, "in_process": 0, "clarification": 0, "completed": 0, "completed_on_date": 0,
        })
    for assigned, owner, st, n in rec:
        r = row_for(_order_team_lead_id(assigned, owner, colleague_tl, tl_ids))
        r["received"] += n
        r[_DASH_STATUS_KEY.get(st, "pending")] += n
    for assigned, owner, n in done:
        row_for(_order_team_lead_id(assigned, owner, colleague_tl, tl_ids))["completed_on_date"] += n

    return sorted(rows.values(), key=lambda r: (r["team_lead_id"] == 0, r["team_lead_name"].lower()))


DASH_METRICS = {"received", "pending", "in_process", "clarification", "completed", "completed_on_date"}
DASH_DETAIL_LIMIT = 5000


@app.get("/dashboard/orders/details", response_model=List[schemas.OrdersDashboardDetail])
def orders_dashboard_details(
    metric: str,
    on_date: Optional[date] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    process_id: Optional[int] = None,
    team_lead_id: Optional[int] = None,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    The raw orders behind a dashboard number — click a count, get exactly
    those orders. metric is one of received / pending / in_process /
    clarification / completed (orders RECEIVED in the date window, by status)
    or completed_on_date (orders marked Completed in the window, by Posted
    Date). on_date = a single day (a date row); otherwise start_date/end_date.
    Same visibility rules as the dashboard itself. At most 5,000 rows.
    """
    if metric not in DASH_METRICS:
        raise HTTPException(status_code=400, detail="Unknown metric")
    scoped = _orders_dash_scope(db, current_user, process_id, team_lead_id)
    W = models.WorkOrder
    q = scoped(db.query(W))

    if metric == "completed_on_date":
        date_col = W.posted_date
        q = q.filter(W.posting_status == "Completed", W.posted_date.isnot(None))
    else:
        date_col = W.received_date
        q = q.filter(W.received_date.isnot(None))
        if metric == "pending":
            # No posting status yet — plus any unexpected status text, which
            # the dashboard counts as pending too (see _DASH_STATUS_KEY use).
            q = q.filter(or_(
                W.posting_status.is_(None), W.posting_status == "",
                ~W.posting_status.in_(["In-Process", "Clarification", "Completed"]),
            ))
        elif metric == "in_process":
            q = q.filter(W.posting_status == "In-Process")
        elif metric == "clarification":
            q = q.filter(W.posting_status == "Clarification")
        elif metric == "completed":
            q = q.filter(W.posting_status == "Completed")

    if on_date:
        q = q.filter(date_col == on_date)
    else:
        if start_date:
            q = q.filter(date_col >= start_date)
        if end_date:
            q = q.filter(date_col <= end_date)

    orders = q.order_by(date_col.desc(), W.id.asc()).limit(DASH_DETAIL_LIMIT).all()

    tls, members, orphans = _tl_membership(db)
    colleague_tl = {cid: tid for tid, ids in members.items() for cid in ids}
    tl_ids = set(tls.keys())
    out = []
    for o in orders:
        item = schemas.OrdersDashboardDetail.model_validate(o)
        tid = _order_team_lead_id(o.assigned_to_id, o.team_lead_id, colleague_tl, tl_ids)
        item.team_lead_name = tls.get(tid, "No Team Lead")
        out.append(item)
    return out


# ---------------------------------------------------------------------------
# Batch Count Dashboard (date-wise, per colleague / process)
# ---------------------------------------------------------------------------

# Production % compares transactions against the process's DAILY target
# scaled to the hours actually worked: expected = daily_target * hours / this.
STANDARD_SHIFT_HOURS = 8.0
HOURS_APPROVAL_THRESHOLD = 8.0     # a daily total above this needs Team Lead approval


# ---------------------------------------------------------------------------
# Clients (Super Admin -> Admin -> Clients): client list, Active / Inactive
# status with the date it went inactive, and the Team Lead per process
# ---------------------------------------------------------------------------

def _client_payload(db: Session, clients: list) -> list:
    ids = [c.id for c in clients]
    assigned = {}
    if ids:
        for a in db.query(models.ClientProcessTeamLead).filter(models.ClientProcessTeamLead.client_id.in_(ids)).all():
            assigned.setdefault(a.client_id, []).append({"process_id": a.process_id, "team_lead_id": a.team_lead_id})
    return [
        {
            "id": c.id, "facility_no": c.facility_no, "client_name": c.client_name,
            "status": c.status, "inactive_date": c.inactive_date,
            "assignments": sorted(assigned.get(c.id, []), key=lambda x: x["process_id"]),
        }
        for c in clients
    ]


def _facility_sort_key(c: models.Client):
    return (0, int(c.facility_no), c.facility_no) if c.facility_no.isdigit() else (1, 0, c.facility_no)


def _clean_client_fields(db: Session, payload: schemas.ClientSave, client_id: Optional[int]) -> dict:
    facility_no = (payload.facility_no or "").strip()
    name = " ".join((payload.client_name or "").split())
    if not facility_no.isdigit():
        raise HTTPException(status_code=400, detail="Facility No must be a number")
    if not name:
        raise HTTPException(status_code=400, detail="Client Name is required")
    if payload.status not in ("Active", "Inactive"):
        raise HTTPException(status_code=400, detail="Status must be 'Active' or 'Inactive'")
    dup = db.query(models.Client).filter(models.Client.facility_no == facility_no)
    if client_id is not None:
        dup = dup.filter(models.Client.id != client_id)
    if dup.first():
        raise HTTPException(status_code=400, detail=f"Facility No {facility_no} already exists")
    inactive_date = None
    if payload.status == "Inactive":
        inactive_date = payload.inactive_date or datetime.now(IST).date()
    return {"facility_no": facility_no, "client_name": name, "status": payload.status, "inactive_date": inactive_date}


def _eligible_team_lead(db: Session, team_lead_id: int) -> models.User:
    tl = db.query(models.User).filter(models.User.id == team_lead_id, models.User.role == "team_lead").first()
    if not tl:
        raise HTTPException(status_code=400, detail="Not a valid Team Lead")
    if tl.employment_status == "Inactive":
        raise HTTPException(status_code=400, detail=f"{tl.full_name} is inactive")
    return tl


@app.get("/clients", response_model=schemas.ClientListResponse)
def list_clients(
    current_user: models.User = Depends(auth.require_role("colleague", "team_lead", "admin", "super_admin")),
    db: Session = Depends(get_db),
):
    """Everything the Clients screens need in one call: clients (with their
    Team Lead per process), the Team Leads, and the processes. READ-ONLY for
    colleagues, Team Leads and Admin (the "Clients" view); only the Super
    Admin can change anything (every write endpoint below is Super Admin
    only). The Quality profile has no access."""
    clients = sorted(db.query(models.Client).all(), key=_facility_sort_key)
    team_leads = [
        {"id": u.id, "full_name": u.full_name, "active": u.employment_status != "Inactive", "process_ids": sorted(p.id for p in u.processes)}
        for u in db.query(models.User).filter(models.User.role == "team_lead").order_by(models.User.full_name.asc()).all()
    ]
    processes = [{"id": p.id, "name": p.name} for p in db.query(models.Process).order_by(models.Process.id.asc()).all()]
    return {"clients": _client_payload(db, clients), "team_leads": team_leads, "processes": processes}


@app.post("/clients", response_model=schemas.ClientOut)
def create_client(
    payload: schemas.ClientSave,
    current_user: models.User = Depends(auth.require_role("super_admin")),
    db: Session = Depends(get_db),
):
    client = models.Client(**_clean_client_fields(db, payload, None))
    db.add(client)
    db.commit()
    db.refresh(client)
    return _client_payload(db, [client])[0]


# Registered before the {client_id} routes (literal path first).
@app.post("/clients/bulk-assign", response_model=schemas.ClientBulkResult)
def bulk_assign_clients(
    payload: schemas.ClientBulkAssign,
    current_user: models.User = Depends(auth.require_role("super_admin")),
    db: Session = Depends(get_db),
):
    """
    Set (or, with team_lead_id null, clear) the Team Lead for many clients at
    once, in one process or in all of them. A Team Lead is only assigned to
    processes they actually work on; the others are reported back as skipped.
    """
    if not payload.client_ids:
        raise HTTPException(status_code=400, detail="Select at least one client")
    clients = db.query(models.Client).filter(models.Client.id.in_(payload.client_ids)).all()
    if len(clients) != len(set(payload.client_ids)):
        raise HTTPException(status_code=404, detail="Some clients were not found")
    all_processes = db.query(models.Process).order_by(models.Process.id.asc()).all()
    wanted = set(payload.process_ids or [p.id for p in all_processes])
    targets = [p for p in all_processes if p.id in wanted]
    if not targets or len(targets) != len(wanted):
        raise HTTPException(status_code=400, detail="Unknown process")

    skipped = []
    if payload.team_lead_id is not None:
        tl = _eligible_team_lead(db, payload.team_lead_id)
        tl_process_ids = {p.id for p in tl.processes}
        skipped = [p.name for p in targets if p.id not in tl_process_ids]
        targets = [p for p in targets if p.id in tl_process_ids]
        if not targets:
            raise HTTPException(status_code=400, detail=f"{tl.full_name} doesn't work on the selected process(es)")

    target_ids = [p.id for p in targets]
    existing = {
        (a.client_id, a.process_id): a
        for a in db.query(models.ClientProcessTeamLead).filter(
            models.ClientProcessTeamLead.client_id.in_([c.id for c in clients]),
            models.ClientProcessTeamLead.process_id.in_(target_ids),
        ).all()
    }
    updated = 0
    for c in clients:
        for pid in target_ids:
            row = existing.get((c.id, pid))
            if payload.team_lead_id is None:
                if row:
                    db.delete(row)
                    updated += 1
            else:
                if row:
                    row.team_lead_id = payload.team_lead_id
                else:
                    db.add(models.ClientProcessTeamLead(client_id=c.id, process_id=pid, team_lead_id=payload.team_lead_id))
                updated += 1
    db.commit()
    return {"updated": updated, "skipped_processes": skipped}


@app.patch("/clients/{client_id}", response_model=schemas.ClientOut)
def update_client(
    client_id: int,
    payload: schemas.ClientSave,
    current_user: models.User = Depends(auth.require_role("super_admin")),
    db: Session = Depends(get_db),
):
    """Edit Facility No / name / status. Going Inactive stamps the Inactive Date
    (the one given, else today IST); going Active clears it."""
    client = db.query(models.Client).filter(models.Client.id == client_id).first()
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    for k, v in _clean_client_fields(db, payload, client_id).items():
        setattr(client, k, v)
    db.commit()
    db.refresh(client)
    return _client_payload(db, [client])[0]


@app.put("/clients/{client_id}/assignments", response_model=schemas.ClientOut)
def save_client_assignments(
    client_id: int,
    payload: schemas.ClientAssignmentsSave,
    current_user: models.User = Depends(auth.require_role("super_admin")),
    db: Session = Depends(get_db),
):
    """Set the Team Lead for this client in the processes sent (null = unassign)."""
    client = db.query(models.Client).filter(models.Client.id == client_id).first()
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    known = {p.id for p in db.query(models.Process).all()}
    for pid, tl_id in payload.assignments.items():
        if pid not in known:
            raise HTTPException(status_code=400, detail="Unknown process")
        row = db.query(models.ClientProcessTeamLead).filter_by(client_id=client_id, process_id=pid).first()
        if tl_id is None:
            if row:
                db.delete(row)
            continue
        tl = _eligible_team_lead(db, tl_id)
        if not any(p.id == pid for p in tl.processes):
            raise HTTPException(status_code=400, detail=f"{tl.full_name} doesn't work on that process")
        if row:
            row.team_lead_id = tl_id
        else:
            db.add(models.ClientProcessTeamLead(client_id=client_id, process_id=pid, team_lead_id=tl_id))
    db.commit()
    return _client_payload(db, [client])[0]


def _batch_visible_colleagues(db: Session, current_user: models.User):
    """Colleagues whose dashboard rows this user may see."""
    q = db.query(models.User).filter(models.User.role == "colleague")
    if current_user.role == "colleague":
        return q.filter(models.User.id == current_user.id).all()
    if current_user.role == "team_lead":
        return q.filter(models.User.reporting_manager == current_user.full_name).all()
    return q.all()  # admin / super_admin: every team


def _batch_aggregate(group: list) -> dict:
    """
    Combines several per-process rows (one colleague's day, or a whole Team
    Lead's range) into one set of figures — used by the "Overall" row and the
    Team Lead-wise view so both follow exactly the same rules:
      * Hours / Accounts Audited / Errors: simple totals of what was entered
      * Production %: transactions of the rows that can be measured (daily
        target AND hours entered) vs their combined expected output
      * Quality %: only rows that have both Accounts Audited and Errors
    """
    hrs = [r["hours_worked"] for r in group if r["hours_worked"] is not None]
    aud = [r["accounts_audited"] for r in group if r["accounts_audited"] is not None]
    err = [r["errors"] for r in group if r["errors"] is not None]
    measured = [r for r in group if r["_expected"]]
    prod = None
    if measured:
        prod = round(sum(r["total_trans_count"] for r in measured) / sum(r["_expected"] for r in measured) * 100, 2)
    complete = [r for r in group if r["accounts_audited"] and r["errors"] is not None]
    qual = None
    if complete:
        a_sum = sum(r["accounts_audited"] for r in complete)
        e_sum = sum(r["errors"] for r in complete)
        qual = round((a_sum - e_sum) / a_sum * 100, 2)
    return {
        "batches_worked": sum(r["batches_worked"] for r in group),
        "total_trans_count": sum(r["total_trans_count"] for r in group),
        "hours_worked": round(sum(hrs), 2) if hrs else None,
        "production_pct": prod,
        "accounts_audited": sum(aud) if aud else None,
        "errors": sum(err) if err else None,
        "quality_pct": qual,
    }


def _batch_base_rows(db: Session, current_user: models.User, start_date, end_date, process_id, user_id) -> list:
    """One row per colleague / process / date with completed work (before any Overall rows)."""
    if process_id is not None and current_user.role != "admin":
        _require_process_access(current_user, process_id)   # Admin / Super Admin see every process

    colleagues = {u.id: u for u in _batch_visible_colleagues(db, current_user)}
    if user_id is not None:
        colleagues = {k: v for k, v in colleagues.items() if k == user_id}
    if not colleagues:
        return []

    query = db.query(models.WorkOrder).filter(
        models.WorkOrder.posting_status == "Completed",
        models.WorkOrder.posted_date.isnot(None),
        models.WorkOrder.assigned_to_id.in_(list(colleagues.keys())),
    )
    if process_id is not None:
        query = query.filter(models.WorkOrder.process_id == process_id)
    if start_date:
        query = query.filter(models.WorkOrder.posted_date >= start_date)
    if end_date:
        query = query.filter(models.WorkOrder.posted_date <= end_date)

    groups = {}
    for o in query.all():
        if o.process_id is None:
            continue
        key = (o.assigned_to_id, o.process_id, o.posted_date)
        g = groups.setdefault(key, {"batches": 0, "trans": 0})
        g["batches"] += 1
        g["trans"] += o.trans_count or 0
    if not groups:
        return []

    processes = {p.id: p for p in db.query(models.Process).all()}
    stats = {
        (st.user_id, st.process_id, st.work_date): st
        for st in db.query(models.DailyBatchStat).filter(
            models.DailyBatchStat.user_id.in_(list(colleagues.keys()))
        ).all()
    }
    tls, members, _orphans = _tl_membership(db)
    colleague_tl = {cid: tid for tid, ids in members.items() for cid in ids}

    rows = []
    for (uid, pid, d), g in groups.items():
        proc = processes.get(pid)
        st = stats.get((uid, pid, d))
        hours = st.hours_worked if st else None
        audited = st.accounts_audited if st else None
        errs = st.errors if st else None
        target = proc.daily_target if proc else None

        # Expected output for the hours worked; None if it can't be worked out.
        expected = (target * hours / STANDARD_SHIFT_HOURS) if (target and hours and hours > 0) else None
        production_pct = round(g["trans"] / expected * 100, 2) if expected else None

        # Quality % = share of audited accounts that were error-free.
        quality_pct = None
        if audited and audited > 0 and errs is not None:
            quality_pct = round((audited - errs) / audited * 100, 2)

        tid = colleague_tl.get(uid, 0)
        rows.append({
            "user_id": uid,
            "employee_name": colleagues[uid].full_name,
            "team_lead_id": tid,
            "team_lead_name": tls.get(tid, "No Team Lead"),
            "process_id": pid,
            "process_name": proc.name if proc else "",
            "work_date": d,
            "batches_worked": g["batches"],
            "total_trans_count": g["trans"],
            "daily_target": target,
            "hours_worked": hours,
            "pending_hours": st.pending_hours if st else None,
            "hours_status": st.hours_status if st else None,
            "production_pct": production_pct,
            "accounts_audited": audited,
            "errors": errs,
            "quality_pct": quality_pct,
            "is_summary": False,
            "_expected": expected,
        })
    return rows


@app.get("/batch-dashboard", response_model=List[schemas.BatchDashboardRow])
def batch_dashboard(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    process_id: Optional[int] = None,
    user_id: Optional[int] = None,
    current_user: models.User = Depends(auth.require_role("colleague", "team_lead", "admin", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    One row per colleague / process / date on which at least one order was
    marked Completed (by Posted Date). Colleagues only ever get their own
    rows; a Team Lead gets the colleagues who report to them; Admin and
    Super Admin get everyone — all enforced here, not just in the UI. Each
    row carries its Team Lead.
    """
    rows = _batch_base_rows(db, current_user, start_date, end_date, process_id, user_id)
    if not rows:
        return []

    # Newest date first, then employee, then process.
    rows.sort(key=lambda r: (-r["work_date"].toordinal(), r["employee_name"], r["process_name"]))

    # Where one colleague worked more than one process on the same date, add
    # an "Overall" row straight after that group.
    out = []
    i = 0
    while i < len(rows):
        j = i
        while j < len(rows) and rows[j]["user_id"] == rows[i]["user_id"] and rows[j]["work_date"] == rows[i]["work_date"]:
            j += 1
        group = rows[i:j]
        out.extend(group)
        if len(group) > 1:
            out.append({
                "user_id": group[0]["user_id"],
                "employee_name": group[0]["employee_name"],
                "team_lead_id": group[0]["team_lead_id"],
                "team_lead_name": group[0]["team_lead_name"],
                "process_id": 0,
                "process_name": f"Overall ({len(group)} processes)",
                "work_date": group[0]["work_date"],
                "daily_target": None,
                "is_summary": True,
                "hours_status": "Pending" if any(r.get("hours_status") == "Pending" for r in group) else None,
                **_batch_aggregate(group),
            })
        i = j
    for r in out:
        r.pop("_expected", None)
    return out


@app.get("/batch-dashboard/by-team-lead", response_model=List[schemas.BatchTeamLeadRow])
def batch_dashboard_by_team_lead(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    process_id: Optional[int] = None,
    current_user: models.User = Depends(auth.require_role("admin", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    The Batch Count Dashboard figures grouped by Team Lead over the whole
    date range (Admin / Super Admin only), plus an "All Team Leads" total
    row. Uses the same rules as the Overall row, so a Team Lead's row is the
    combined figure for everything their colleagues did in the range.
    """
    rows = _batch_base_rows(db, current_user, start_date, end_date, process_id, None)
    by_tl = {}
    for r in rows:
        by_tl.setdefault(r["team_lead_id"], []).append(r)

    def build(tid, name, group):
        return {
            "team_lead_id": tid, "team_lead_name": name,
            "colleagues": len({r["user_id"] for r in group}),
            **_batch_aggregate(group),
        }

    out = [build(tid, group[0]["team_lead_name"], group) for tid, group in by_tl.items()]
    out.sort(key=lambda r: (r["team_lead_id"] == 0, r["team_lead_name"].lower()))
    if out:
        out.append(build(-1, "All Team Leads", rows))
    return out


@app.put("/batch-dashboard", response_model=schemas.BatchDashboardRow)
def save_batch_stat(
    payload: schemas.BatchStatUpdate,
    current_user: models.User = Depends(auth.require_role("colleague", "team_lead", "admin", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    Saves the manual inputs for one colleague / process / date.
    Colleague -> Total Hours Worked on their own rows only.
    Team Lead / Super Admin -> # of Accounts Audited and # of Errors, for
    colleagues in their team (Super Admin: anyone).
    """
    if current_user.role == "admin":
        raise HTTPException(status_code=403, detail="Admin can view the Batch Dashboard but not edit it")
    data = payload.dict(exclude_unset=True)
    colleague_fields = {"hours_worked"}
    lead_fields = {"accounts_audited", "errors"}
    sent = set(data.keys()) & (colleague_fields | lead_fields)
    if not sent:
        raise HTTPException(status_code=400, detail="Nothing to save")

    if current_user.role == "colleague":
        if sent & lead_fields:
            raise HTTPException(status_code=403, detail="Accounts Audited and Errors are entered by your Team Lead")
        target_user_id = current_user.id
    else:
        if sent & colleague_fields:
            raise HTTPException(status_code=403, detail="Total Hours Worked is entered by the colleague")
        if payload.user_id is None:
            raise HTTPException(status_code=400, detail="user_id is required")
        allowed = {u.id for u in _batch_visible_colleagues(db, current_user)}
        if payload.user_id not in allowed:
            raise HTTPException(status_code=403, detail="This colleague is not on your team")
        target_user_id = payload.user_id

    _require_process_access(current_user, payload.process_id)

    # The row only exists on the dashboard if the colleague completed work
    # that day in that process.
    has_work = db.query(models.WorkOrder.id).filter(
        models.WorkOrder.assigned_to_id == target_user_id,
        models.WorkOrder.process_id == payload.process_id,
        models.WorkOrder.posting_status == "Completed",
        models.WorkOrder.posted_date == payload.work_date,
    ).first()
    if not has_work:
        raise HTTPException(status_code=404, detail="No completed work for that colleague, process and date")

    if "hours_worked" in sent and data["hours_worked"] is not None:
        if data["hours_worked"] < 0 or data["hours_worked"] > 24:
            raise HTTPException(status_code=400, detail="Total Hours Worked must be between 0 and 24")
    for f in lead_fields & sent:
        if data[f] is not None and data[f] < 0:
            raise HTTPException(status_code=400, detail="Accounts Audited and Errors can't be negative")

    st = db.query(models.DailyBatchStat).filter(
        models.DailyBatchStat.user_id == target_user_id,
        models.DailyBatchStat.process_id == payload.process_id,
        models.DailyBatchStat.work_date == payload.work_date,
    ).first()
    if not st:
        st = models.DailyBatchStat(
            user_id=target_user_id, process_id=payload.process_id, work_date=payload.work_date,
        )
        db.add(st)
    for f in sent:
        if f == "hours_worked":
            continue            # handled below: may need approval
        setattr(st, f, data[f])

    if "hours_worked" in sent:
        new_hours = data["hours_worked"]
        # The day's total across ALL processes (this one replaced by the new figure).
        others = db.query(func.coalesce(func.sum(models.DailyBatchStat.hours_worked), 0.0)).filter(
            models.DailyBatchStat.user_id == target_user_id,
            models.DailyBatchStat.work_date == payload.work_date,
            models.DailyBatchStat.process_id != payload.process_id,
        ).scalar() or 0.0
        if new_hours is not None and new_hours + others > HOURS_APPROVAL_THRESHOLD + 1e-9:
            # Over 8 hours for the day: hold it for the Team Lead. The approved
            # figure (hours_worked) is left alone until they decide.
            st.pending_hours = new_hours
            st.hours_status = "Pending"
            st.hours_decided_by = None
            st.hours_decided_at = None
        else:
            st.hours_worked = new_hours
            st.pending_hours = None
            st.hours_status = None

    if st.accounts_audited is not None and st.errors is not None and st.errors > st.accounts_audited:
        db.rollback()
        raise HTTPException(status_code=400, detail="# of Errors can't be more than # of Accounts Audited")

    st.updated_by = current_user.username
    db.commit()

    rows = batch_dashboard(
        start_date=payload.work_date, end_date=payload.work_date,
        process_id=payload.process_id, user_id=target_user_id,
        current_user=current_user, db=db,
    )
    return rows[0]


def _hours_approval_rows(db: Session, current_user: models.User, status: str):
    colleagues = {u.id: u for u in _batch_visible_colleagues(db, current_user)}
    if not colleagues:
        return []
    stats = db.query(models.DailyBatchStat).filter(
        models.DailyBatchStat.user_id.in_(list(colleagues.keys())),
        models.DailyBatchStat.hours_status == status,
    ).all()
    processes = {p.id: p.name for p in db.query(models.Process).all()}
    tls, members, _o = _tl_membership(db)
    colleague_tl = {cid: tid for tid, ids in members.items() for cid in ids}
    out = []
    for st in stats:
        others = db.query(func.coalesce(func.sum(models.DailyBatchStat.hours_worked), 0.0)).filter(
            models.DailyBatchStat.user_id == st.user_id,
            models.DailyBatchStat.work_date == st.work_date,
            models.DailyBatchStat.process_id != st.process_id,
        ).scalar() or 0.0
        req = st.pending_hours if status == "Pending" else st.hours_worked
        tid = colleague_tl.get(st.user_id, 0)
        out.append({
            "user_id": st.user_id, "employee_name": colleagues[st.user_id].full_name,
            "team_lead_id": tid, "team_lead_name": tls.get(tid, "No Team Lead"),
            "process_id": st.process_id, "process_name": processes.get(st.process_id, ""),
            "work_date": st.work_date, "requested_hours": req, "other_hours": round(float(others), 2),
            "total_hours": round(float(others) + (req or 0), 2), "status": status,
            "decided_by": st.hours_decided_by, "decided_at": st.hours_decided_at,
        })
    out.sort(key=lambda r: (-r["work_date"].toordinal(), r["employee_name"]))
    return out


@app.get("/batch-dashboard/hours-approvals", response_model=List[schemas.HoursApprovalOut])
def list_hours_approvals(
    status: str = "Pending",
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "super_admin")),
    db: Session = Depends(get_db),
):
    """Hours entries over 8 hours/day waiting (or decided). Team Lead: their team; Admin / Super Admin: everyone."""
    if status not in ("Pending", "Approved", "Rejected"):
        raise HTTPException(status_code=400, detail="status must be Pending, Approved or Rejected")
    return _hours_approval_rows(db, current_user, status)


@app.post("/batch-dashboard/hours-approvals/decide")
def decide_hours_approval(
    payload: schemas.HoursDecision,
    current_user: models.User = Depends(auth.require_role("team_lead", "super_admin")),
    db: Session = Depends(get_db),
):
    """Team Lead (or Super Admin) approves or rejects a colleague's over-8-hour entry."""
    allowed = {u.id for u in _batch_visible_colleagues(db, current_user)}
    if payload.user_id not in allowed:
        raise HTTPException(status_code=403, detail="This colleague is not on your team")
    st = db.query(models.DailyBatchStat).filter(
        models.DailyBatchStat.user_id == payload.user_id,
        models.DailyBatchStat.process_id == payload.process_id,
        models.DailyBatchStat.work_date == payload.work_date,
        models.DailyBatchStat.hours_status == "Pending",
    ).first()
    if not st:
        raise HTTPException(status_code=404, detail="No pending hours for that row")
    if payload.approve:
        st.hours_worked = st.pending_hours
        st.hours_status = "Approved"
    else:
        st.hours_status = "Rejected"
    st.pending_hours = None
    st.hours_decided_by = current_user.full_name
    st.hours_decided_at = datetime.now(IST).replace(tzinfo=None)
    db.commit()
    return {"status": st.hours_status}


@app.post("/orders/submit-day")
def submit_end_of_day(
    process_id: int,
    current_user: models.User = Depends(auth.require_role("colleague")),
    db: Session = Depends(get_db),
):
    """
    Moves all of this colleague's Completed orders IN THIS PROCESS to
    Production — hides them from both the colleague's and Team Lead's active
    queue for good. Only Completed orders are eligible; anything still
    In-Process or in Clarification is left untouched. Scoped to one process
    so a colleague working multiple processes submits each one separately.
    """
    _require_process_access(current_user, process_id)
    orders = (
        db.query(models.WorkOrder)
        .filter(
            models.WorkOrder.process_id == process_id,
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


@app.patch("/orders/{order_id}/timer/pause", response_model=schemas.WorkOrderOut)
def pause_timer(
    order_id: int,
    current_user: models.User = Depends(auth.require_role("colleague")),
    db: Session = Depends(get_db),
):
    order = db.query(models.WorkOrder).filter(models.WorkOrder.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    if order.assigned_to_id != current_user.id:
        raise HTTPException(status_code=403, detail="This order is not assigned to you")
    if order.escalated or order.posting_status == "Completed":
        raise HTTPException(status_code=400, detail="This order is locked and its timer can't be changed")
    if order.timer_status != "running":
        raise HTTPException(status_code=400, detail="Timer isn't running")
    _finalize_timer(order, "paused")
    db.commit()
    db.refresh(order)
    return order


@app.patch("/orders/{order_id}/timer/resume", response_model=schemas.WorkOrderOut)
def resume_timer(
    order_id: int,
    current_user: models.User = Depends(auth.require_role("colleague")),
    db: Session = Depends(get_db),
):
    order = db.query(models.WorkOrder).filter(models.WorkOrder.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    if order.assigned_to_id != current_user.id:
        raise HTTPException(status_code=403, detail="This order is not assigned to you")
    if order.escalated or order.posting_status == "Completed":
        raise HTTPException(status_code=400, detail="This order is locked and its timer can't be changed")
    if order.timer_status != "paused":
        raise HTTPException(status_code=400, detail="Timer isn't paused")
    order.timer_status = "running"
    order.timer_started_at = datetime.now(IST).replace(tzinfo=None)
    db.commit()
    db.refresh(order)
    return order


@app.patch("/orders/{order_id}/team-lead-correction", response_model=schemas.WorkOrderOut)
def correct_completed_order(
    order_id: int,
    payload: schemas.TeamLeadCorrection,
    current_user: models.User = Depends(auth.require_role("team_lead", "super_admin")),
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
    _require_process_access(current_user, order.process_id)
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

    reasons = _order_lock_reasons(order) if _audit_applies(current_user) else []   # state BEFORE the fix
    before = _order_snapshot(order) if reasons else None

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

    if reasons:
        _log_order_change(db, current_user, order, "correction", reasons, before)
    db.commit()
    db.refresh(order)
    return order


@app.patch("/orders/{order_id}/resolve-escalation", response_model=schemas.WorkOrderOut)
def resolve_escalation(
    order_id: int,
    payload: schemas.EscalationResolve,
    current_user: models.User = Depends(auth.require_role("team_lead", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    A Team Lead's VENTRA Comment resolves the escalation: it auto-stamps
    Issue Closed Date and unlocks the row back to the colleague — Posting
    Status stays 'Clarification' so they can finish posting it themselves.
    """
    order = db.query(models.WorkOrder).filter(models.WorkOrder.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    _require_process_access(current_user, order.process_id)
    if not order.escalated:
        raise HTTPException(status_code=400, detail="This order isn't currently escalated")

    comment = payload.ventra_comment.strip()
    if not comment:
        raise HTTPException(status_code=400, detail="VENTRA Comment can't be empty")

    order.ventra_comment = comment
    order.issue_closed_date = datetime.now(IST).date()
    order.escalated = False
    order.last_edited_by = current_user.username
    if order.timer_status == "paused":
        order.timer_status = "running"
        order.timer_started_at = datetime.now(IST).replace(tzinfo=None)
    db.commit()
    db.refresh(order)
    return order


ESCALATION_TYPES = (
    "Duplicate", "Images", "Pending Generic Account",
    "Out of Balance/Posting Clarification", "Lockbox - Posting Variance",
    "PO Box - Posting Variance", "Pulled/Logged from ORM - OOB",
    "Not a Client Payment", "Client File Name Issue", "Insurance CC",
    "Not in Batch Division", "Withhold Fee",
)


def _require_clarification_access(current_user: models.User, order: models.WorkOrder):
    """Same actors who can touch escalation_category on a row: the
    colleague it's assigned to, or a Team Lead/Super Admin with access
    to its process."""
    _deny_quality(current_user)
    if current_user.role == "colleague":
        if order.assigned_to_id != current_user.id:
            raise HTTPException(status_code=403, detail="This order is not assigned to you")
    else:
        _require_process_access(current_user, order.process_id)


@app.get("/orders/{order_id}/clarification-detail", response_model=Optional[schemas.ClarificationDetailOut])
def get_clarification_detail(
    order_id: int,
    current_user: models.User = Depends(auth.get_current_user),
    db: Session = Depends(get_db),
):
    order = db.query(models.WorkOrder).filter(models.WorkOrder.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    _require_clarification_access(current_user, order)
    return (
        db.query(models.ClarificationDetail)
        .filter(models.ClarificationDetail.order_id == order_id)
        .first()
    )


@app.put("/orders/{order_id}/clarification-detail", response_model=schemas.ClarificationDetailOut)
def save_clarification_detail(
    order_id: int,
    payload: schemas.ClarificationDetailSave,
    current_user: models.User = Depends(auth.get_current_user),
    db: Session = Depends(get_db),
):
    """
    Creates or updates the Clarification popup's detail record for one
    order. Only escalation_type comes from the person filling it in —
    everything else (deposit_type, exchange, era_check, batch numbers,
    description, team, poster_login, amount posted, and now
    clarification_details itself, copied from Poster Comment) is derived
    here from the order/process/current user, never trusted from the
    client, since the form presents those as fixed/read-only.
    """
    order = db.query(models.WorkOrder).filter(models.WorkOrder.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    _require_clarification_access(current_user, order)

    if not payload.escalation_type:
        raise HTTPException(status_code=400, detail="escalation_type is required")
    if payload.escalation_type not in ESCALATION_TYPES:
        raise HTTPException(status_code=400, detail=f"escalation_type must be one of {ESCALATION_TYPES}")

    process = db.query(models.Process).filter(models.Process.id == order.process_id).first()

    detail = (
        db.query(models.ClarificationDetail)
        .filter(models.ClarificationDetail.order_id == order_id)
        .first()
    )
    if not detail:
        detail = models.ClarificationDetail(order_id=order_id)
        db.add(detail)

    detail.deposit_type = process.name if process else None
    detail.exchange = "-"
    detail.era_check = "-"
    detail.edm_batch_number = order.edm
    detail.bar_batch_number = order.bar_batch
    detail.batch_description = order.description
    detail.team = "CBE"
    detail.poster_login = current_user.full_name
    detail.amount_posted = str(order.posted_amount) if order.posted_amount is not None else None
    detail.escalation_type = payload.escalation_type
    detail.clarification_details = order.poster_comment
    detail.updated_at = datetime.now(IST)

    db.commit()
    db.refresh(detail)
    return detail


UTILITY_CATEGORY_OPTIONS = (
    "Unmatched", "Withhold", "Interest", "MIPS", "W9 Request", "IBIS",
    "Other Bill", "Credential", "Transaction Limit Increase",
    "Unmatched Refund", "Withhold Fees", "CA Commission", "Offset Payment",
)

# Field spec per (non-Clarification) Escalation Category. Each entry is
# (key, label, kind):
#   "auto:<attr>"      -> read-only, copied from that WorkOrder attribute
#   "fixed:<value>"    -> read-only constant
#   "user"             -> read-only, current user's full name
#   "poster_comment"   -> read-only, copied from the order's Poster Comment
#   "manual"           -> free text, typed in by whoever's filling it out
#   "manual_date"      -> a date, typed in via a date picker
#   "manual_currency"  -> a dollar amount, typed in with $ formatting
#   "manual_select"    -> dropdown (Utility Category's options, currently
#                          the only one) typed in by whoever's filling it out
# Categories not listed here (e.g. "DUVA Verification") have no extra
# popup — selecting them behaves like it always did.
ESCALATION_CATEGORY_FIELDS = {
    "EOB not found": [
        ("edm_number", "EDM#", "auto:edm"),
        ("page_number", "Page #", "manual"),
        ("division_number", "Division #", "auto:division"),
        ("payer", "Payer", "manual"),
        ("check_number", "Check#", "manual"),
        ("amount", "Amount", "manual_currency"),
        ("deposit_date", "Deposit date", "auto:deposit_date"),
        ("poster_comments", "Poster Comments", "poster_comment"),
    ],
    "Invoice Creation": [
        ("division", "Division", "auto:division"),
        ("utility_category", "Utility Category", "manual_select"),
        ("edm_batch_number", "EDM Batch#", "auto:edm"),
        ("bar_batch_number", "Bar Batch #", "auto:bar_batch"),
        ("deposit_date", "Deposit Date", "auto:deposit_date"),
        ("page_number", "Page Number", "manual"),
        ("approx_invoice_count", "Approximate Invoice Count#", "manual"),
        ("notes", "Notes", "poster_comment"),
        ("poster_login", "Poster Login", "user"),
    ],
    "Patient not found": [
        ("edm_batch_number", "EDM Batch #", "auto:edm"),
        ("bar_batch_number", "BAR Batch #", "auto:bar_batch"),
        ("batch_description", "Batch Description", "auto:description"),
        ("dos", "DOS", "manual_date"),
        ("cb_migration_date", "CB Migration date", "manual_date"),
        ("notes", "Notes", "poster_comment"),
        ("team", "Team", "fixed:EDM"),
        ("poster_login", "Poster Login", "user"),
    ],
    "Need to Delete": [
        ("edm_batch_number", "EDM Batch #", "auto:edm"),
        ("status", "Status", "auto:status"),
        ("created", "Created", "auto:created"),
        ("images", "Images", "auto:image_count"),
        ("docs", "Docs", "auto:doc_count"),
        ("def_doc_type", "Def Doc Type", "auto:def_doc_type"),
        ("amount", "Amount", "auto:amount"),
        ("last_edited_by", "Last Edited By", "auto:last_edited_by"),
        ("description", "Description", "auto:description"),
        ("division", "Division", "auto:division"),
        ("bar_batch_number", "Bar Batch#", "auto:bar_batch"),
        ("notes", "Notes", "poster_comment"),
    ],
}


def _compute_escalation_auto_fields(category: str, order: models.WorkOrder, current_user: models.User) -> dict:
    result = {}
    for key, _label, kind in ESCALATION_CATEGORY_FIELDS.get(category, []):
        if kind.startswith("auto:"):
            val = getattr(order, kind.split(":", 1)[1], None)
            result[key] = str(val) if val is not None else None
        elif kind.startswith("fixed:"):
            result[key] = kind.split(":", 1)[1]
        elif kind == "user":
            result[key] = current_user.full_name
        elif kind == "poster_comment":
            result[key] = order.poster_comment
    return result


@app.get("/orders/{order_id}/escalation-detail", response_model=Optional[schemas.EscalationDetailOut])
def get_escalation_detail(
    order_id: int,
    current_user: models.User = Depends(auth.get_current_user),
    db: Session = Depends(get_db),
):
    order = db.query(models.WorkOrder).filter(models.WorkOrder.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    _require_clarification_access(current_user, order)
    return db.query(models.EscalationDetail).filter(models.EscalationDetail.order_id == order_id).first()


@app.put("/orders/{order_id}/escalation-detail", response_model=schemas.EscalationDetailOut)
def save_escalation_detail(
    order_id: int,
    payload: schemas.EscalationDetailSave,
    current_user: models.User = Depends(auth.get_current_user),
    db: Session = Depends(get_db),
):
    """
    Creates or updates the category-specific detail popup for whichever
    Escalation Category is currently selected on the row (EOB not found,
    Invoice Creation, Patient not found, Need to Delete). The category
    comes from payload.category, not order.escalation_category — the row
    is very often not saved yet at the point this popup is used (the
    colleague picks a category, which opens the popup, before ever
    clicking the row's own Save), so the order's escalation_category can
    still be None in the database. Only the category's "manual"/
    "manual_date"/"manual_currency"/"manual_select" fields come from
    payload.data — every auto/fixed/user field is recomputed here from
    the order/current user, same principle as the Clarification popup.
    """
    order = db.query(models.WorkOrder).filter(models.WorkOrder.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    _require_clarification_access(current_user, order)

    category = payload.category
    fields = ESCALATION_CATEGORY_FIELDS.get(category)
    if not fields:
        raise HTTPException(status_code=400, detail=f"No detail form defined for escalation category '{category}'")

    merged = _compute_escalation_auto_fields(category, order, current_user)
    manual_kinds = ("manual", "manual_date", "manual_currency", "manual_select")
    manual_keys = {key for key, _label, kind in fields if kind in manual_kinds}
    for key in manual_keys:
        if key in payload.data:
            merged[key] = payload.data[key]

    if "utility_category" in manual_keys and merged.get("utility_category"):
        if merged["utility_category"] not in UTILITY_CATEGORY_OPTIONS:
            raise HTTPException(status_code=400, detail=f"utility_category must be one of {UTILITY_CATEGORY_OPTIONS}")

    detail = db.query(models.EscalationDetail).filter(models.EscalationDetail.order_id == order_id).first()
    if not detail:
        detail = models.EscalationDetail(order_id=order_id, category=category)
        db.add(detail)
    detail.category = category
    detail.data = merged
    detail.posted_by_name = current_user.full_name
    detail.updated_at = datetime.now(IST)

    db.commit()
    db.refresh(detail)
    return detail


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
