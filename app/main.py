from datetime import date, datetime, timedelta, timezone
from typing import List, Optional
from dateutil import parser as date_parser
import secrets

from fastapi import Depends, FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
from sqlalchemy import or_
import csv
import io

import os

from . import models, schemas, auth, email_utils
from .database import Base, engine, get_db, SessionLocal

Base.metadata.create_all(bind=engine)

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
    if process_id is not None and not _user_has_process(user, process_id):
        raise HTTPException(status_code=403, detail="You don't have access to that process")
    token = auth.create_access_token({"sub": user.username})
    processes = db.query(models.Process).all() if user.role == "super_admin" else user.processes
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
    if payload.role not in ("colleague", "team_lead", "admin", "super_admin"):
        raise HTTPException(status_code=400, detail="role must be 'colleague', 'team_lead', 'admin', or 'super_admin'")
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
    if payload.role not in ("colleague", "team_lead", "admin", "super_admin"):
        raise HTTPException(status_code=400, detail="role must be 'colleague', 'team_lead', 'admin', or 'super_admin'")

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
        .filter(models.CelebrationComment.target_user_id.in_(target_ids))
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


@app.get("/users/colleagues", response_model=List[schemas.UserOut])
def list_colleagues(
    process_id: int,
    current_user: models.User = Depends(auth.require_role("team_lead", "super_admin")),
    db: Session = Depends(get_db),
):
    """Colleagues who have access to this process — populates the Reassign dropdown."""
    _require_process_access(current_user, process_id)
    return (
        db.query(models.User)
        .filter(models.User.role == "colleague", models.User.processes.any(models.Process.id == process_id))
        .order_by(models.User.full_name.asc())
        .all()
    )


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

    new_colleague = (
        db.query(models.User)
        .filter(
            models.User.id == payload.assigned_to_id,
            models.User.role == "colleague",
            models.User.processes.any(models.Process.id == order.process_id),
        )
        .first()
    )
    if not new_colleague:
        raise HTTPException(status_code=400, detail="Not a valid colleague for this process")

    order.assigned_to_id = new_colleague.id
    order.assigned_date = datetime.now(IST).date()
    order.employee_id = new_colleague.employee_id or new_colleague.username
    order.employee_name = new_colleague.full_name
    order.last_edited_by = current_user.username
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
    other processes' data.
    """
    _require_process_access(current_user, process_id)
    deleted_count = db.query(models.WorkOrder).filter(models.WorkOrder.process_id == process_id).delete()
    db.commit()
    return {"deleted": deleted_count}


# ---------------------------------------------------------------------------
# Inventory import (E-O) — Team Lead bulk-loads from the existing Excel export
# ---------------------------------------------------------------------------

@app.post("/orders/import")
def import_inventory(
    process_id: int,
    file: UploadFile = File(...),
    on_behalf_of_team_lead_id: Optional[int] = Form(None),
    current_user: models.User = Depends(auth.require_role("team_lead", "super_admin", "admin")),
    db: Session = Depends(get_db),
):
    """
    Accepts a CSV with columns matching E-O:
    edm,status,created,image_count,doc_count,def_doc_type,amount,description,division,deposit_date
    Creates one unassigned WorkOrder row per line, tagged to this process.
    Orders imported by a Team Lead are tagged to that Team Lead, so
    auto-assignment only offers them to colleagues reporting to that same
    Team Lead. An Admin can import on a specific Team Lead's behalf (e.g.
    covering for one who's out) by passing on_behalf_of_team_lead_id, which
    tags the import exactly as if that Team Lead had done it themselves.
    A Super Admin, or an Admin importing without specifying a Team Lead,
    leaves the order open to any colleague in the process.
    """
    _require_process_access(current_user, process_id)

    resolved_team_lead_id = None
    if current_user.role == "team_lead":
        resolved_team_lead_id = current_user.id
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
        resolved_team_lead_id = delegate.id

    content = file.file.read().decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content))
    created_count = 0
    # IST, not the server's own (UTC) date — see todays_celebrations for
    # why: near IST midnight the two disagree on which calendar day it is.
    today = datetime.now(IST).date()
    for row in reader:
        order = models.WorkOrder(
            process_id=process_id,
            team_lead_id=resolved_team_lead_id,
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
    _auto_assign_open_slots(db, process_id)
    return {"imported": created_count}


def _parse_date(v: Optional[str]) -> Optional[date]:
    if not v:
        return None
    try:
        return date_parser.parse(v.strip()).date()
    except (ValueError, OverflowError) as e:
        raise ValueError(f"Unrecognized date format: {v!r}") from e


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
    For every colleague who has access to this process and currently has no
    open (non-completed) order *in this process*, hand them the oldest
    unassigned order they're actually eligible for. An order imported by a
    Team Lead only goes to colleagues whose Reporting Manager matches that
    same Team Lead's name; an order imported by a Super Admin (team_lead_id
    is None) is open to any colleague in the process.
    """
    colleagues = (
        db.query(models.User)
        .filter(models.User.role == "colleague", models.User.processes.any(models.Process.id == process_id))
        .all()
    )
    for colleague in colleagues:
        has_open = (
            db.query(models.WorkOrder)
            .filter(
                models.WorkOrder.process_id == process_id,
                models.WorkOrder.assigned_to_id == colleague.id,
                models.WorkOrder.posting_status != "Completed",
            )
            .first()
        )
        if has_open:
            continue

        candidates = (
            db.query(models.WorkOrder)
            .filter(models.WorkOrder.process_id == process_id, models.WorkOrder.assigned_to_id.is_(None))
            .order_by(models.WorkOrder.id.asc())
            .all()
        )
        next_order = None
        for cand in candidates:
            if cand.team_lead_id is None:
                next_order = cand
                break
            owner = db.query(models.User).filter(models.User.id == cand.team_lead_id).first()
            if owner and colleague.reporting_manager == owner.full_name:
                next_order = cand
                break

        if next_order:
            next_order.assigned_to_id = colleague.id
            next_order.assigned_date = datetime.now(IST).date()
            next_order.employee_id = colleague.employee_id or colleague.username
            next_order.employee_name = colleague.full_name
            next_order.posting_status = "In-Process"
            db.add(next_order)
            db.flush()
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
    _require_process_access(current_user, process_id)
    query = db.query(models.WorkOrder).filter(
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
    return query.order_by(models.WorkOrder.id.asc()).all()


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
    Everything already submitted to Production, within one process. Team
    Lead sees everyone's; a colleague only ever sees their own — enforced
    server-side regardless of what filters are passed, not just hidden in
    the UI. All filters are optional and combine with AND. start_date/end_date
    filter by Posted Date (inclusive) — the day the work was actually completed.
    """
    _require_process_access(current_user, process_id)
    query = db.query(models.WorkOrder).filter(
        models.WorkOrder.submitted == True,  # noqa: E712
        models.WorkOrder.process_id == process_id,
    )
    if current_user.role == "colleague":
        query = query.filter(models.WorkOrder.assigned_to_id == current_user.id)

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

    # Poster Comment, Escalation Category, and Issue Raised Date only make
    # sense while a Clarification is open — locked both during In-Process
    # and once Completed. (VENTRA Comment and Issue Closed Date are excluded
    # here entirely since they're never colleague-editable — see the pops
    # above.)
    CLARIFICATION_ONLY_FIELDS = [
        "poster_comment", "escalation_category", "issue_raised_date",
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
        order.issue_raised_date = datetime.now(IST).date()

    # Auto-set Posted Date the moment a colleague marks Completed — no manual
    # entry needed, and it guarantees TAT can always be calculated below.
    if order.posting_status == "Completed" and not order.posted_date:
        order.posted_date = datetime.now(IST).date()

    # Resolving a Clarification (moving to any other status) auto-stamps
    # Issue Closed Date, mirroring how Issue Raised Date auto-stamps on the
    # way in. Escalation Category and Issue Raised Date are deliberately
    # NOT cleared anymore — they're kept as history so the TAT pause-window
    # calculation at Completion stays accurate.
    if previous_status == "Clarification" and order.posting_status != "Clarification" and not order.issue_closed_date:
        order.issue_closed_date = datetime.now(IST).date()

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
        _auto_assign_open_slots(db, order.process_id)

    return order


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
