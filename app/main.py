from datetime import date, datetime, timedelta, timezone
from typing import List, Optional
from dateutil import parser as date_parser
from dateutil.relativedelta import relativedelta
import secrets
import uuid
import logging
import contextvars
import threading
import time
import collections
import shutil
from logging.handlers import RotatingFileHandler

from fastapi import Depends, FastAPI, HTTPException, UploadFile, File, Form, Response, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.orm import Session, selectinload
from sqlalchemy import or_, and_, case, cast, String, false as sa_false, func, inspect as sa_inspect, text as sa_text
import csv
import json
import io
import re
import base64

import os
import sys

from . import models, schemas, auth, email_utils, syslock
from .database import Base, engine, get_db, SessionLocal

Base.metadata.create_all(bind=engine)


# Columns added to tables that already exist. create_all() only creates NEW
# tables, so each of these is added here if it's missing — safe to run on every
# start, which means a deploy no longer depends on someone running the ALTER by hand.
# Escalation teams that work a queue of their own. Which team an escalation goes to
# depends on its category: EOB not found -> Recon (and Recon may pass it on to Calling);
# Patient not found / Invoice Creation / Clarification -> Onshore.
TEAM_ROLES = ("onshore", "recon", "calling")
TEAM_LABELS = {"onshore": "Onshore", "recon": "Recon", "calling": "Calling"}
TEAM_FOR_CATEGORY = {
    "Patient not found": "onshore", "Invoice Creation": "onshore", "Clarification": "onshore",
    "EOB not found": "recon",
}

_ADDED_COLUMNS = [
    ("users", "last_login_date", "DATE"),
    ("work_orders", "onshore_status", "VARCHAR"),
    ("onshore_messages", "track", "VARCHAR"),
    ("escalation_reminders", "track", "VARCHAR"),
    ("work_orders", "onshore_comment", "TEXT"),
    ("work_orders", "onshore_tl_reply", "TEXT"),
    ("work_orders", "onshore_sent_at", "TIMESTAMP"),
    ("work_orders", "onshore_team", "VARCHAR"),
    ("import_exceptions", "row_json", "TEXT"),
    ("daily_batch_stats", "comment", "TEXT"),
    ("daily_batch_stats", "pending_hours", "DOUBLE PRECISION"),
    ("daily_batch_stats", "hours_status", "VARCHAR"),
    ("daily_batch_stats", "hours_decided_by", "VARCHAR"),
    ("daily_batch_stats", "hours_decided_at", "TIMESTAMP"),
    ("celebration_comments", "image_b64", "TEXT"),
    ("celebration_comments", "image_type", "VARCHAR"),
    ("work_orders", "division_name", "VARCHAR"),
    ("work_orders", "run", "VARCHAR"),
    ("work_orders", "inits", "VARCHAR"),
    ("work_orders", "ecom_type", "VARCHAR"),
    ("work_orders", "eobs", "INTEGER"),
    ("work_orders", "checks", "INTEGER"),
    ("work_orders", "not_posted", "INTEGER"),
    ("work_orders", "not_posted_amount", "DOUBLE PRECISION"),
    ("work_orders", "offset_amount", "DOUBLE PRECISION"),
    ("work_orders", "posted_835_amount", "DOUBLE PRECISION"),
    ("break_logs", "comment", "VARCHAR"),
    ("users", "photo", "TEXT"),
    ("users", "shift", "VARCHAR"),
    ("process_updates", "ack_required", "BOOLEAN DEFAULT FALSE"),
    ("session_logs", "auto_closed", "BOOLEAN"),
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

# Indexes on the columns every queue / dashboard query filters by. IF NOT EXISTS makes this safe on
# every start (SQLite and PostgreSQL); a failure never blocks startup.
_INDEXES = [
    ("ix_wo_process", "work_orders", "process_id"),
    ("ix_wo_assigned", "work_orders", "assigned_to_id"),
    ("ix_wo_received", "work_orders", "received_date"),
    ("ix_wo_posting_status", "work_orders", "posting_status"),
    ("ix_wo_edm", "work_orders", "edm"),
    ("ix_wo_proc_received", "work_orders", "process_id, received_date"),
    ("ix_wo_team_lead", "work_orders", "team_lead_id"),
    ("ix_impexc_process", "import_exceptions", "process_id, created_at"),
    ("ix_celeb_target", "celebration_comments", "target_user_id, occasion_date"),
    ("ix_wo_proc_status_recv", "work_orders", "process_id, posting_status, received_date"),
    ("ix_wo_posted", "work_orders", "posted_date"),
    ("ix_wo_submitted", "work_orders", "submitted, process_id"),
    ("ix_wo_assigned_posted", "work_orders", "assigned_to_id, posted_date"),
    ("ix_wo_onshore_status", "work_orders", "onshore_status"),
    ("ix_login_user_date", "login_days", "user_id, login_date"),
    ("ix_leave_user_date", "leave_days", "user_id, leave_date"),
    ("ix_batch_date", "daily_batch_stats", "work_date, process_id"),
    ("ix_escev_raised", "escalation_events", "process_id, raised_date"),
    ("ix_onmsg_order", "onshore_messages", "order_id"),
    ("ix_qerr_user_date", "quality_errors", "user_id, error_date"),
    ("ix_esrem_order", "escalation_reminders", "order_id"),
]


def _ensure_indexes():
    try:
        insp = sa_inspect(engine)
        tables = set(insp.get_table_names())
        for name, table, cols in _INDEXES:
            if table not in tables:
                continue
            try:
                with engine.begin() as conn:
                    conn.execute(sa_text(f"CREATE INDEX IF NOT EXISTS {name} ON {table} ({cols})"))
            except Exception as exc:
                print(f"[startup] index {name} skipped: {exc}")
    except Exception as exc:
        print(f"[startup] could not create indexes: {exc}")


_ensure_indexes()

_rid_var = contextvars.ContextVar("rid", default="-")


def _log_dir() -> str:
    return os.getenv("LOG_DIR") or os.path.join(os.getcwd(), "logs")


class _ISTFormatter(logging.Formatter):
    """Log times in India time so they match what people say ('around 10:42')."""
    def formatTime(self, record, datefmt=None):
        t = datetime.utcfromtimestamp(record.created) + timedelta(hours=5, minutes=30)
        return t.strftime("%Y-%m-%d %H:%M:%S")


def _setup_logging():
    """Rotating log file (5 x 2 MB) next to the database: logs/workflowhub.log. Errors, logins, changes and slow requests land here, each with a request ID."""
    lg = logging.getLogger("workflowhub")
    if lg.handlers:
        return
    lg.setLevel(logging.INFO)
    lg.propagate = False
    try:
        d = _log_dir()
        os.makedirs(d, exist_ok=True)
        h = RotatingFileHandler(os.path.join(d, "workflowhub.log"), maxBytes=2_000_000, backupCount=5, encoding="utf-8")
        h.setFormatter(_ISTFormatter("%(asctime)s %(levelname)s %(message)s"))
        lg.addHandler(h)
    except Exception:
        lg.addHandler(logging.StreamHandler())


_setup_logging()

app = FastAPI(title="WorkFlow Hub")

# Compress anything over 1 KB (the page itself, big order lists): ~70-80% less data on the network.
app.add_middleware(GZipMiddleware, minimum_size=1000)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


_RC = {}                                   # (user token, path+query) -> (expires, status, body, media_type)
_RC_LOCK = threading.Lock()
_RC_TTL = 30                               # seconds
_RC_PATHS = ("/performance/data", "/forecast", "/clients/dashboard", "/escalations/reasons", "/workload", "/orders/aging",
             "/orders/escalation-aging", "/orders/time-benchmarks", "/quality/errors/summary", "/reports/management-summary")


@app.middleware("http")
async def dashboard_cache(request, call_next):
    """Heavy read-only dashboards are remembered for 30 seconds per user, and forgotten at once when anyone saves a change.
    (With several server workers a change made on another worker can take up to 30 seconds to show on these dashboards.)"""
    import time as _t
    from fastapi.responses import Response as _R
    m = request.method
    if m in ("POST", "PUT", "PATCH", "DELETE"):
        response = await call_next(request)
        if response.status_code < 400:
            with _RC_LOCK:
                _RC.clear()
        return response
    path = request.url.path
    if m != "GET" or path not in _RC_PATHS:
        return await call_next(request)
    key = (request.headers.get("authorization", ""), path + "?" + request.url.query)
    now = _t.time()
    with _RC_LOCK:
        hit = _RC.get(key)
        if hit and hit[0] > now:
            return _R(content=hit[2], status_code=hit[1], headers={**hit[3], "X-Cache": "HIT"})
    response = await call_next(request)
    if response.status_code == 200:
        body = b"".join([chunk async for chunk in response.body_iterator])
        with _RC_LOCK:
            if len(_RC) > 500:
                for k in [k for k, v in _RC.items() if v[0] < now]:
                    _RC.pop(k, None)
            hdrs = {k: v for k, v in response.headers.items() if k.lower() not in ("content-length", "x-cache")}
            _RC[key] = (now + _RC_TTL, 200, body, hdrs)
        return _R(content=body, status_code=200, headers={**hdrs, "X-Cache": "MISS"})
    return response


def _log_user(request) -> str:
    try:
        from jose import jwt as _jwt
        h = request.headers.get("authorization", "")
        if h.lower().startswith("bearer "):
            return str(_jwt.get_unverified_claims(h[7:]).get("sub") or "-")
    except Exception:
        pass
    return "-"


# ---- endpoint timing for the System health screen (per worker, kept in memory) ----
_EP: dict = {}
_EP_LOCK = threading.Lock()
_STARTED = datetime.now(timezone(timedelta(hours=5, minutes=30)))


def _ep_record(request, status: int, ms: float):
    route = request.scope.get("route")
    key = request.method + " " + (getattr(route, "path", None) or re.sub(r"/\d+", "/{id}", request.url.path))
    with _EP_LOCK:
        e = _EP.get(key)
        if e is None:
            if len(_EP) > 400:
                return
            e = _EP[key] = {"n": 0, "total": 0.0, "max": 0.0, "errors": 0, "recent": collections.deque(maxlen=300)}
        e["n"] += 1; e["total"] += ms; e["max"] = max(e["max"], ms); e["recent"].append(ms)
        if status >= 500:
            e["errors"] += 1


@app.middleware("http")
async def request_log(request, call_next):
    """Gives every request an ID (also sent back as the X-Request-ID header), and logs: every error with its traceback,
    every change (POST/PUT/PATCH/DELETE), refused logins / permissions, and anything slower than 2 seconds."""
    import time as _t
    from fastapi.responses import JSONResponse
    rid = uuid.uuid4().hex[:8]
    _rid_var.set(rid)
    request.state.rid = rid
    path = request.url.path
    lg = logging.getLogger("workflowhub")
    t0 = _t.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        lg.exception("rid=%s user=%s %s %s UNHANDLED ERROR", rid, _log_user(request), request.method, path)
        return JSONResponse({"detail": f"Something went wrong. Please tell the Super Admin this reference: {rid}"}, status_code=500, headers={"X-Request-ID": rid})
    response.headers["X-Request-ID"] = rid
    ms = (_t.perf_counter() - t0) * 1000
    st = response.status_code
    if path != "/health" and not path.startswith("/static") and path != "/":
        try:
            _ep_record(request, st, ms)
        except Exception:
            pass
    if path == "/health" or path.startswith("/static") or (request.method == "GET" and st < 400 and ms < 2000 and path != "/auth/login"):
        return response
    msg = "rid=%s user=%s %s %s -> %s %.0fms" % (rid, _log_user(request), request.method, path, st, ms)
    if st >= 500:
        lg.error(msg)
    elif st in (401, 403, 429) or ms >= 2000:
        lg.warning(msg + (" SLOW" if ms >= 2000 else ""))
    elif request.method != "GET" and st < 400:
        lg.info(msg)
    elif st >= 400:
        lg.info(msg)
    return response


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
        # "no-cache" = always ask the server, but the server answers 304 (nothing to download) when the page is unchanged.
        response.headers["Cache-Control"] = "no-cache, must-revalidate"
    return response


@app.get("/health")
def health():
    """For IT monitoring / auto-restart: confirms the app is up and the database answers. No login needed, no data returned."""
    try:
        db = SessionLocal()
        try:
            db.execute(sa_text("SELECT 1"))
        finally:
            db.close()
    except Exception:
        raise HTTPException(status_code=503, detail="database unavailable")
    return {"status": "ok", "time": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")}


@app.on_event("startup")
def start_digest_scheduler():
    import threading
    threading.Thread(target=_digest_loop, daemon=True, name="digest").start()
    threading.Thread(target=_scheduled_jobs_loop, daemon=True, name="jobs").start()


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
    """
    Called whenever an order is freshly (re)assigned — resets the clock to zero
    but does NOT start it: Time Taken only runs after the colleague clicks Start.
    """
    order.timer_status = "not_started"
    order.timer_started_at = None
    order.time_taken_seconds = 0


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Login throttling: slow down repeated wrong passwords
# ---------------------------------------------------------------------------
_THROTTLE = {}
_THROTTLE_LOCK = threading.Lock()
_THROTTLE_WINDOW = 900            # failures are counted over 15 minutes
_THROTTLE_RULES = {"u": (5, 60), "ip": (30, 300), "pw": (5, 60)}      # kind -> (failures allowed, first lock in seconds); the lock doubles each time, max 15 min


def _client_ip(request) -> str:
    if os.getenv("TRUST_PROXY") == "1":
        xf = request.headers.get("x-forwarded-for")
        if xf:
            return xf.split(",")[0].strip()
    return request.client.host if request.client else "?"


def _throttle_wait(keys) -> int:
    """Seconds this caller must still wait (0 = free to try)."""
    now = datetime.utcnow().timestamp()
    with _THROTTLE_LOCK:
        w = 0
        for k in keys:
            e = _THROTTLE.get(k)
            if e and e["until"] > now:
                w = max(w, int(e["until"] - now) + 1)
        return w


def _throttle_fail(kind_keys) -> int:
    """Record one failure for each (kind, key); returns the lock seconds started now (0 = none)."""
    now = datetime.utcnow().timestamp()
    started = 0
    with _THROTTLE_LOCK:
        if len(_THROTTLE) > 5000:
            for k in [k for k, e in _THROTTLE.items() if e["until"] < now and now - e["first"] > _THROTTLE_WINDOW]:
                _THROTTLE.pop(k, None)
        for kind, k in kind_keys:
            allowed, base = _THROTTLE_RULES[kind]
            e = _THROTTLE.get(k)
            if e is None or (e["until"] < now and now - e["first"] > _THROTTLE_WINDOW):
                e = _THROTTLE[k] = {"n": 0, "first": now, "until": 0, "level": 0}
            e["n"] += 1
            if e["n"] >= allowed:
                e["level"] += 1
                secs = min(900, base * 2 ** (e["level"] - 1))
                e["until"] = now + secs
                started = max(started, secs)
    return started


def _throttle_clear(keys):
    with _THROTTLE_LOCK:
        for k in keys:
            _THROTTLE.pop(k, None)


def _throttle_raise(secs: int):
    mins = max(1, (secs + 59) // 60)
    raise HTTPException(status_code=429, detail=f"Too many wrong attempts. Please wait {mins} minute{'s' if mins != 1 else ''} and try again.",
                        headers={"Retry-After": str(secs)})


@app.post("/auth/login", response_model=schemas.LoginResponse)
def login(
    request: Request,
    form_data: OAuth2PasswordRequestForm = Depends(),
    process_id: Optional[int] = Form(None),
    db: Session = Depends(get_db),
):
    ip = _client_ip(request)
    ukey = f"u:{(form_data.username or '').strip().lower()}|{ip}"
    ipkey = f"ip:{ip}"
    lg = logging.getLogger("workflowhub")
    wait = _throttle_wait([ukey, ipkey])
    if wait:
        lg.warning("rid=%s login BLOCKED user=%s ip=%s wait=%ss", getattr(request.state, "rid", "-"), form_data.username, ip, wait)
        _throttle_raise(wait)
    user = db.query(models.User).filter(models.User.username == form_data.username).first()
    if not user or not auth.verify_password(form_data.password, user.password_hash):
        lg.warning("rid=%s login FAILED user=%s ip=%s", getattr(request.state, "rid", "-"), form_data.username, ip)
        started = _throttle_fail([("u", ukey), ("ip", ipkey)])
        if started:
            lg.warning("rid=%s login LOCKED user=%s ip=%s for %ss", getattr(request.state, "rid", "-"), form_data.username, ip, started)
            _audit(db, None, "Security", "Login locked after repeated wrong passwords", None, None, f"user '{(form_data.username or '')[:40]}' from {ip} for {max(1, started // 60)} min")
            db.commit()
            _throttle_raise(started)
        raise HTTPException(status_code=401, detail="Incorrect username or password")
    _throttle_clear([ukey])
    lg.info("rid=%s login OK user=%s ip=%s", getattr(request.state, "rid", "-"), form_data.username, ip)
    if user.employment_status == "Inactive":
        raise HTTPException(status_code=403, detail="This account is inactive")
    # Quality (view/export Production only) can open any process; everyone else needs it assigned.
    if process_id is not None and user.role not in ("quality", "onshore", "recon", "calling") and not _user_has_process(user, process_id):
        raise HTTPException(status_code=403, detail="You don't have access to that process")
    token = auth.create_access_token({"sub": user.username})
    processes = db.query(models.Process).all() if user.role in ("super_admin", "quality") else user.processes
    if user.role in TEAM_ROLES:
        processes = []          # these queues span every process; no process picker
    if user.role == "colleague":
        user.last_login_date = _work_today()     # logged in today -> eligible for auto-assignment
        db.commit()
        _record_login_day(db, user.id)
        _open_session(db, user.id)
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
    pwkey = f"pw:{current_user.id}"
    wait = _throttle_wait([pwkey])
    if wait:
        _throttle_raise(wait)
    if not auth.verify_password(payload.old_password, current_user.password_hash):
        started = _throttle_fail([("pw", pwkey)])
        if started:
            _throttle_raise(started)
        raise HTTPException(status_code=401, detail="Current password is incorrect")
    _throttle_clear([pwkey])
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
    current_user: models.User = Depends(auth.require_role("colleague", "team_lead", "admin", "manager", "quality", "onshore", "recon", "calling")),
    db: Session = Depends(get_db),
):
    """
    Self-service profile edit for Colleague, Team Lead, Admin, Quality and Onshore:
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
    _audit(db, current_user, "Setting", "Changed session timeout", details=f"{setting.value if setting else 'default'} -> {payload.minutes} minutes")
    if setting:
        setting.value = str(payload.minutes)
    else:
        setting = models.AppSetting(key=SESSION_TIMEOUT_SETTING_KEY, value=str(payload.minutes))
        db.add(setting)
    db.commit()
    return {"minutes": payload.minutes}


SHIFT_OPTIONS = [
    "Day Shift (08:00 AM - 05:00 PM)", "Day Shift (09:00 AM - 06:00 PM)",
    "Mid Shift (03:30 PM - 12:30 AM)",
    "Night Shift (05:30 PM - 02:30 AM)", "Night Shift (06:30 PM - 03:30 AM)",
]


def _clean_shift(v):
    v = (v or "").strip()
    if not v:
        return None
    if v not in SHIFT_OPTIONS:
        raise HTTPException(status_code=400, detail="Choose a shift from the list")
    return v


BREAK_IDLE_SETTING_KEY = "break_idle_minutes"
DEFAULT_BREAK_IDLE_MINUTES = 5
BREAK_TYPES = ["Tea Break", "Lunch Break", "Dinner Break", "Other Break", "Meeting Break"]


BREAK_LOCK_SYSTEM_KEY = "break_lock_system"


def _break_settings(db: Session) -> dict:
    row = db.query(models.AppSetting).filter(models.AppSetting.key == BREAK_IDLE_SETTING_KEY).first()
    sysrow = db.query(models.AppSetting).filter(models.AppSetting.key == BREAK_LOCK_SYSTEM_KEY).first()
    return {"minutes": int(row.value) if row else DEFAULT_BREAK_IDLE_MINUTES,
            "lock_system": (sysrow.value != "0") if sysrow else True}


@app.get("/settings/break-idle", response_model=schemas.BreakIdleSetting)
def get_break_idle(current_user: models.User = Depends(auth.get_current_user), db: Session = Depends(get_db)):
    """Minutes of no activity after which a colleague's screen locks for a break."""
    return _break_settings(db)


@app.put("/settings/break-idle", response_model=schemas.BreakIdleSetting)
def set_break_idle(
    payload: schemas.BreakIdleSetting,
    current_user: models.User = Depends(auth.require_role("super_admin")),
    db: Session = Depends(get_db),
):
    if payload.minutes < 1 or payload.minutes > 120:
        raise HTTPException(status_code=400, detail="Enter 1 to 120 minutes")
    _audit(db, current_user, "Setting", "Changed break lock settings", details=f"Idle minutes {payload.minutes}; lock whole PC {'off' if payload.lock_system is False else 'on'}")
    for key, val in ((BREAK_IDLE_SETTING_KEY, str(payload.minutes)),
                     (BREAK_LOCK_SYSTEM_KEY, "0" if payload.lock_system is False else "1")):
        row = db.query(models.AppSetting).filter(models.AppSetting.key == key).first()
        if row:
            row.value = val
        else:
            db.add(models.AppSetting(key=key, value=val))
    db.commit()
    return _break_settings(db)


def _local_request(request: Request) -> bool:
    host = request.client.host if request.client else ""
    return host in ("127.0.0.1", "::1", "localhost")


@app.post("/breaks/lock-system")
def lock_system_screen(request: Request, payload: Optional[dict] = None, current_user: models.User = Depends(auth.require_role("colleague"))):
    """
    Shows the full-screen break lock in front of every application and blocks
    Alt+Tab / Win key / Win+D (see app/syslock.py). Only possible when this
    server runs on the colleague's own Windows PC and the request comes from
    that PC; on a hosted server there is nothing to lock.
    """
    if not _local_request(request) or not syslock.IS_WIN:
        return {"locked": False}
    start = None
    try:
        if payload and payload.get("started_at"):
            start = datetime.fromisoformat(str(payload["started_at"]).replace("Z", "+00:00")).timestamp()
    except ValueError:
        start = None
    return {"locked": syslock.start(start)}


@app.get("/breaks/idle")
def system_idle(request: Request, current_user: models.User = Depends(auth.require_role("colleague"))):
    """Seconds since any mouse/keyboard input on this PC (only when the server runs on the colleague's own Windows PC)."""
    if not _local_request(request):
        return {"idle_seconds": None}
    return {"idle_seconds": syslock.idle_seconds()}


@app.get("/breaks/lock-status")
def lock_system_status(request: Request, current_user: models.User = Depends(auth.require_role("colleague"))):
    if not _local_request(request):
        return {"active": False, "resumed": False, "chosen": ""}
    return syslock.status()


@app.post("/breaks/unlock-system")
def unlock_system_screen(request: Request, current_user: models.User = Depends(auth.require_role("colleague"))):
    if _local_request(request):
        syslock.stop()
        syslock.clear_result()
    return {"ok": True}


@app.post("/breaks")
def record_break(
    payload: schemas.BreakIn,
    current_user: models.User = Depends(auth.require_role("colleague")),
    db: Session = Depends(get_db),
):
    """Saved when the colleague presses Resume: the break ends now."""
    if payload.break_type not in BREAK_TYPES:
        raise HTTPException(status_code=400, detail="Pick a break type: " + ", ".join(BREAK_TYPES))
    ended = datetime.utcnow()
    started = ended
    if payload.started_at:
        try:
            dt = datetime.fromisoformat(payload.started_at.replace("Z", "+00:00"))
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            started = dt
        except ValueError:
            started = ended
    if started > ended:
        started = ended
    if ended - started > timedelta(hours=16):
        started = ended - timedelta(hours=16)
    row = models.BreakLog(
        user_id=current_user.id, process_id=payload.process_id, break_type=payload.break_type,
        source="manual" if payload.source == "manual" else "auto",
        started_at=started, ended_at=ended, minutes=round((ended - started).total_seconds() / 60, 2),
        comment=((payload.comment or "").strip()[:300] or None),
    )
    db.add(row)
    _away_clear(db, current_user.id)
    db.commit()
    return {"id": row.id, "minutes": row.minutes}


@app.get("/breaks")
def list_breaks(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    current_user: models.User = Depends(auth.require_role("colleague", "team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    """Breaks in an IST date range. Colleague: own; Team Lead: their team; Admin / Super Admin: everyone."""
    today = datetime.now(IST).date()
    try:
        d1 = date.fromisoformat(date_from) if date_from else today.replace(day=1)
        d2 = date.fromisoformat(date_to) if date_to else today
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date")
    if (d2 - d1).days > 92:
        raise HTTPException(status_code=400, detail="Choose a range of 92 days or less")
    def _utc(d):
        return datetime(d.year, d.month, d.day, tzinfo=IST).astimezone(timezone.utc).replace(tzinfo=None)
    lo, hi = _utc(d1), _utc(d2 + timedelta(days=1))
    q = db.query(models.BreakLog).filter(models.BreakLog.started_at >= lo, models.BreakLog.started_at < hi)
    if current_user.role == "colleague":
        q = q.filter(models.BreakLog.user_id == current_user.id)
    elif current_user.role == "team_lead":
        ids = [u.id for u in _batch_visible_colleagues(db, current_user)]
        q = q.filter(models.BreakLog.user_id.in_(ids or [-1]))
    rows = q.order_by(models.BreakLog.started_at.desc()).limit(5000).all()
    retype = {}
    for a in db.query(models.BreakAdjustment).filter(models.BreakAdjustment.break_id.in_([r.id for r in rows] or [-1])).order_by(models.BreakAdjustment.id).all():
        if a.new_type:
            retype[a.break_id] = a.new_type
    out = []
    for r in rows:
        u = r.user
        out.append({
            "id": r.id, "user_id": r.user_id, "employee": u.full_name if u else "", "employee_id": u.employee_id if u else None,
            "team_lead": (u.reporting_manager if u else None) or "", "break_type": retype.get(r.id, r.break_type),
            "original_type": r.break_type if r.id in retype else "", "source": r.source,
            "started_at": r.started_at.isoformat(), "ended_at": r.ended_at.isoformat(), "minutes": r.minutes,
            "comment": r.comment or "",
        })
    return out


def _shift_start_minutes(shift):
    """Start of a profile shift, e.g. 'Day Shift (08:00 AM - 05:00 PM)' -> minutes after midnight (None if no shift)."""
    import re as _re
    m = _re.search(r"\((\d{1,2}):(\d{2})\s*(AM|PM)", shift or "", _re.I)
    if not m:
        return None
    h, mi, ap = int(m.group(1)) % 12, int(m.group(2)), m.group(3).upper()
    return (h + (12 if ap == "PM" else 0)) * 60 + mi


def _shift_length_minutes(shift):
    """Length of a profile shift in minutes, e.g. 'Day Shift (09:00 AM - 06:00 PM)' -> 540 (None if it can't be read)."""
    import re as _re
    m = _re.search(r"\((\d{1,2}):(\d{2})\s*(AM|PM)\s*-\s*(\d{1,2}):(\d{2})\s*(AM|PM)", shift or "", _re.I)
    if not m:
        return None
    a = (int(m.group(1)) % 12 + (12 if m.group(3).upper() == "PM" else 0)) * 60 + int(m.group(2))
    b = (int(m.group(4)) % 12 + (12 if m.group(6).upper() == "PM" else 0)) * 60 + int(m.group(5))
    return (b - a) % 1440 or 1440


LATE_GRACE_MIN = 5          # login up to 5 minutes after the shift start is not late
LIM_TEA_LUNCH, LIM_OTHER = 60, 30
LIM_CLOCKED, LIM_PRODUCTION = 540, 450      # 9h clocked, 7:30h production per working day
LEFT_EARLY_MIN = 15                         # logging out more than 15 minutes before the shift ends is flagged
LIM_LONG_BREAK = 120                        # a single break over 2 hours is flagged


def _hol_set(db: Session, d1: date, d2: date) -> set:
    return {h.holiday_date for h in db.query(models.Holiday).filter(models.Holiday.holiday_date >= d1, models.Holiday.holiday_date <= d2).all()}


def _is_off_day(d: date, holidays: set) -> bool:
    """Weekend or company holiday: late login and clocked / production hour checks are not applied."""
    return d.weekday() >= 5 or d in holidays


def _ist_dt(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc).astimezone(IST)


def _ist_bounds(d1: date, d2: date):
    lo = datetime(d1.year, d1.month, d1.day, tzinfo=IST).astimezone(timezone.utc).replace(tzinfo=None)
    hi = datetime(d2.year, d2.month, d2.day, tzinfo=IST).astimezone(timezone.utc).replace(tzinfo=None) + timedelta(days=1)
    return lo, hi


def _scope_ids(db: Session, current_user: models.User, active_only: bool = False):
    """None = everybody (Admin / Super Admin); otherwise the visible colleague ids."""
    if current_user.role == "colleague":
        return [current_user.id]
    if current_user.role == "team_lead":
        us = _batch_visible_colleagues(db, current_user)
        return [u.id for u in us if not active_only or u.employment_status != "Inactive"]
    return None


def _collect_days(db: Session, ids, d1: date, d2: date) -> dict:
    """
    Per (user_id, IST date): first login, last logout, clocked minutes (overlapping sessions counted once),
    whether a session is live, and the break totals + reasons. One place for every break report.
    """
    lo, hi = _ist_bounds(d1, d2)
    now = datetime.utcnow()
    days = {}
    def day(uid, d):
        return days.setdefault((uid, d), {"ivs": [], "live": False, "tea_lunch": 0.0, "other": 0.0, "meeting": 0.0, "breaks": 0, "reasons": [], "longest": 0.0, "no_logout": False, "bk": [], "bt": {}})
    sq = db.query(models.SessionLog).filter(models.SessionLog.login_at >= lo, models.SessionLog.login_at < hi)
    bq = db.query(models.BreakLog).filter(models.BreakLog.started_at >= lo, models.BreakLog.started_at < hi)
    if ids is not None:
        sq = sq.filter(models.SessionLog.user_id.in_(ids or [-1]))
        bq = bq.filter(models.BreakLog.user_id.in_(ids or [-1]))
    for sl in sq.all():
        end = sl.logout_at or sl.last_seen
        if end < sl.login_at:
            end = sl.login_at
        live = sl.logout_at is None and (now - sl.last_seen) < timedelta(minutes=5)
        if live:
            end = max(end, now)
        d = day(sl.user_id, _ist_dt(sl.login_at).date())
        d["ivs"].append((sl.login_at, end))
        d["live"] = d["live"] or live
        if sl.auto_closed or (sl.logout_at is None and not live):      # no logout recorded for this session
            d["no_logout"] = True
    # Team Lead / Admin corrections: a break can be re-typed, and a day's flags can be excused (see BreakAdjustment).
    aq = db.query(models.BreakAdjustment).filter(models.BreakAdjustment.adj_date >= d1, models.BreakAdjustment.adj_date <= d2)
    if ids is not None:
        aq = aq.filter(models.BreakAdjustment.user_id.in_(ids or [-1]))
    retype, excuse, adj_notes = {}, {}, {}
    for a in aq.order_by(models.BreakAdjustment.id).all():
        if a.break_id and a.new_type:
            retype[a.break_id] = a.new_type
        for x in (a.excused or "").split(","):
            if x:
                excuse.setdefault((a.user_id, a.adj_date), set()).add(x)
        adj_notes.setdefault((a.user_id, a.adj_date), []).append(a.note + (" (" + a.created_by_name + ")" if a.created_by_name else ""))
    for b in bq.all():
        d = day(b.user_id, _ist_dt(b.started_at).date())
        m = b.minutes or 0
        bt = retype.get(b.id, b.break_type)
        if bt in ("Tea Break", "Lunch Break", "Dinner Break"):
            d["tea_lunch"] += m
        elif bt == "Other Break":
            d["other"] += m
        else:
            d["meeting"] += m
        d["breaks"] += 1
        tt = d["bt"].setdefault(bt, [0, 0.0]); tt[0] += 1; tt[1] += m
        d["longest"] = max(d["longest"], m)
        d["bk"].append(b.comment or "")
        if b.comment:
            d["reasons"].append(b.comment)
    lq = db.query(models.LeaveDay).filter(models.LeaveDay.leave_date >= d1, models.LeaveDay.leave_date <= d2)
    if ids is not None:
        lq = lq.filter(models.LeaveDay.user_id.in_(ids or [-1]))
    for lv in lq.all():
        if (lv.user_id, lv.leave_date) in days:
            days[(lv.user_id, lv.leave_date)]["leave"] = lv.kind
    sc = db.query(models.ShiftChange).filter(models.ShiftChange.date_from <= d2, models.ShiftChange.date_to >= d1)
    if ids is not None:
        sc = sc.filter(models.ShiftChange.user_id.in_(ids or [-1]))
    sc = sc.order_by(models.ShiftChange.id).all()
    for (uid, dd), dv in days.items():
        for c_ in sc:
            if c_.user_id == uid and c_.date_from <= dd <= c_.date_to:
                dv["shift"] = c_.shift
    rq = db.query(models.BreakRequest).filter(models.BreakRequest.status == "Approved", models.BreakRequest.req_date >= d1, models.BreakRequest.req_date <= d2)
    if ids is not None:
        rq = rq.filter(models.BreakRequest.user_id.in_(ids or [-1]))
    for r_ in rq.all():
        if (r_.user_id, r_.req_date) in days:
            dv = days[(r_.user_id, r_.req_date)]
            dv["extra"] = dv.get("extra", 0) + r_.extra_minutes
            adj_notes.setdefault((r_.user_id, r_.req_date), []).append(f"Approved break request +{r_.extra_minutes}m: {r_.reason}" + (" (" + r_.decided_by_name + ")" if r_.decided_by_name else ""))
    for (uid, dd), ex in excuse.items():
        day(uid, dd)["excused"] = ex
    for (uid, dd), notes in adj_notes.items():
        day(uid, dd)["adj"] = notes
    for d in days.values():
        d.setdefault("excused", set()); d.setdefault("adj", []); d.setdefault("leave", None); d.setdefault("shift", None); d.setdefault("extra", 0)
        d["ivs"].sort()
        merged = []
        for a, b in d["ivs"]:
            if merged and a <= merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], b)
            else:
                merged.append([a, b])
        d["first_login"] = d["ivs"][0][0] if d["ivs"] else None
        d["last_logout"] = merged[-1][1] if merged else None
        d["clocked"] = round(sum((b - a).total_seconds() for a, b in merged) / 60, 1)
        d["brk_total"] = d["tea_lunch"] + d["other"] + d["meeting"]
        d["production"] = max(0.0, d["clocked"] - d["brk_total"]) if merged else 0.0
    return days


def _late_minutes(first_login, shift, grace=LATE_GRACE_MIN):
    """Minutes after the shift start (None = on time, or no shift set / no login)."""
    st = _shift_start_minutes(shift)
    if st is None or first_login is None:
        return None
    t = _ist_dt(first_login)
    over = t.hour * 60 + t.minute - st
    return over if over > grace else None


def _day_flags(d: dict, shift, day_date: date, off: bool, today: date):
    """Policy flags for one person-day."""
    f = []
    ex = d.get("excused", set())
    shift = d.get("shift") or shift          # a temporary shift change for that day wins over the profile shift
    off = off or bool(d.get("leave"))        # a marked leave day is not checked for late login / hour targets
    ongoing = day_date >= today and d["live"]
    late = None if (off or "late" in ex) else _late_minutes(d["first_login"], shift)
    if late is not None:
        f.append("Late login")
    if "break" not in ex:
        extra = d.get("extra", 0)          # extra break minutes a Team Lead approved in advance for this day
        over = max(0.0, d["tea_lunch"] - LIM_TEA_LUNCH) + max(0.0, d["other"] - LIM_OTHER)
        if over > extra:
            if d["tea_lunch"] > LIM_TEA_LUNCH:
                f.append("Tea + Lunch over 1h")
            if d["other"] > LIM_OTHER:
                f.append("Other break over 30m")
        if d["longest"] > LIM_LONG_BREAK + extra:
            f.append("Single break over 2h")
    if d.get("no_logout") and "hours" not in ex and day_date < today:
        f.append("No logout recorded")
    if not off and not ongoing and "hours" not in ex and d["first_login"] is not None:
        if d["clocked"] < LIM_CLOCKED:
            f.append("Clocked under 9h")
        if d["production"] < LIM_PRODUCTION:
            f.append("Production under 7:30h")
        st_, ln_ = _shift_start_minutes(shift), _shift_length_minutes(shift)
        if st_ is not None and ln_ and d["last_logout"] is not None and not d.get("no_logout"):
            due = datetime(day_date.year, day_date.month, day_date.day) + timedelta(minutes=st_ + ln_)
            if _ist_dt(d["last_logout"]).replace(tzinfo=None) < due - timedelta(minutes=LEFT_EARLY_MIN):
                f.append("Left early")
    return f, late


@app.get("/breaks/month-summary")
def break_month_summary(
    current_user: models.User = Depends(auth.require_role("colleague", "team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    Current IST month, per person: days logged in more than 5 minutes after the shift start (late login)
    and days over the break limit. Weekends and holidays are not checked for late login.
    Scope: colleague own / Team Lead team / Admin + Super Admin everyone.
    """
    today = datetime.now(IST).date()
    first = today.replace(day=1)
    ids = _scope_ids(db, current_user)
    days = _collect_days(db, ids, first, today)
    hol = _hol_set(db, first, today)
    users = {u.id: u for u in db.query(models.User).filter(models.User.id.in_({k[0] for k in days} or {-1})).all()}
    res = {}
    for (uid, d), v in sorted(days.items(), key=lambda x: x[0][1]):
        u = users.get(uid)
        flags, late = _day_flags(v, u.shift if u else None, d, _is_off_day(d, hol), today)
        over = any(x in flags for x in ("Tea + Lunch over 1h", "Other break over 30m", "Single break over 2h"))
        if late is None and not over:
            continue
        r = res.setdefault(uid, {"user_id": uid, "employee": u.full_name if u else "", "team_lead": (u.reporting_manager if u else None) or "",
                                 "shift": (u.shift if u else None) or "", "late_days": 0, "over_days": 0, "late_dates": [], "over_dates": []})
        if late is not None:
            r["late_days"] += 1; r["late_dates"].append(d.isoformat())
        if over:
            r["over_days"] += 1; r["over_dates"].append(d.isoformat())
    out = list(res.values())
    uq = db.query(models.User).filter(models.User.role == "colleague", models.User.employment_status == "Active")
    if ids is not None:
        uq = uq.filter(models.User.id.in_(ids or [-1]))
    no_shift = sum(1 for u in uq.all() if _shift_start_minutes(u.shift) is None)
    return {"month": today.strftime("%B %Y"), "no_shift": no_shift, "grace": LATE_GRACE_MIN, "late_days": sum(r["late_days"] for r in out),
            "over_days": sum(r["over_days"] for r in out), "users": out}


# ---------------------------------------------------------------------------
# Daily exception digest: yesterday's late logins / break-limit days / short days, emailed to each Team Lead
# (their team) and, optionally, a full summary to Admins and Super Admins. Needs SMTP (see email_utils.py).
# ---------------------------------------------------------------------------
DIGEST_KEYS = {"enabled": "digest_enabled", "time": "digest_time", "admins": "digest_admins", "last": "digest_last_sent"}


def _setting(db: Session, key: str, default: str = "") -> str:
    row = db.query(models.AppSetting).filter(models.AppSetting.key == key).first()
    return row.value if row else default


def _set_setting(db: Session, key: str, val: str):
    row = db.query(models.AppSetting).filter(models.AppSetting.key == key).first()
    if row:
        row.value = val
    else:
        db.add(models.AppSetting(key=key, value=val))


def _digest_settings(db: Session) -> dict:
    return {"enabled": _setting(db, DIGEST_KEYS["enabled"], "0") == "1", "time": _setting(db, DIGEST_KEYS["time"], "09:00"),
            "admins": _setting(db, DIGEST_KEYS["admins"], "1") == "1", "last_sent": _setting(db, DIGEST_KEYS["last"], ""),
            "weekly_enabled": _setting(db, WEEKLY_KEYS["enabled"], "0") == "1", "weekly_time": _setting(db, WEEKLY_KEYS["time"], "09:00"),
            "weekly_last": _setting(db, WEEKLY_KEYS["last"], ""),
            "smtp_configured": email_utils.is_configured()}


def _digest_rows(db: Session, users: list, day_: date) -> list:
    """Exception rows for these colleagues on one IST day (same rules as the Exceptions report)."""
    today = datetime.now(IST).date()
    ids = [u.id for u in users]
    days = _collect_days(db, ids, day_, day_)
    hol = _hol_set(db, day_, day_)
    by = {u.id: u for u in users}
    out = []
    for (uid, d), v in days.items():
        u = by.get(uid)
        if not u:
            continue
        flags, late = _day_flags(v, u.shift, d, _is_off_day(d, hol), today)
        if flags:
            out.append({"employee": u.full_name, "team_lead": u.reporting_manager or "", "shift": (u.shift or "").split(" (")[0],
                        "login": _ist_dt(v["first_login"]).strftime("%I:%M %p") if v["first_login"] else "", "late": late or 0,
                        "tea_lunch": round(v["tea_lunch"]), "other": round(v["other"]), "clocked": round(v["clocked"]),
                        "production": round(v["production"]), "flags": ", ".join(flags), "reasons": "; ".join(v["reasons"])})
    out.sort(key=lambda r: (r["team_lead"], r["employee"]))
    return out


def _fmt_m(m: int) -> str:
    return f"{m // 60}h {m % 60:02d}m" if m >= 60 else f"{m}m"


def _digest_email(title: str, day_: date, rows: list, show_tl: bool):
    heads = ["Employee"] + (["Team Lead"] if show_tl else []) + ["Login", "Tea+Lunch", "Other", "Clocked", "Production", "Exceptions", "Reasons"]
    def cells(r):
        return [r["employee"]] + ([r["team_lead"]] if show_tl else []) + [(r["login"] + (f" ({r['late']}m late)" if r["late"] else "")), _fmt_m(r["tea_lunch"]),
                _fmt_m(r["other"]), _fmt_m(r["clocked"]), _fmt_m(r["production"]), r["flags"], r["reasons"]]
    import html as _h
    text = f"{title}\nDate: {day_.strftime('%A, %d %b %Y')}\n\n" + "\n".join(" | ".join(cells(r)) for r in rows) + \
        "\n\nLate login = more than 5 minutes after the shift start. Limits: Tea + Lunch 1h, Other 30m, clocked 9h, production 7:30h. " \
        "Weekends, holidays and days excused by a Team Lead are not counted.\n"
    th = "".join(f'<th style="background:#5b2a63;color:#fff;padding:6px 8px;text-align:left;font-size:12px;">{_h.escape(x)}</th>' for x in heads)
    trs = "".join("<tr>" + "".join(f'<td style="padding:5px 8px;border-bottom:1px solid #e3e7ef;font-size:12.5px;">{_h.escape(str(c))}</td>' for c in cells(r)) + "</tr>" for r in rows)
    html = (f'<div style="font-family:Segoe UI,Arial,sans-serif;color:#222;"><h3 style="color:#0b2545;margin:0 0 4px;">{_h.escape(title)}</h3>'
            f'<div style="color:#666;font-size:13px;margin-bottom:10px;">{_h.escape(day_.strftime("%A, %d %b %Y"))} &middot; {len(rows)} exception{"s" if len(rows) != 1 else ""}</div>'
            f'<table style="border-collapse:collapse;"><tr>{th}</tr>{trs}</table>'
            '<p style="color:#888;font-size:11.5px;margin-top:10px;">Late login = more than 5 minutes after the shift start. Limits: Tea + Lunch 1h, Other 30m, clocked 9h, production 7:30h. '
            'Weekends, holidays and days excused by a Team Lead are not counted. Sent automatically by WorkFlow Hub.</p></div>')
    return text, html


def run_exception_digest(db: Session, day_: date = None, only_to: str = None) -> dict:
    """Send yesterday's digest. only_to = send the full summary to just that address (test)."""
    day_ = day_ or (datetime.now(IST).date() - timedelta(days=1))
    cfg = _digest_settings(db)
    colleagues = [u for u in db.query(models.User).filter(models.User.role == "colleague", models.User.employment_status != "Inactive").all()]
    all_rows = _digest_rows(db, colleagues, day_)
    res = {"date": day_.isoformat(), "exceptions": len(all_rows), "sent": 0, "failed": [], "skipped_no_email": []}
    def send(to, title, rows, show_tl):
        text, html = _digest_email(title, day_, rows, show_tl)
        if email_utils.send_email(to, f"{title} - {day_.strftime('%d %b %Y')}", text, html):
            res["sent"] += 1
        else:
            res["failed"].append(f"{to}: {email_utils.last_error}")
    if only_to:
        if all_rows:
            send(only_to, "Break Tracker exceptions - all teams (test)", all_rows, True)
        else:
            res["note"] = "No exceptions yesterday, so no test mail was sent."
        return res
    tls = db.query(models.User).filter(models.User.role == "team_lead", models.User.employment_status != "Inactive").all()
    for tl in tls:
        rows = [r for r in all_rows if r["team_lead"] == tl.full_name]
        if not rows:
            continue
        if not tl.email:
            res["skipped_no_email"].append(tl.full_name); continue
        send(tl.email, "Break Tracker exceptions - your team", rows, False)
    if cfg["admins"] and all_rows:
        for a in db.query(models.User).filter(models.User.role.in_(["admin", "manager", "super_admin"]), models.User.employment_status != "Inactive").all():
            if a.email:
                send(a.email, "Break Tracker exceptions - all teams", all_rows, True)
    return res


_JOBS_TOKEN = f"{os.getpid()}-{uuid.uuid4().hex[:8]}"


def _jobs_leader(db: Session) -> bool:
    """With several server workers/processes, only ONE may run the timed jobs (backups, summary / reminder emails).
    A heartbeat row in app_settings decides who the leader is; if the leader stops for 3 minutes another takes over."""
    now = datetime.utcnow()
    row = db.query(models.AppSetting).filter(models.AppSetting.key == "jobs_leader").first()
    mine = f"{_JOBS_TOKEN}|{now.isoformat()}"
    if row is None:
        try:
            db.add(models.AppSetting(key="jobs_leader", value=mine)); db.commit()
            return True
        except Exception:
            db.rollback()
            return False
    old = row.value
    try:
        tok, ts = old.split("|", 1)
        stale = (now - datetime.fromisoformat(ts)).total_seconds() > 180
    except Exception:
        tok, stale = "", True
    if tok != _JOBS_TOKEN and not stale:
        return False
    n = db.query(models.AppSetting).filter(models.AppSetting.key == "jobs_leader", models.AppSetting.value == old).update({"value": mine})
    db.commit()
    return n == 1


def _digest_loop():
    import time as _t
    while True:
        _t.sleep(60)
        db = SessionLocal()
        try:
            if not _jobs_leader(db):
                continue
            cfg = _digest_settings(db)
            if not cfg["smtp_configured"]:
                continue
            now = datetime.now(IST)
            if cfg["weekly_enabled"] and now.weekday() == 0:          # Monday: send last week's summary
                try:
                    wh, wm = (int(x) for x in cfg["weekly_time"].split(":"))
                except ValueError:
                    wh, wm = 9, 0
                if (now.hour, now.minute) >= (wh, wm) and cfg["weekly_last"] != now.date().isoformat():
                    _set_setting(db, WEEKLY_KEYS["last"], now.date().isoformat())
                    db.commit()
                    print(f"[weekly] {run_weekly_summary(db)}")
            if not cfg["enabled"]:
                continue
            try:
                hh, mm = (int(x) for x in cfg["time"].split(":"))
            except ValueError:
                hh, mm = 9, 0
            if (now.hour, now.minute) >= (hh, mm) and cfg["last_sent"] != now.date().isoformat():
                _set_setting(db, DIGEST_KEYS["last"], now.date().isoformat())     # mark first: never send twice
                db.commit()
                r = run_exception_digest(db)
                print(f"[digest] {r}")
        except Exception as e:
            print("[digest] error:", e)
        finally:
            db.close()


@app.get("/settings/digest")
def get_digest_settings(current_user: models.User = Depends(auth.require_role("super_admin")), db: Session = Depends(get_db)):
    return _digest_settings(db)


@app.put("/settings/digest")
def set_digest_settings(payload: dict, current_user: models.User = Depends(auth.require_role("super_admin")), db: Session = Depends(get_db)):
    t = str(payload.get("time", "09:00"))
    if not re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", t):
        raise HTTPException(status_code=400, detail="Time must look like 09:00")
    _set_setting(db, DIGEST_KEYS["enabled"], "1" if payload.get("enabled") else "0")
    _set_setting(db, DIGEST_KEYS["time"], t)
    _set_setting(db, DIGEST_KEYS["admins"], "1" if payload.get("admins", True) else "0")
    wt = str(payload.get("weekly_time", "09:00"))
    if not re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", wt):
        raise HTTPException(status_code=400, detail="Weekly time must look like 09:00")
    _set_setting(db, WEEKLY_KEYS["enabled"], "1" if payload.get("weekly_enabled") else "0")
    _set_setting(db, WEEKLY_KEYS["time"], wt)
    _audit(db, current_user, "Setting", "Changed email summary settings", details=f"Daily email {'on' if payload.get('enabled') else 'off'} at {t}; to Admins {'yes' if payload.get('admins', True) else 'no'}; weekly email {'on' if payload.get('weekly_enabled') else 'off'} at {wt}")
    db.commit()
    return _digest_settings(db)


@app.post("/settings/digest/send")
def send_digest_now(mode: str = "test", current_user: models.User = Depends(auth.require_role("super_admin")), db: Session = Depends(get_db)):
    """mode=test: yesterday's full summary to your own email. mode=now: the real digest to every Team Lead / Admin now."""
    if not email_utils.is_configured():
        raise HTTPException(status_code=400, detail=email_utils.last_error or "SMTP is not configured on the server (see the SMTP lines in run.bat).")
    _audit(db, current_user, "Setting", "Sent email summary", details=f"mode={mode}")
    db.commit()
    if mode in ("weekly_test", "weekly_now"):
        if mode == "weekly_test":
            if not current_user.email:
                raise HTTPException(status_code=400, detail="Add your email address in My Profile / your user record first.")
            return run_weekly_summary(db, only_to=current_user.email)
        return run_weekly_summary(db)
    if mode == "test":
        if not current_user.email:
            raise HTTPException(status_code=400, detail="Add your email address in My Profile / your user record first.")
        return run_exception_digest(db, only_to=current_user.email)
    return run_exception_digest(db)


ADJ_KINDS = ("late", "break", "hours")


def _audit(db: Session, actor, category: str, action: str, target=None, ref_date=None, details: str = ""):
    """Queue one audit row in the caller's transaction (it is saved by the caller's db.commit())."""
    try:
        db.add(models.AuditEvent(at=datetime.utcnow(), actor_id=getattr(actor, "id", None), actor_name=getattr(actor, "full_name", None) or "system",
                                 actor_role=getattr(actor, "role", None), category=category, action=action,
                                 target_user_id=getattr(target, "id", None), target_name=getattr(target, "full_name", None),
                                 ref_date=str(ref_date) if ref_date else None, details=(details or "")[:600]))
    except Exception as e:           # auditing must never break the action itself
        print("[audit] could not queue:", e)


def _order_diff_text(before: dict, order) -> str:
    after = _order_snapshot(order)
    parts = [f"{k}: {str(before.get(k))[:40]} -> {str(v)[:40]}" for k, v in after.items() if before.get(k) != v and k != "posted_date_text"]
    return "; ".join(parts[:10]) + (f" (+{len(parts) - 10} more)" if len(parts) > 10 else "")


def _audit_order(db: Session, actor, action: str, order, details: str = ""):
    """One audit row for a Team Lead / Admin action on an order (shown against the colleague it was assigned to)."""
    who = None
    try:
        who = order.assignee if order.assigned_to_id else None
    except Exception:
        who = None
    _audit(db, actor, "Order", action, who, None, f"{order.edm or '#' + str(order.id)} ({order.process.name if order.process else ''}): {details}")


def _may_adjust(db: Session, current_user: models.User, user_id: int):
    u = db.query(models.User).filter(models.User.id == user_id, models.User.role == "colleague").first()
    if not u:
        raise HTTPException(status_code=404, detail="Colleague not found")
    if current_user.role == "team_lead" and u.reporting_manager != current_user.full_name:
        raise HTTPException(status_code=403, detail="That colleague is not in your team")
    return u


@app.post("/breaks/adjustments")
def add_break_adjustment(
    payload: schemas.AdjustmentIn,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    """Excuse flags of one day (late login / break limits / hour targets) and / or change one break's type. A note is required."""
    u = _may_adjust(db, current_user, payload.user_id)
    try:
        day_ = date.fromisoformat(payload.date)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date")
    if day_ > datetime.now(IST).date():
        raise HTTPException(status_code=400, detail="You cannot adjust a future date")
    note = (payload.note or "").strip()
    if len(note) < 3:
        raise HTTPException(status_code=400, detail="Please write a short reason for the adjustment")
    excused = [x for x in dict.fromkeys(payload.excused or []) if x]
    if any(x not in ADJ_KINDS for x in excused):
        raise HTTPException(status_code=400, detail="Unknown item to excuse")
    old_type = None
    if payload.break_id or payload.new_type:
        if payload.new_type not in BREAK_TYPES:
            raise HTTPException(status_code=400, detail="Pick a break type: " + ", ".join(BREAK_TYPES))
        b = db.query(models.BreakLog).filter(models.BreakLog.id == payload.break_id, models.BreakLog.user_id == u.id).first() if payload.break_id else None
        if not b or _ist_dt(b.started_at).date() != day_:
            raise HTTPException(status_code=400, detail="That break does not belong to this colleague and day")
        old_type = b.break_type
        for a in db.query(models.BreakAdjustment).filter(models.BreakAdjustment.break_id == b.id, models.BreakAdjustment.new_type.isnot(None)).order_by(models.BreakAdjustment.id).all():
            old_type = a.new_type
        if old_type == payload.new_type:
            raise HTTPException(status_code=400, detail="The break already has that type")
    elif not excused:
        raise HTTPException(status_code=400, detail="Choose what to excuse or which break to change")
    row = models.BreakAdjustment(user_id=u.id, adj_date=day_, excused=",".join(excused) or None, break_id=payload.break_id if payload.new_type else None,
                                 old_type=old_type, new_type=payload.new_type if payload.break_id else None, note=note[:300],
                                 created_by_name=current_user.full_name, created_at=datetime.utcnow())
    db.add(row)
    bits = []
    if excused:
        bits.append("Excused: " + ", ".join(excused))
    if payload.new_type:
        bits.append(f"Break {old_type} -> {payload.new_type}")
    _audit(db, current_user, "Adjustment", "Added adjustment", u, day_, "; ".join(bits) + " | Reason: " + note[:300])
    db.commit()
    return {"id": row.id}


@app.get("/breaks/adjustments")
def list_break_adjustments(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    current_user: models.User = Depends(auth.require_role("colleague", "team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    today = datetime.now(IST).date()
    try:
        d1 = date.fromisoformat(date_from) if date_from else today.replace(day=1)
        d2 = date.fromisoformat(date_to) if date_to else today
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date")
    q = db.query(models.BreakAdjustment).filter(models.BreakAdjustment.adj_date >= d1, models.BreakAdjustment.adj_date <= d2)
    ids = _scope_ids(db, current_user)
    if ids is not None:
        q = q.filter(models.BreakAdjustment.user_id.in_(ids or [-1]))
    return [{"id": a.id, "user_id": a.user_id, "employee": a.user.full_name if a.user else "", "date": a.adj_date.isoformat(),
             "excused": [x for x in (a.excused or "").split(",") if x], "break_id": a.break_id, "old_type": a.old_type or "", "new_type": a.new_type or "",
             "note": a.note, "by": a.created_by_name or "", "at": a.created_at.isoformat() + "Z" if a.created_at else None}
            for a in q.order_by(models.BreakAdjustment.adj_date.desc(), models.BreakAdjustment.id.desc()).all()]


@app.delete("/breaks/adjustments/{adj_id}")
def delete_break_adjustment(
    adj_id: int,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    a = db.query(models.BreakAdjustment).filter(models.BreakAdjustment.id == adj_id).first()
    if not a:
        raise HTTPException(status_code=404, detail="Adjustment not found")
    tu = _may_adjust(db, current_user, a.user_id)
    _audit(db, current_user, "Adjustment", "Removed adjustment", tu, a.adj_date, ((("Excused: " + a.excused + "; ") if a.excused else "") + (f"Break {a.old_type} -> {a.new_type}; " if a.new_type else "") + "Reason was: " + (a.note or "")))
    db.delete(a)
    db.commit()
    return {"ok": True}


def _reason_cat(c: str) -> str:
    c = (c or "").strip().lower()
    if not c:
        return "none"
    if c.startswith("medical"):
        return "medical"
    if c.startswith("personal"):
        return "personal"
    return "other"


def _period_range(period: str):
    today = datetime.now(IST).date()
    if period == "week":
        return today - timedelta(days=today.weekday()), today, "This week"
    if period == "last_month":
        last = today.replace(day=1) - timedelta(days=1)
        return last.replace(day=1), last, last.strftime("%B %Y")
    return today.replace(day=1), today, today.strftime("%B %Y") + " (so far)"


@app.get("/breaks/heatmap")
def break_heatmap(
    period: str = "month",
    shift: Optional[str] = None,
    team_lead: Optional[str] = None,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    When are people away at the same time? For every weekday x hour (IST): the average number of colleagues away
    and the highest number away at once, built from the start and end time of every break. Night breaks that run past midnight are split by day.
    """
    if period not in ("week", "month", "last_month"):
        raise HTTPException(status_code=400, detail="period must be week, month or last_month")
    d1, d2, label = _period_range(period)
    users = [u for u in _batch_visible_colleagues(db, current_user) if u.employment_status != "Inactive"]
    shifts = sorted({((u.shift or "").split(" (")[0].strip() or "(No shift)") for u in users})
    tls = sorted({u.reporting_manager for u in users if u.reporting_manager}) if current_user.role != "team_lead" else []
    if shift:
        users = [u for u in users if ((u.shift or "").split(" (")[0].strip() or "(No shift)") == shift]
    if team_lead:
        users = [u for u in users if (u.reporting_manager or "") == team_lead]
    ids = [u.id for u in users]
    lo, hi = _ist_bounds(d1, d2)
    diffs, active = {}, set()
    if ids:
        for sl in db.query(models.SessionLog.login_at).filter(models.SessionLog.user_id.in_(ids), models.SessionLog.login_at >= lo, models.SessionLog.login_at < hi).all():
            active.add(_ist_dt(sl[0]).date())
        for b in db.query(models.BreakLog).filter(models.BreakLog.user_id.in_(ids), models.BreakLog.started_at >= lo, models.BreakLog.started_at < hi).all():
            st, en = _ist_dt(b.started_at).replace(tzinfo=None), _ist_dt(b.ended_at).replace(tzinfo=None)
            if en <= st:
                continue
            cur = st
            while cur < en:
                day_end = datetime(cur.year, cur.month, cur.day) + timedelta(days=1)
                seg_end = min(en, day_end)
                a_min = cur.hour * 60 + cur.minute
                b_min = a_min + max(1, int(round((seg_end - cur).total_seconds() / 60)))
                dd = diffs.setdefault(cur.date(), [0] * 1442)
                dd[a_min] += 1
                dd[min(b_min, 1440)] -= 1
                active.add(cur.date())
                cur = seg_end
    # per date: minute-by-minute people away -> hourly average and hourly peak
    avg_sum = [[0.0] * 24 for _ in range(7)]
    peak = [[0] * 24 for _ in range(7)]
    peak_on = [[None] * 24 for _ in range(7)]
    n_days = [0] * 7
    for d in sorted(x for x in active if d1 <= x <= d2):
        wd = d.weekday()
        n_days[wd] += 1
        arr = diffs.get(d)
        if not arr:
            continue
        run, mins = 0, []
        for m in range(1440):
            run += arr[m]
            mins.append(run)
        for h in range(24):
            seg = mins[h * 60:(h + 1) * 60]
            avg_sum[wd][h] += sum(seg) / 60.0
            mx = max(seg)
            if mx > peak[wd][h]:
                peak[wd][h] = mx
                peak_on[wd][h] = d.isoformat()
    grid = []
    for wd in range(7):
        row = []
        for h in range(24):
            row.append({"avg": round(avg_sum[wd][h] / n_days[wd], 2) if n_days[wd] else None, "peak": peak[wd][h], "peak_on": peak_on[wd][h]})
        grid.append(row)
    allrow = []
    tot_days = sum(n_days)
    for h in range(24):
        pk = max(peak[w][h] for w in range(7))
        allrow.append({"avg": round(sum(avg_sum[w][h] for w in range(7)) / tot_days, 2) if tot_days else None, "peak": pk})
    cells = [(grid[w][h]["avg"] or 0, grid[w][h]["peak"], w, h) for w in range(7) for h in range(24) if grid[w][h]["avg"]]
    top = [{"weekday": w, "hour": h, "avg": a_, "peak": p_, "peak_on": grid[w][h]["peak_on"]} for a_, p_, w, h in sorted(cells, reverse=True)[:5]]
    return {"label": label, "from": d1.isoformat(), "to": d2.isoformat(), "colleagues": len(ids), "days_counted": n_days, "grid": grid, "all": allrow, "top": top,
            "shifts": shifts, "team_leads": tls}


@app.get("/breaks/insights")
def break_insights(
    period: str = "month",
    min_days: int = 3,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    Repeat offenders (min_days or more exception days in the period) and, for Admin / Super Admin,
    a Team Lead scorecard: share of colleagues with no exception day. Excused days do not count.
    """
    if period not in ("week", "month", "last_month"):
        raise HTTPException(status_code=400, detail="period must be week, month or last_month")
    min_days = max(1, min(min_days, 31))
    d1, d2, label = _period_range(period)
    today = datetime.now(IST).date()
    users = [u for u in _batch_visible_colleagues(db, current_user) if u.employment_status != "Inactive"]
    ids = [u.id for u in users]
    days = _collect_days(db, ids, d1, d2)
    hol = _hol_set(db, d1, d2)
    per = {u.id: {"u": u, "measured": 0, "exc": 0, "late": [], "brk": [], "hrs": [], "clk": 0.0, "prod": 0.0, "done": 0, "dates": [], "bmin": 0.0, "bdays": 0, "short": 0} for u in users}
    for (uid, d), v in sorted(days.items(), key=lambda x: x[0][1]):
        r = per.get(uid)
        if not r or v["first_login"] is None:
            continue
        r["measured"] += 1
        off = _is_off_day(d, hol)
        flags, late = _day_flags(v, r["u"].shift, d, off, today)
        if "Late login" in flags:
            r["late"].append(d.isoformat())
        if any(x in flags for x in ("Tea + Lunch over 1h", "Other break over 30m", "Single break over 2h")):
            r["brk"].append(d.isoformat())
        if any(x in flags for x in ("Clocked under 9h", "Production under 7:30h", "No logout recorded", "Left early")):
            r["hrs"].append(d.isoformat())
        if flags:
            r["exc"] += 1
            r["dates"].append(d.isoformat())
        if not off and not v.get("leave") and not (d >= today and v["live"]):
            r["done"] += 1; r["clk"] += v["clocked"]; r["prod"] += v["production"]
            r["bmin"] += v["brk_total"]; r["bdays"] += 1
            if "Clocked under 9h" in flags or "Production under 7:30h" in flags or "Left early" in flags:
                r["short"] += 1
    # Shift comparison (Day / Mid / Night ...): same numbers grouped by the shift name.
    sg = {}
    for r in per.values():
        nm = ((r["u"].shift or "").split(" (")[0].strip()) or "(No shift)"
        g = sg.setdefault(nm, {"shift": nm, "colleagues": 0, "measured": 0, "compliant": 0, "exc": 0, "late": 0, "brk": 0, "short": 0, "clk": 0.0, "prod": 0.0, "done": 0, "bmin": 0.0, "bdays": 0})
        g["colleagues"] += 1
        if r["measured"]:
            g["measured"] += 1
            g["compliant"] += 1 if r["exc"] == 0 else 0
        g["exc"] += r["exc"]; g["late"] += len(r["late"]); g["brk"] += len(r["brk"]); g["short"] += r["short"]
        g["clk"] += r["clk"]; g["prod"] += r["prod"]; g["done"] += r["done"]; g["bmin"] += r["bmin"]; g["bdays"] += r["bdays"]
    shifts = [{"shift": g["shift"], "colleagues": g["colleagues"], "measured": g["measured"],
               "compliance_pct": round(100 * g["compliant"] / g["measured"]) if g["measured"] else None,
               "exception_days": g["exc"], "late_days": g["late"], "break_days": g["brk"], "short_days": g["short"],
               "avg_break": round(g["bmin"] / g["bdays"], 1) if g["bdays"] else 0,
               "avg_clocked": round(g["clk"] / g["done"], 1) if g["done"] else 0,
               "avg_production": round(g["prod"] / g["done"], 1) if g["done"] else 0} for g in sg.values()]
    shifts.sort(key=lambda x: x["shift"])
    offenders = []
    for r in per.values():
        if r["exc"] >= min_days:
            offenders.append({"user_id": r["u"].id, "employee": r["u"].full_name, "team_lead": r["u"].reporting_manager or "", "shift": r["u"].shift or "",
                              "exception_days": r["exc"], "late_days": len(r["late"]), "break_days": len(r["brk"]), "hours_days": len(r["hrs"]),
                              "late_dates": r["late"], "break_dates": r["brk"], "hours_dates": r["hrs"], "worked_days": r["measured"]})
    offenders.sort(key=lambda x: (-x["exception_days"], x["employee"]))
    # Reasons given on the lock screen, per person (and per Team Lead for Admin / Super Admin).
    rp = {}
    for (uid, d), v in days.items():
        u = next((x for x in users if x.id == uid), None) if False else per.get(uid, {}).get("u")
        if not u or not v["bk"]:
            continue
        e = rp.setdefault(uid, {"user_id": uid, "employee": u.full_name, "team_lead": u.reporting_manager or "", "breaks": 0, "medical": 0, "personal": 0, "other": 0, "none": 0})
        for c in v["bk"]:
            e["breaks"] += 1
            e[_reason_cat(c)] += 1
    reasons = sorted(rp.values(), key=lambda e: (-(e["medical"] + e["personal"] + e["other"]), e["employee"]))
    rteams = []
    if current_user.role in ("admin", "manager", "super_admin"):
        g = {}
        for e in reasons:
            t = g.setdefault(e["team_lead"] or "(No Team Lead)", {"team_lead": e["team_lead"] or "(No Team Lead)", "breaks": 0, "medical": 0, "personal": 0, "other": 0, "none": 0})
            for k in ("breaks", "medical", "personal", "other", "none"):
                t[k] += e[k]
        rteams = sorted(g.values(), key=lambda t: t["team_lead"])
    rtotal = {k: sum(e[k] for e in reasons) for k in ("breaks", "medical", "personal", "other", "none")}
    score = []
    if current_user.role in ("admin", "manager", "super_admin"):
        grp = {}
        for r in per.values():
            g = grp.setdefault(r["u"].reporting_manager or "(No Team Lead)", {"team_lead": r["u"].reporting_manager or "(No Team Lead)", "colleagues": 0, "measured": 0, "compliant": 0,
                                                                              "exception_days": 0, "late": 0, "brk": 0, "hrs": 0, "clk": 0.0, "prod": 0.0, "done": 0})
            g["colleagues"] += 1
            if r["measured"]:
                g["measured"] += 1
                g["compliant"] += 1 if r["exc"] == 0 else 0
            g["exception_days"] += r["exc"]; g["late"] += len(r["late"]); g["brk"] += len(r["brk"]); g["hrs"] += len(r["hrs"])
            g["clk"] += r["clk"]; g["prod"] += r["prod"]; g["done"] += r["done"]
        for g in grp.values():
            score.append({"team_lead": g["team_lead"], "colleagues": g["colleagues"], "measured": g["measured"], "compliant": g["compliant"],
                          "compliance_pct": round(100 * g["compliant"] / g["measured"]) if g["measured"] else None,
                          "exception_days": g["exception_days"], "late_days": g["late"], "break_days": g["brk"], "hours_days": g["hrs"],
                          "avg_clocked": round(g["clk"] / g["done"], 1) if g["done"] else 0, "avg_production": round(g["prod"] / g["done"], 1) if g["done"] else 0})
        score.sort(key=lambda x: (-(x["compliance_pct"] if x["compliance_pct"] is not None else -1), x["team_lead"]))
    return {"period": period, "label": label, "from": d1.isoformat(), "to": d2.isoformat(), "min_days": min_days, "offenders": offenders, "scorecard": score, "shifts": shifts,
            "reasons": {"total": rtotal, "people": reasons, "teams": rteams}}


LEAVE_KINDS = ("Leave", "Sick", "Training", "Other")


@app.post("/breaks/leave")
def mark_leave(
    payload: schemas.LeaveIn,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    """Mark a day (or a range, up to 31 days) as Leave / Sick / Training / Other for a colleague. Re-marking a day just changes its kind."""
    u = _may_adjust(db, current_user, payload.user_id)
    if payload.kind not in LEAVE_KINDS:
        raise HTTPException(status_code=400, detail="Kind must be one of: " + ", ".join(LEAVE_KINDS))
    try:
        d1 = date.fromisoformat(payload.date_from)
        d2 = date.fromisoformat(payload.date_to) if payload.date_to else d1
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date")
    if d2 < d1 or (d2 - d1).days > 30:
        raise HTTPException(status_code=400, detail="Choose a range of 31 days or less")
    today = datetime.now(IST).date()
    if d1 < today - timedelta(days=100) or d2 > today + timedelta(days=100):
        raise HTTPException(status_code=400, detail="Dates must be within 100 days of today")
    n = 0
    d = d1
    while d <= d2:
        row = db.query(models.LeaveDay).filter(models.LeaveDay.user_id == u.id, models.LeaveDay.leave_date == d).first()
        note = (payload.note or "").strip()[:200] or None
        if row:
            row.kind, row.note, row.created_by_name, row.created_at = payload.kind, note, current_user.full_name, datetime.utcnow()
        else:
            db.add(models.LeaveDay(user_id=u.id, leave_date=d, kind=payload.kind, note=note, created_by_name=current_user.full_name, created_at=datetime.utcnow()))
        n += 1
        d += timedelta(days=1)
    _audit(db, current_user, "Leave", "Marked leave", u, d1 if d1 == d2 else f"{d1} to {d2}", f"{payload.kind}, {n} day(s)" + (f" | {(payload.note or '').strip()[:200]}" if (payload.note or "").strip() else ""))
    db.commit()
    return {"days": n}


@app.get("/breaks/leave")
def list_leave(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    current_user: models.User = Depends(auth.require_role("colleague", "team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    today = datetime.now(IST).date()
    try:
        d1 = date.fromisoformat(date_from) if date_from else today.replace(day=1)
        d2 = date.fromisoformat(date_to) if date_to else today
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date")
    q = db.query(models.LeaveDay).filter(models.LeaveDay.leave_date >= d1, models.LeaveDay.leave_date <= d2)
    ids = _scope_ids(db, current_user)
    if ids is not None:
        q = q.filter(models.LeaveDay.user_id.in_(ids or [-1]))
    return [{"id": x.id, "user_id": x.user_id, "employee": x.user.full_name if x.user else "", "date": x.leave_date.isoformat(), "kind": x.kind,
             "note": x.note or "", "by": x.created_by_name or ""} for x in q.order_by(models.LeaveDay.leave_date, models.LeaveDay.id).all()]


@app.delete("/breaks/leave/{leave_id}")
def delete_leave(
    leave_id: int,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    x = db.query(models.LeaveDay).filter(models.LeaveDay.id == leave_id).first()
    if not x:
        raise HTTPException(status_code=404, detail="Not found")
    tu = _may_adjust(db, current_user, x.user_id)
    _audit(db, current_user, "Leave", "Removed leave", tu, x.leave_date, x.kind)
    db.delete(x)
    db.commit()
    return {"ok": True}


# ---------------- Approved break requests ----------------
@app.post("/breaks/requests")
def create_break_request(
    payload: schemas.BreakRequestIn,
    current_user: models.User = Depends(auth.require_role("colleague")),
    db: Session = Depends(get_db),
):
    today = datetime.now(IST).date()
    try:
        d = date.fromisoformat(payload.req_date)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date")
    if d < today or d > today + timedelta(days=30):
        raise HTTPException(status_code=400, detail="Choose today or a day within the next 30 days")
    if payload.extra_minutes < 5 or payload.extra_minutes > 480:
        raise HTTPException(status_code=400, detail="Extra time must be between 5 and 480 minutes")
    reason = (payload.reason or "").strip()
    if len(reason) < 3:
        raise HTTPException(status_code=400, detail="Please give a reason")
    if db.query(models.BreakRequest).filter(models.BreakRequest.user_id == current_user.id, models.BreakRequest.req_date == d,
                                            models.BreakRequest.status.in_(["Pending", "Approved"])).first():
        raise HTTPException(status_code=400, detail="You already have a pending or approved request for that day")
    db.add(models.BreakRequest(user_id=current_user.id, req_date=d, extra_minutes=payload.extra_minutes, reason=reason[:300], status="Pending", created_at=datetime.utcnow()))
    db.commit()
    return {"ok": True}


def _req_out(x):
    return {"id": x.id, "user_id": x.user_id, "employee": x.user.full_name if x.user else "", "team_lead": (x.user.reporting_manager if x.user else "") or "",
            "date": x.req_date.isoformat(), "extra_minutes": x.extra_minutes, "reason": x.reason, "status": x.status, "decided_by": x.decided_by_name or "",
            "note": x.decision_note or "", "created_at": x.created_at.isoformat() + "Z" if x.created_at else ""}


@app.get("/breaks/requests")
def list_break_requests(
    status: str = "all",
    current_user: models.User = Depends(auth.require_role("colleague", "team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    """Colleague: own requests. Team Lead: own team. Admin / Manager / Super Admin: everyone. status = pending | all."""
    today = datetime.now(IST).date()
    q = db.query(models.BreakRequest)
    if current_user.role == "colleague":
        q = q.filter(models.BreakRequest.user_id == current_user.id)
    else:
        ids = [u.id for u in _batch_visible_colleagues(db, current_user)]
        q = q.filter(models.BreakRequest.user_id.in_(ids or [-1]))
    if status == "pending":
        q = q.filter(models.BreakRequest.status == "Pending")
    else:
        q = q.filter(models.BreakRequest.req_date >= today - timedelta(days=45))
    return [_req_out(x) for x in q.order_by(models.BreakRequest.req_date.desc(), models.BreakRequest.id.desc()).limit(300).all()]


@app.post("/breaks/requests/{req_id}/decide")
def decide_break_request(
    req_id: int,
    payload: schemas.BreakRequestDecision,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    x = db.query(models.BreakRequest).filter(models.BreakRequest.id == req_id).first()
    if not x:
        raise HTTPException(status_code=404, detail="Request not found")
    _may_adjust(db, current_user, x.user_id)
    if x.status != "Pending":
        raise HTTPException(status_code=400, detail="This request was already " + x.status.lower())
    note = (payload.note or "").strip()
    if not payload.approve and not note:
        raise HTTPException(status_code=400, detail="Please give a reason for rejecting")
    x.status = "Approved" if payload.approve else "Rejected"
    x.decided_by_name, x.decision_note, x.decided_at = current_user.full_name, note[:300] or None, datetime.utcnow()
    _audit(db, current_user, "Break request", "Approved break request" if payload.approve else "Rejected break request", x.user, x.req_date,
           f"+{x.extra_minutes} min | Asked: {x.reason}" + (f" | Note: {note[:200]}" if note else ""))
    db.commit()
    return _req_out(x)


@app.delete("/breaks/requests/{req_id}")
def cancel_break_request(
    req_id: int,
    current_user: models.User = Depends(auth.require_role("colleague")),
    db: Session = Depends(get_db),
):
    x = db.query(models.BreakRequest).filter(models.BreakRequest.id == req_id, models.BreakRequest.user_id == current_user.id).first()
    if not x:
        raise HTTPException(status_code=404, detail="Request not found")
    if x.status != "Pending":
        raise HTTPException(status_code=400, detail="Only a pending request can be cancelled")
    x.status = "Cancelled"
    db.commit()
    return {"ok": True}


# ---------------- Weekly aggregates: trend lines + weekly email ----------------
def _week_aggregate(db: Session, users: list, d1: date, d2: date):
    """Per week (Monday start): person-days, exception days, late / break / short-hours days, average break, clocked and production time. Also per-person counts."""
    today = datetime.now(IST).date()
    by = {u.id: u for u in users}
    days = _collect_days(db, list(by), d1, d2)
    hol = _hol_set(db, d1, d2)
    weeks, per = {}, {}
    for (uid, d), v in days.items():
        u = by.get(uid)
        if not u or v["first_login"] is None:
            continue
        ws = d - timedelta(days=d.weekday())
        w = weeks.setdefault(ws, {"days": 0, "exc": 0, "late": 0, "brk": 0, "short": 0, "done": 0, "clk": 0.0, "prod": 0.0, "bmin": 0.0})
        off = _is_off_day(d, hol)
        flags, late = _day_flags(v, u.shift, d, off, today)
        p = per.setdefault((ws, uid), {"name": u.full_name, "team_lead": u.reporting_manager or "", "exc": 0, "late": 0, "brk": 0, "hrs": 0})
        w["days"] += 1
        if "Late login" in flags:
            w["late"] += 1; p["late"] += 1
        if any(x in flags for x in ("Tea + Lunch over 1h", "Other break over 30m", "Single break over 2h")):
            w["brk"] += 1; p["brk"] += 1
        if any(x in flags for x in ("Clocked under 9h", "Production under 7:30h", "Left early")):
            w["short"] += 1; p["hrs"] += 1
        if flags:
            w["exc"] += 1; p["exc"] += 1
        if not off and not v.get("leave") and not (d >= today and v["live"]):
            w["done"] += 1; w["clk"] += v["clocked"]; w["prod"] += v["production"]; w["bmin"] += v["brk_total"]
    return weeks, per


def _week_row(ws: date, w: dict, today: date) -> dict:
    w = w or {"days": 0, "exc": 0, "late": 0, "brk": 0, "short": 0, "done": 0, "clk": 0.0, "prod": 0.0, "bmin": 0.0}
    return {"week_start": ws.isoformat(), "label": ws.strftime("%d %b"), "partial": ws + timedelta(days=6) >= today, "person_days": w["days"], "exception_days": w["exc"],
            "compliance_pct": round(100 * (w["days"] - w["exc"]) / w["days"]) if w["days"] else None, "late_days": w["late"], "break_days": w["brk"], "short_days": w["short"],
            "avg_break": round(w["bmin"] / w["done"], 1) if w["done"] else None, "avg_clocked": round(w["clk"] / w["done"], 1) if w["done"] else None,
            "avg_production": round(w["prod"] / w["done"], 1) if w["done"] else None}


@app.get("/breaks/trends")
def break_trends(
    weeks: int = 8,
    user_id: Optional[int] = None,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    """Week-by-week trend (Monday weeks, newest last) for everyone the caller can see, or for one colleague."""
    weeks = max(4, min(weeks, 26))
    today = datetime.now(IST).date()
    users = [u for u in _batch_visible_colleagues(db, current_user) if u.employment_status != "Inactive"]
    people = sorted(({"id": u.id, "name": u.full_name} for u in users), key=lambda x: x["name"])
    if user_id is not None:
        users = [u for u in users if u.id == user_id]
        if not users:
            raise HTTPException(status_code=404, detail="Colleague not found")
    cur = today - timedelta(days=today.weekday())
    start = cur - timedelta(days=7 * (weeks - 1))
    agg, _per = _week_aggregate(db, users, start, today)
    rows = [_week_row(start + timedelta(days=7 * i), agg.get(start + timedelta(days=7 * i)), today) for i in range(weeks)]
    return {"weeks": rows, "people": people, "scope": users[0].full_name if user_id is not None else "All colleagues you can see", "colleagues": len(users)}


WEEKLY_KEYS = {"enabled": "weekly_enabled", "time": "weekly_time", "last": "weekly_last_sent"}


def _weekly_email(title: str, ws: date, cur: dict, prev: dict, offenders: list, tl_rows: list):
    import html as _h
    we = ws + timedelta(days=6)
    rng = f"{ws.strftime('%d %b')} - {we.strftime('%d %b %Y')}"
    def delta(a, b, good_up):
        if a is None or b is None:
            return ""
        d_ = a - b
        if d_ == 0:
            return " (same)"
        return f" ({'+' if d_ > 0 else ''}{d_:g} vs last week)"
    items = [("Compliance (days without an exception)", f"{cur['compliance_pct']}%" if cur["compliance_pct"] is not None else "-", delta(cur["compliance_pct"], prev["compliance_pct"], True)),
             ("Exception days", str(cur["exception_days"]) + f" of {cur['person_days']}", delta(cur["exception_days"], prev["exception_days"], False)),
             ("Late logins", str(cur["late_days"]), delta(cur["late_days"], prev["late_days"], False)),
             ("Break-limit days", str(cur["break_days"]), delta(cur["break_days"], prev["break_days"], False)),
             ("Short-hours / left-early days", str(cur["short_days"]), delta(cur["short_days"], prev["short_days"], False)),
             ("Average break per day", _fmt_m(int(cur["avg_break"] or 0)), ""),
             ("Average clocked per day", _fmt_m(int(cur["avg_clocked"] or 0)), ""),
             ("Average production per day", _fmt_m(int(cur["avg_production"] or 0)), "")]
    text = f"{title}\nWeek: {rng}\n\n" + "\n".join(f"{a}: {b}{c}" for a, b, c in items)
    if offenders:
        text += "\n\nMost exception days:\n" + "\n".join(f"- {o['name']}: {o['exc']} (late {o['late']}, breaks {o['brk']}, hours {o['hrs']})" for o in offenders)
    if tl_rows:
        text += "\n\nBy Team Lead:\n" + "\n".join(f"- {t['team_lead']}: {t['compliance']}% ({t['exc']} exception days of {t['days']})" for t in tl_rows)
    td = 'style="padding:5px 8px;border-bottom:1px solid #e3e7ef;font-size:12.5px;"'
    th = lambda x: f'<th style="background:#0b2545;color:#fff;padding:6px 8px;text-align:left;font-size:12px;">{_h.escape(x)}</th>'
    html = f'<div style="font-family:Segoe UI,Arial,sans-serif;color:#222;"><h3 style="color:#0b2545;margin:0 0 4px;">{_h.escape(title)}</h3><div style="color:#666;font-size:13px;margin-bottom:10px;">Week {_h.escape(rng)}</div>'
    html += "<table style='border-collapse:collapse;'>" + "".join(f"<tr><td {td}>{_h.escape(a)}</td><td {td}><b>{_h.escape(b)}</b>{_h.escape(c)}</td></tr>" for a, b, c in items) + "</table>"
    if offenders:
        html += '<h4 style="color:#0b2545;margin:14px 0 4px;">Most exception days</h4><table style="border-collapse:collapse;"><tr>' + "".join(th(x) for x in ("Employee", "Exception days", "Late", "Breaks", "Hours")) + "</tr>" + \
                "".join(f"<tr><td {td}>{_h.escape(o['name'])}</td><td {td}>{o['exc']}</td><td {td}>{o['late']}</td><td {td}>{o['brk']}</td><td {td}>{o['hrs']}</td></tr>" for o in offenders) + "</table>"
    if tl_rows:
        html += '<h4 style="color:#0b2545;margin:14px 0 4px;">By Team Lead</h4><table style="border-collapse:collapse;"><tr>' + "".join(th(x) for x in ("Team Lead", "Compliance", "Exception days", "Person-days")) + "</tr>" + \
                "".join(f"<tr><td {td}>{_h.escape(t['team_lead'])}</td><td {td}>{t['compliance']}%</td><td {td}>{t['exc']}</td><td {td}>{t['days']}</td></tr>" for t in tl_rows) + "</table>"
    html += '<p style="color:#888;font-size:11.5px;margin-top:10px;">Weeks run Monday to Sunday. Weekends, holidays, leave and days excused by a Team Lead are not counted against anyone. Sent automatically by WorkFlow Hub.</p></div>'
    return text, html


def run_weekly_summary(db: Session, week_start: date = None, only_to: str = None) -> dict:
    today = datetime.now(IST).date()
    ws = week_start or (today - timedelta(days=today.weekday()) - timedelta(days=7))
    pws = ws - timedelta(days=7)
    colleagues = db.query(models.User).filter(models.User.role == "colleague", models.User.employment_status != "Inactive").all()
    res = {"week": ws.isoformat(), "sent": 0, "failed": [], "skipped_no_email": [], "note": ""}
    def build(users, with_tl):
        agg, per = _week_aggregate(db, users, pws, ws + timedelta(days=6))
        cur, prev = _week_row(ws, agg.get(ws), today), _week_row(pws, agg.get(pws), today)
        offs = sorted((p for (w_, _u), p in per.items() if w_ == ws and p["exc"] > 0), key=lambda p: (-p["exc"], p["name"]))[:5]
        tls = []
        if with_tl:
            by_tl = {}
            for u in users:
                by_tl.setdefault(u.reporting_manager or "(No Team Lead)", []).append(u)
            for tl_name, us in sorted(by_tl.items()):
                a2, _p2 = _week_aggregate(db, us, ws, ws + timedelta(days=6))
                r2 = _week_row(ws, a2.get(ws), today)
                if r2["person_days"]:
                    tls.append({"team_lead": tl_name, "compliance": r2["compliance_pct"], "exc": r2["exception_days"], "days": r2["person_days"]})
        return cur, prev, offs, tls
    def send(to, title, users, with_tl):
        cur, prev, offs, tls = build(users, with_tl)
        if not cur["person_days"]:
            return False
        text, html = _weekly_email(title, ws, cur, prev, offs, tls)
        if email_utils.send_email(to, f"{title} - week of {ws.strftime('%d %b %Y')}", text, html):
            res["sent"] += 1
        else:
            res["failed"].append(f"{to}: {email_utils.last_error}")
        return True
    if only_to:
        if not send(only_to, "Weekly break and attendance summary - all teams (test)", colleagues, True):
            res["note"] = "No attendance data for last week, so no test mail was sent."
        return res
    for tl in db.query(models.User).filter(models.User.role == "team_lead", models.User.employment_status != "Inactive").all():
        team = [u for u in colleagues if u.reporting_manager == tl.full_name]
        if not team:
            continue
        if not tl.email:
            res["skipped_no_email"].append(tl.full_name); continue
        send(tl.email, "Weekly break and attendance summary - your team", team, False)
    if _setting(db, DIGEST_KEYS["admins"], "1") == "1":
        for a in db.query(models.User).filter(models.User.role.in_(["admin", "manager", "super_admin"]), models.User.employment_status != "Inactive").all():
            if a.email:
                send(a.email, "Weekly break and attendance summary - all teams", colleagues, True)
    return res


@app.post("/breaks/shift-change")
def mark_shift_change(
    payload: schemas.ShiftChangeIn,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    """Give a colleague a temporary shift for a date range; late-login and left-early checks use it on those days."""
    u = _may_adjust(db, current_user, payload.user_id)
    shift = (payload.shift or "").strip()
    if _shift_start_minutes(shift) is None or len(shift) > 80:
        raise HTTPException(status_code=400, detail="Pick a shift such as: Day Shift (09:00 AM - 06:00 PM)")
    try:
        d1 = date.fromisoformat(payload.date_from)
        d2 = date.fromisoformat(payload.date_to) if payload.date_to else d1
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date")
    today = datetime.now(IST).date()
    if d2 < d1 or (d2 - d1).days > 92:
        raise HTTPException(status_code=400, detail="Choose a range of 93 days or less")
    if d1 < today - timedelta(days=100) or d2 > today + timedelta(days=200):
        raise HTTPException(status_code=400, detail="Dates are too far from today")
    # a new range replaces anything it overlaps for this colleague
    for old in db.query(models.ShiftChange).filter(models.ShiftChange.user_id == u.id, models.ShiftChange.date_from <= d2, models.ShiftChange.date_to >= d1).all():
        db.delete(old)
    db.add(models.ShiftChange(user_id=u.id, date_from=d1, date_to=d2, shift=shift, note=(payload.note or "").strip()[:200] or None,
                              created_by_name=current_user.full_name, created_at=datetime.utcnow()))
    _audit(db, current_user, "Shift change", "Set temporary shift", u, d1 if d1 == d2 else f"{d1} to {d2}", f"{shift} (profile shift: {u.shift or 'not set'})" + (f" | {(payload.note or '').strip()[:200]}" if (payload.note or "").strip() else ""))
    db.commit()
    return {"ok": True}


@app.get("/breaks/shift-change")
def list_shift_changes(
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    today = datetime.now(IST).date()
    ids = [u.id for u in _batch_visible_colleagues(db, current_user)]
    rows = db.query(models.ShiftChange).filter(models.ShiftChange.user_id.in_(ids or [-1]), models.ShiftChange.date_to >= today - timedelta(days=31)).order_by(models.ShiftChange.date_from.desc()).all()
    return [{"id": x.id, "user_id": x.user_id, "employee": x.user.full_name if x.user else "", "date_from": x.date_from.isoformat(), "date_to": x.date_to.isoformat(),
             "shift": x.shift, "note": x.note or "", "by": x.created_by_name or ""} for x in rows]


@app.delete("/breaks/shift-change/{sc_id}")
def delete_shift_change(
    sc_id: int,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    x = db.query(models.ShiftChange).filter(models.ShiftChange.id == sc_id).first()
    if not x:
        raise HTTPException(status_code=404, detail="Shift change not found")
    tu = _may_adjust(db, current_user, x.user_id)
    _audit(db, current_user, "Shift change", "Removed temporary shift", tu, f"{x.date_from} to {x.date_to}", x.shift)
    db.delete(x)
    db.commit()
    return {"ok": True}


@app.get("/breaks/my-notices")
def my_break_notices(
    current_user: models.User = Depends(auth.require_role("colleague")),
    db: Session = Depends(get_db),
):
    """What a Team Lead / Admin changed on this colleague's days (last 60 days) and the colleague's own queries."""
    today = datetime.now(IST).date()
    since = today - timedelta(days=60)
    items = []
    for a in db.query(models.BreakAdjustment).filter(models.BreakAdjustment.user_id == current_user.id, models.BreakAdjustment.adj_date >= since).all():
        bits = []
        if a.excused:
            bits.append("Excused: " + ", ".join({"late": "late login", "break": "break limits", "hours": "hour targets"}.get(x, x) for x in a.excused.split(",") if x))
        if a.new_type:
            bits.append("Break re-typed " + (a.old_type or "") + " -> " + a.new_type)
        items.append({"kind": "Adjustment", "date": a.adj_date.isoformat(), "text": "; ".join(bits) + (" - " + a.note if a.note else ""), "by": a.created_by_name or ""})
    for x in db.query(models.LeaveDay).filter(models.LeaveDay.user_id == current_user.id, models.LeaveDay.leave_date >= since).all():
        items.append({"kind": "Leave", "date": x.leave_date.isoformat(), "text": x.kind + (" - " + x.note if x.note else ""), "by": x.created_by_name or ""})
    for x in db.query(models.ShiftChange).filter(models.ShiftChange.user_id == current_user.id, models.ShiftChange.date_to >= since).all():
        items.append({"kind": "Shift change", "date": x.date_from.isoformat(), "text": x.shift + (" until " + x.date_to.isoformat() if x.date_to != x.date_from else "") + (" - " + x.note if x.note else ""), "by": x.created_by_name or ""})
    items.sort(key=lambda i: i["date"], reverse=True)
    qs = [{"id": q.id, "kind": q.kind or "", "ref_date": q.ref_date.isoformat() if q.ref_date else "", "ref_text": q.ref_text or "", "message": q.message, "status": q.status,
           "reply": q.reply or "", "replied_by": q.replied_by or "", "created_at": q.created_at.isoformat() + "Z" if q.created_at else ""}
          for q in db.query(models.BreakQuery).filter(models.BreakQuery.user_id == current_user.id).order_by(models.BreakQuery.id.desc()).limit(30).all()]
    return {"items": items[:60], "queries": qs}


@app.post("/breaks/queries")
def raise_break_query(
    payload: schemas.QueryIn,
    current_user: models.User = Depends(auth.require_role("colleague")),
    db: Session = Depends(get_db),
):
    msg = (payload.message or "").strip()
    if len(msg) < 3:
        raise HTTPException(status_code=400, detail="Please type your question")
    try:
        rd = date.fromisoformat(payload.ref_date) if payload.ref_date else None
    except ValueError:
        rd = None
    db.add(models.BreakQuery(user_id=current_user.id, kind=(payload.kind or "")[:30] or None, ref_date=rd, ref_text=(payload.ref_text or "")[:300] or None,
                             message=msg[:500], status="Open", created_at=datetime.utcnow()))
    db.commit()
    return {"ok": True}


@app.get("/breaks/queries")
def list_break_queries(
    status: str = "open",
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    ids = [u.id for u in _batch_visible_colleagues(db, current_user)]
    q = db.query(models.BreakQuery).filter(models.BreakQuery.user_id.in_(ids or [-1]))
    if status == "open":
        q = q.filter(models.BreakQuery.status == "Open")
    return [{"id": x.id, "employee": x.user.full_name if x.user else "", "team_lead": (x.user.reporting_manager if x.user else "") or "", "kind": x.kind or "",
             "ref_date": x.ref_date.isoformat() if x.ref_date else "", "ref_text": x.ref_text or "", "message": x.message, "status": x.status, "reply": x.reply or "",
             "replied_by": x.replied_by or "", "created_at": x.created_at.isoformat() + "Z" if x.created_at else ""} for x in q.order_by(models.BreakQuery.id.desc()).limit(200).all()]


@app.post("/breaks/queries/{query_id}/reply")
def reply_break_query(
    query_id: int,
    payload: schemas.QueryReplyIn,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    x = db.query(models.BreakQuery).filter(models.BreakQuery.id == query_id).first()
    if not x:
        raise HTTPException(status_code=404, detail="Query not found")
    _may_adjust(db, current_user, x.user_id)
    r = (payload.reply or "").strip()
    if not r:
        raise HTTPException(status_code=400, detail="Please type a reply")
    x.reply, x.replied_by, x.status, x.replied_at = r[:500], current_user.full_name, "Resolved", datetime.utcnow()
    _audit(db, current_user, "Query", "Replied to colleague query", x.user, x.ref_date, f"Q: {x.message[:150]} | A: {r[:250]}")
    db.commit()
    return {"ok": True}


# ======================================================================
# Order aging / SLA, escalation ageing, management summary, database backup
# ======================================================================
SLA_KEYS = {"warn": "sla_warn_days", "late": "sla_late_days"}


def _sla_days(db: Session):
    def num(k, d):
        try:
            return max(1, int(_setting(db, k, str(d))))
        except ValueError:
            return d
    return num(SLA_KEYS["warn"], 2), num(SLA_KEYS["late"], 3)


@app.get("/settings/sla")
def get_sla_settings(current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")), db: Session = Depends(get_db)):
    w, l = _sla_days(db)
    return {"warn_days": w, "late_days": l}


@app.put("/settings/sla")
def set_sla_settings(payload: dict, current_user: models.User = Depends(auth.require_role("super_admin")), db: Session = Depends(get_db)):
    try:
        w, l = int(payload.get("warn_days")), int(payload.get("late_days"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Enter whole numbers of days")
    if w < 1 or l < w or l > 60:
        raise HTTPException(status_code=400, detail="'At risk' must be at least 1 day and 'Late' must be the same or more (max 60)")
    _audit(db, current_user, "Setting", "Changed order aging limits", details=f"At risk from {w} business days, late from {l}")
    _set_setting(db, SLA_KEYS["warn"], str(w))
    _set_setting(db, SLA_KEYS["late"], str(l))
    db.commit()
    return {"warn_days": w, "late_days": l}


def _order_age(received: date, today: date, cache: dict) -> int:
    if received not in cache:
        cache[received] = _count_business_days_ex_holidays(received, today)
    return cache[received]


def _team_names(db: Session):
    tls, members, _orph = _tl_membership(db)
    colleague_tl = {cid: tid for tid, ids in members.items() for cid in ids}
    return tls, colleague_tl


_WAITING = ("with_onshore", "blue", "orange")


class _ExtraProxy:
    """An extra reason of a multi-reason escalation, looking like an order whose hand-off state is that reason's own."""
    def __init__(self, order, ex: dict):
        self._o, self._ex = order, ex

    def __getattr__(self, n):
        if n == "track" or n == "escalation_category":
            return self._ex.get("category")
        if n in ("onshore_status", "onshore_team", "onshore_comment", "onshore_tl_reply"):
            return self._ex.get(n)
        if n == "onshore_sent_at":
            v = self._ex.get(n)
            try:
                return datetime.fromisoformat(v) if isinstance(v, str) else v
            except ValueError:
                return None
        if n == "escalated":
            return True
        return getattr(self._o, n)


def _order_tracks(o) -> list:
    """The order itself (unless its main reason is already resolved) plus one proxy per extra open reason."""
    det = getattr(o, "escalation_detail", None)
    dd = (det.data or {}) if det else {}
    out = [] if dd.get("_pdone") else [o]
    out += [_ExtraProxy(o, ex) for ex in (dd.get("_queue") or [])]
    return out


def _holders(o) -> list:
    return list(dict.fromkeys(_holder(t) for t in _order_tracks(o))) or ["Team Lead"]


def _holder_all(o) -> str:
    hs = _holders(o)
    return hs[0] if len(hs) == 1 else " + ".join(hs)


def _has_extra_state(db: Session):
    """Orders that have an extra reason with its own hand-off state."""
    return db.query(models.EscalationDetail.order_id).filter(cast(models.EscalationDetail.data, String).like('%"onshore_status"%'))


def _holder(o) -> str:
    """Who has an escalated order right now."""
    if o.onshore_status in ("with_onshore", "blue", "orange"):
        return {"onshore": "Onshore", "recon": "Recon", "calling": "Calling"}.get(o.onshore_team or "onshore", "Onshore")
    return "Team Lead"


@app.get("/orders/aging")
def orders_aging(
    process_id: Optional[int] = None,
    team_lead_id: Optional[int] = None,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    """Open orders (not Completed, not yet in Production) by age in business days since the Received Date, with an at-risk list."""
    W = models.WorkOrder
    today = datetime.now(IST).date()
    warn, late = _sla_days(db)
    scoped = _orders_dash_scope(db, current_user, process_id, team_lead_id)
    rows = scoped(db.query(W).options(selectinload(W.assignee), selectinload(W.process)).filter(
        W.received_date.isnot(None), W.submitted.is_(False), func.coalesce(W.posting_status, "") != "Completed")).all()
    tls, colleague_tl = _team_names(db)
    tl_ids = set(tls)
    cache = {}
    buckets = [("0-1 days", 0, 1), ("2 days", 2, 2), ("3 days", 3, 3), ("4-5 days", 4, 5), ("6+ days", 6, 10 ** 6)]
    bk = [0] * len(buckets)
    by_proc, by_tl, items = {}, {}, []
    for o in rows:
        age = _order_age(o.received_date, today, cache)
        for i, (_n, lo_, hi_) in enumerate(buckets):
            if lo_ <= age <= hi_:
                bk[i] += 1
                break
        st = o.posting_status or "Pending"
        pn = o.process.name if o.process else "(none)"
        tlid = _order_team_lead_id(o.assigned_to_id, o.team_lead_id, colleague_tl, tl_ids)
        tln = tls.get(tlid, "No Team Lead")
        for d, key in ((by_proc, pn), (by_tl, tln)):
            g = d.setdefault(key, {"name": key, "open": 0, "at_risk": 0, "late": 0, "oldest": 0})
            g["open"] += 1
            g["oldest"] = max(g["oldest"], age)
            if age >= late:
                g["late"] += 1
            elif age >= warn:
                g["at_risk"] += 1
        if age >= warn:
            items.append({"id": o.id, "process": pn, "edm": o.edm or "", "received": o.received_date.isoformat(), "age": age, "status": st,
                          "assignee": o.assignee.full_name if o.assignee else "(unassigned)", "team_lead": tln,
                          "escalated": bool(o.escalated or o.onshore_status), "holder": _holder_all(o) if (o.escalated or o.onshore_status) else "",
                          "level": "late" if age >= late else "risk", "amount": o.amount})
    items.sort(key=lambda x: (-x["age"], x["process"], x["edm"]))
    return {"as_of": today.isoformat(), "warn_days": warn, "late_days": late, "open": len(rows),
            "at_risk": sum(1 for x in items if x["level"] == "risk"), "late": sum(1 for x in items if x["level"] == "late"),
            "buckets": [{"label": b[0], "count": bk[i]} for i, b in enumerate(buckets)],
            "by_process": sorted(by_proc.values(), key=lambda g: (-g["late"], -g["at_risk"], g["name"])),
            "by_team_lead": sorted(by_tl.values(), key=lambda g: (-g["late"], -g["at_risk"], g["name"])) if current_user.role != "team_lead" else [],
            "orders": items[:500], "truncated": len(items) > 500}


@app.get("/orders/escalation-aging")
def escalation_aging(
    process_id: Optional[int] = None,
    period: str = "month",
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    """Escalations open right now (who holds them, how long) and turnaround for the period: raised, closed, days to close by category, team response time."""
    if period not in ("week", "month", "last_month"):
        raise HTTPException(status_code=400, detail="period must be week, month or last_month")
    W, E = models.WorkOrder, models.EscalationEvent
    d1, d2, label = _period_range(period)
    today = datetime.now(IST).date()
    now_ist = datetime.now(IST).replace(tzinfo=None)
    scoped = _orders_dash_scope(db, current_user, process_id)
    open_orders = scoped(db.query(W).options(selectinload(W.assignee), selectinload(W.process)).filter(
        or_(W.escalated.is_(True), W.onshore_status.in_(("with_onshore", "red", "yellow", "blue", "orange"))))).all()
    ids = [o.id for o in open_orders]
    last_msg = {}
    if ids:
        for m in db.query(models.OnshoreMessage).filter(models.OnshoreMessage.order_id.in_(ids)).order_by(models.OnshoreMessage.id).all():
            last_msg[(m.order_id, m.track or None)] = m.created_at
    tls, colleague_tl = _team_names(db)
    tl_ids, cache, open_rows = set(tls), {}, []
    for o in [t for oo in open_orders for t in (_order_tracks(oo) if oo.escalated else [oo])]:
        since_dt = last_msg.get((o.id, getattr(o, "track", None))) or o.onshore_sent_at
        since_hours = round((now_ist - since_dt).total_seconds() / 3600, 1) if since_dt else None
        raised = o.issue_raised_date
        tlid = _order_team_lead_id(o.assigned_to_id, o.team_lead_id, colleague_tl, tl_ids)
        open_rows.append({"id": o.id, "process": o.process.name if o.process else "", "edm": o.edm or "", "category": o.escalation_category or "", "raised": raised.isoformat() if raised else "",
                          "age": _order_age(raised, today, cache) if raised else None, "holder": _holder(o), "waiting_hours": since_hours,
                          "colleague": o.assignee.full_name if o.assignee else "", "team_lead": tls.get(tlid, "No Team Lead")})
    open_rows.sort(key=lambda r: (-(r["age"] or 0), -(r["waiting_hours"] or 0)))
    # turnaround for escalations raised in the period
    eq = db.query(E).filter(E.raised_date >= d1, E.raised_date <= d2)
    if process_id is not None:
        eq = eq.filter(E.process_id == process_id)
    if current_user.role == "team_lead":
        eq = eq.filter(E.team_lead_id == current_user.id)
    cats = {}
    for e, cat in [(e, c) for e in eq.all() for c in _event_cats(e)]:
        g = cats.setdefault(cat, {"category": cat, "raised": 0, "closed": 0, "days_total": 0, "open": 0})
        g["raised"] += 1
        if e.closed_date:
            g["closed"] += 1
            g["days_total"] += _count_business_days_ex_holidays(e.raised_date, e.closed_date)
        else:
            g["open"] += 1
    cat_rows = [{"category": g["category"], "raised": g["raised"], "closed": g["closed"], "open": g["open"],
                 "avg_days": round(g["days_total"] / g["closed"], 1) if g["closed"] else None} for g in cats.values()]
    cat_rows.sort(key=lambda g: (-g["raised"], g["category"]))
    # how fast each receiving team answers: from "sent" (or the Team Lead's extra info) to the team's reply
    scope_ids = {o_id for (o_id,) in scoped(db.query(W.id)).all()} if current_user.role == "team_lead" or process_id is not None else None
    t_lo, t_hi = datetime(d1.year, d1.month, d1.day), datetime(d2.year, d2.month, d2.day) + timedelta(days=1)
    mq = db.query(models.OnshoreMessage).filter(models.OnshoreMessage.created_at >= t_lo - timedelta(days=30), models.OnshoreMessage.created_at < t_hi).order_by(models.OnshoreMessage.order_id, models.OnshoreMessage.id)
    pend, resp = {}, {}
    for m in mq.all():
        if scope_ids is not None and m.order_id not in scope_ids:
            continue
        if m.kind in ("sent", "tl_info"):
            pend[m.order_id] = m.created_at
        elif m.kind in ("onshore_red", "onshore_yellow") and m.order_id in pend:
            if t_lo <= m.created_at < t_hi:
                team = (m.author_role or "onshore").lower()
                resp.setdefault(team, []).append((m.created_at - pend[m.order_id]).total_seconds() / 3600)
            del pend[m.order_id]
    teams = [{"team": {"onshore": "Onshore", "recon": "Recon", "calling": "Calling"}.get(t, t.title()), "answers": len(v), "avg_hours": round(sum(v) / len(v), 1), "longest_hours": round(max(v), 1)} for t, v in sorted(resp.items())]
    holders = {}
    for r in open_rows:
        holders[r["holder"]] = holders.get(r["holder"], 0) + 1
    return {"label": label, "from": d1.isoformat(), "to": d2.isoformat(), "open": open_rows, "holders": holders, "categories": cat_rows, "teams": teams}


def _scope_label(user):
    return "your team" if user.role == "team_lead" else "all teams"


def _mgmt_summary(db: Session, user: models.User, d1: date, d2: date, process_id: Optional[int] = None) -> dict:
    """Production summary for a date range, limited to what this user may see (Team Lead: own team; Admin / Manager / Super Admin: everything)."""
    W, E = models.WorkOrder, models.EscalationEvent
    today = datetime.now(IST).date()
    warn, late = _sla_days(db)
    scoped = _orders_dash_scope(db, user, process_id)
    procs = {p.id: p.name for p in db.query(models.Process).all()}
    rows = {}
    def row(pid):
        return rows.setdefault(pid, {"process": procs.get(pid, "(none)"), "received": 0, "completed": 0, "trans": 0, "tat_total": 0, "tat_n": 0, "to_production": 0,
                                     "raised": 0, "closed": 0, "open": 0, "at_risk": 0, "late": 0})
    for pid, n in scoped(db.query(W.process_id, func.count(W.id)).filter(W.received_date >= d1, W.received_date <= d2)).group_by(W.process_id).all():
        row(pid)["received"] += n
    for pid, n, tr, tat_sum, tat_n in scoped(db.query(W.process_id, func.count(W.id), func.coalesce(func.sum(W.trans_count), 0), func.coalesce(func.sum(W.tat_days), 0), func.count(W.tat_days)).filter(
            W.posting_status == "Completed", W.posted_date >= d1, W.posted_date <= d2)).group_by(W.process_id).all():
        r = row(pid); r["completed"] += n; r["trans"] += int(tr or 0); r["tat_total"] += int(tat_sum or 0); r["tat_n"] += tat_n
    lo, hi = datetime(d1.year, d1.month, d1.day), datetime(d2.year, d2.month, d2.day) + timedelta(days=1)
    for pid, n in scoped(db.query(W.process_id, func.count(W.id)).filter(W.submitted_at >= lo, W.submitted_at < hi)).group_by(W.process_id).all():
        row(pid)["to_production"] += n
    eq = db.query(E.process_id, E.raised_date, E.closed_date, E.team_lead_id)
    if process_id is not None:
        eq = eq.filter(E.process_id == process_id)
    if user.role == "team_lead":
        eq = eq.filter(E.team_lead_id == user.id)
    for pid, rd, cd, _tl in eq.filter(or_(and_(E.raised_date >= d1, E.raised_date <= d2), and_(E.closed_date >= d1, E.closed_date <= d2))).all():
        if rd and d1 <= rd <= d2:
            row(pid)["raised"] += 1
        if cd and d1 <= cd <= d2:
            row(pid)["closed"] += 1
    cache = {}
    for pid, rd in scoped(db.query(W.process_id, W.received_date).filter(W.received_date.isnot(None), W.submitted.is_(False), func.coalesce(W.posting_status, "") != "Completed")).all():
        r = row(pid); r["open"] += 1
        age = _order_age(rd, today, cache)
        if age >= late:
            r["late"] += 1
        elif age >= warn:
            r["at_risk"] += 1
    top = scoped(db.query(W.employee_name, func.count(W.id), func.coalesce(func.sum(W.trans_count), 0)).filter(
        W.posting_status == "Completed", W.posted_date >= d1, W.posted_date <= d2, W.employee_name.isnot(None))).group_by(W.employee_name).order_by(func.count(W.id).desc()).limit(5).all()
    out_rows = []
    for r in sorted(rows.values(), key=lambda x: x["process"]):
        r["avg_tat"] = round(r["tat_total"] / r["tat_n"], 1) if r["tat_n"] else None
        out_rows.append(r)
    tot = {k: sum(r[k] for r in out_rows) for k in ("received", "completed", "trans", "to_production", "raised", "closed", "open", "at_risk", "late")}
    tat_n = sum(r["tat_n"] for r in out_rows)
    tot["avg_tat"] = round(sum(r["tat_total"] for r in out_rows) / tat_n, 1) if tat_n else None
    tl_rows = []
    if user.role != "team_lead":
        tls, colleague_tl = _team_names(db)
        tl_ids = set(tls)
        g = {}
        def grow(tid):
            return g.setdefault(tid, {"team_lead": tls.get(tid, "No Team Lead"), "received": 0, "completed": 0, "open": 0, "late": 0})
        for a, o, n in scoped(db.query(W.assigned_to_id, W.team_lead_id, func.count(W.id)).filter(W.received_date >= d1, W.received_date <= d2)).group_by(W.assigned_to_id, W.team_lead_id).all():
            grow(_order_team_lead_id(a, o, colleague_tl, tl_ids))["received"] += n
        for a, o, n in scoped(db.query(W.assigned_to_id, W.team_lead_id, func.count(W.id)).filter(W.posting_status == "Completed", W.posted_date >= d1, W.posted_date <= d2)).group_by(W.assigned_to_id, W.team_lead_id).all():
            grow(_order_team_lead_id(a, o, colleague_tl, tl_ids))["completed"] += n
        for a, o, rd in scoped(db.query(W.assigned_to_id, W.team_lead_id, W.received_date).filter(W.received_date.isnot(None), W.submitted.is_(False), func.coalesce(W.posting_status, "") != "Completed")).all():
            x = grow(_order_team_lead_id(a, o, colleague_tl, tl_ids)); x["open"] += 1
            if _order_age(rd, today, cache) >= late:
                x["late"] += 1
        tl_rows = sorted(g.values(), key=lambda x: x["team_lead"])
    return {"from": d1.isoformat(), "to": d2.isoformat(), "scope": _scope_label(user), "warn_days": warn, "late_days": late, "rows": out_rows, "total": tot,
            "top": [{"name": n, "completed": c, "trans": int(t or 0)} for n, c, t in top], "team_leads": tl_rows}


@app.get("/reports/management-summary")
def management_summary_report(
    period: str = "week",
    process_id: Optional[int] = None,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    today = datetime.now(IST).date()
    if period == "day":
        d1 = d2 = today - timedelta(days=1)
    elif period == "week":
        d1, d2 = today - timedelta(days=today.weekday() + 7), today - timedelta(days=today.weekday() + 1)
    elif period == "month":
        d1, d2, _l = _period_range("month")
    elif period == "last_month":
        d1, d2, _l = _period_range("last_month")
    else:
        raise HTTPException(status_code=400, detail="period must be day, week, month or last_month")
    return _mgmt_summary(db, current_user, d1, d2, process_id)


def _mgmt_email(title: str, s: dict):
    import html as _h
    t = s["total"]
    def tat(v):
        return "-" if v is None else f"{v:g}"
    head = ["Process", "Received", "Completed", "Trans count", "To Production", "Avg TAT (days)", "Escalations raised", "Closed", "Open now", "At risk", "Late"]
    def cells(r):
        return [r.get("process", "Total"), r["received"], r["completed"], r["trans"], r["to_production"], tat(r.get("avg_tat")), r["raised"], r["closed"], r["open"], r["at_risk"], r["late"]]
    rng = s["from"] if s["from"] == s["to"] else f"{s['from']} to {s['to']}"
    text = f"{title}\n{rng} ({s['scope']})\n\n" + "\n".join(" | ".join(str(c) for c in cells(r)) for r in s["rows"] + [dict(t, process="TOTAL")])
    if s["top"]:
        text += "\n\nTop completed: " + ", ".join(f"{x['name']} ({x['completed']})" for x in s["top"])
    td = 'style="padding:5px 8px;border-bottom:1px solid #e3e7ef;font-size:12.5px;text-align:right;"'
    tdl = td.replace("text-align:right", "text-align:left")
    th = lambda x: f'<th style="background:#0b2545;color:#fff;padding:6px 8px;text-align:left;font-size:12px;">{_h.escape(x)}</th>'
    def trow(r, bold=False):
        cs = cells(r)
        b = "font-weight:700;" if bold else ""
        return "<tr>" + f'<td {tdl.replace("font-size", b + "font-size")}>{_h.escape(str(cs[0]))}</td>' + "".join(f'<td {td.replace("font-size", b + "font-size")}>{_h.escape(str(c))}</td>' for c in cs[1:]) + "</tr>"
    html = f'<div style="font-family:Segoe UI,Arial,sans-serif;color:#222;"><h3 style="color:#0b2545;margin:0 0 4px;">{_h.escape(title)}</h3><div style="color:#666;font-size:13px;margin-bottom:10px;">{_h.escape(rng)} &middot; {_h.escape(s["scope"])}</div>'
    html += "<table style='border-collapse:collapse;'><tr>" + "".join(th(x) for x in head) + "</tr>" + "".join(trow(r) for r in s["rows"]) + trow(dict(t, process="Total"), True) + "</table>"
    if s["team_leads"]:
        text += "\n\nBy Team Lead:\n" + "\n".join(f"{x['team_lead']}: received {x['received']}, completed {x['completed']}, open {x['open']}, late {x['late']}" for x in s["team_leads"])
        html += '<h4 style="color:#0b2545;margin:14px 0 4px;">By Team Lead</h4><table style="border-collapse:collapse;"><tr>' + "".join(th(x) for x in ("Team Lead", "Received", "Completed", "Open now", "Late")) + "</tr>" + \
                "".join(f"<tr><td {tdl}>{_h.escape(x['team_lead'])}</td><td {td}>{x['received']}</td><td {td}>{x['completed']}</td><td {td}>{x['open']}</td><td {td}>{x['late']}</td></tr>" for x in s["team_leads"]) + "</table>"
    if s["top"]:
        html += '<h4 style="color:#0b2545;margin:14px 0 4px;">Top completed</h4><table style="border-collapse:collapse;"><tr>' + "".join(th(x) for x in ("Colleague", "Completed", "Trans count")) + "</tr>" + \
                "".join(f"<tr><td {tdl}>{_h.escape(x['name'])}</td><td {td}>{x['completed']}</td><td {td}>{x['trans']}</td></tr>" for x in s["top"]) + "</table>"
    html += (f'<p style="color:#888;font-size:11.5px;margin-top:10px;">Open now = not Completed and not yet in Production. At risk = {s["warn_days"]}+ business days since the Received Date, '
             f'late = {s["late_days"]}+. Sent automatically by WorkFlow Hub.</p></div>')
    return text, html


MGMT_KEYS = {"daily": "mgmt_daily", "weekly": "mgmt_weekly", "time": "mgmt_time", "last_d": "mgmt_last_daily", "last_w": "mgmt_last_weekly"}


def _mgmt_settings(db: Session) -> dict:
    return {"daily": _setting(db, MGMT_KEYS["daily"], "0") == "1", "weekly": _setting(db, MGMT_KEYS["weekly"], "0") == "1", "time": _setting(db, MGMT_KEYS["time"], "09:00"),
            "last_daily": _setting(db, MGMT_KEYS["last_d"], ""), "last_weekly": _setting(db, MGMT_KEYS["last_w"], ""), "smtp_configured": email_utils.is_configured()}


def run_management_summary(db: Session, kind: str = "daily", only_to: str = None) -> dict:
    today = datetime.now(IST).date()
    if kind == "weekly":
        d1, d2, name = today - timedelta(days=today.weekday() + 7), today - timedelta(days=today.weekday() + 1), "Weekly"
    else:
        d1 = d2 = today - timedelta(days=1)
        name = "Daily"
    res = {"kind": kind, "from": d1.isoformat(), "to": d2.isoformat(), "sent": 0, "failed": [], "skipped_no_email": [], "note": ""}
    def send(user, to, title):
        s = _mgmt_summary(db, user, d1, d2)
        t = s["total"]
        if not any(t[k] for k in ("received", "completed", "to_production", "raised", "closed")) and not t["late"]:
            return False
        text, html = _mgmt_email(title, s)
        if email_utils.send_email(to, f"{title} - {d1.strftime('%d %b %Y')}" + ("" if d1 == d2 else f" to {d2.strftime('%d %b %Y')}"), text, html):
            res["sent"] += 1
        else:
            res["failed"].append(f"{to}: {email_utils.last_error}")
        return True
    if only_to:
        su = db.query(models.User).filter(models.User.role == "super_admin", models.User.employment_status != "Inactive").first()
        if not su or not send(su, only_to, f"{name} production summary - all teams (test)"):
            res["note"] = "Nothing to report for that period, so no test mail was sent."
        return res
    for tl in db.query(models.User).filter(models.User.role == "team_lead", models.User.employment_status != "Inactive").all():
        if not tl.email:
            res["skipped_no_email"].append(tl.full_name); continue
        send(tl, tl.email, f"{name} production summary - your team")
    if _setting(db, DIGEST_KEYS["admins"], "1") == "1":
        for a in db.query(models.User).filter(models.User.role.in_(["admin", "manager", "super_admin"]), models.User.employment_status != "Inactive").all():
            if a.email:
                send(a, a.email, f"{name} production summary - all teams")
    return res


@app.get("/settings/mgmt-summary")
def get_mgmt_settings(current_user: models.User = Depends(auth.require_role("super_admin")), db: Session = Depends(get_db)):
    return _mgmt_settings(db)


@app.put("/settings/mgmt-summary")
def set_mgmt_settings(payload: dict, current_user: models.User = Depends(auth.require_role("super_admin")), db: Session = Depends(get_db)):
    t = str(payload.get("time", "09:00"))
    if not re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", t):
        raise HTTPException(status_code=400, detail="Time must look like 09:00")
    _set_setting(db, MGMT_KEYS["daily"], "1" if payload.get("daily") else "0")
    _set_setting(db, MGMT_KEYS["weekly"], "1" if payload.get("weekly") else "0")
    _set_setting(db, MGMT_KEYS["time"], t)
    _audit(db, current_user, "Setting", "Changed production summary email settings", details=f"Daily {'on' if payload.get('daily') else 'off'}; weekly (Mondays) {'on' if payload.get('weekly') else 'off'}; at {t}")
    db.commit()
    return _mgmt_settings(db)


@app.post("/settings/mgmt-summary/send")
def send_mgmt_summary(mode: str = "daily_test", current_user: models.User = Depends(auth.require_role("super_admin")), db: Session = Depends(get_db)):
    if not email_utils.is_configured():
        raise HTTPException(status_code=400, detail=email_utils.last_error or "SMTP is not configured on the server (see the SMTP lines in run.bat).")
    kind, _, how = mode.partition("_")
    if kind not in ("daily", "weekly") or how not in ("test", "now"):
        raise HTTPException(status_code=400, detail="Unknown mode")
    _audit(db, current_user, "Setting", "Sent production summary email", details=f"mode={mode}")
    db.commit()
    if how == "test":
        if not current_user.email:
            raise HTTPException(status_code=400, detail="Add your email address in your user record first.")
        return run_management_summary(db, kind, only_to=current_user.email)
    return run_management_summary(db, kind)


# ---------------- Database backup (laptop / SQLite version) ----------------
BACKUP_KEYS = {"auto": "backup_auto", "time": "backup_time", "keep": "backup_keep", "last_auto": "backup_last_auto"}


def _sqlite_path():
    from .database import DATABASE_URL
    if not DATABASE_URL.startswith("sqlite"):
        return None
    p = DATABASE_URL.split("///", 1)[-1]
    return os.path.abspath(p) if p and p != ":memory:" else None


def _backup_dir(db_path: str) -> str:
    d = os.path.join(os.path.dirname(db_path), "backups")
    os.makedirs(d, exist_ok=True)
    return d


def _make_backup(kind: str = "manual") -> dict:
    import sqlite3
    src_path = _sqlite_path()
    if not src_path or not os.path.exists(src_path):
        raise RuntimeError("Backups from this screen work with the laptop (SQLite) version only.")
    d = _backup_dir(src_path)
    name = f"workorders_{datetime.now(IST).strftime('%Y%m%d_%H%M%S')}_{kind}.db"
    dest = os.path.join(d, name)
    src, dst = sqlite3.connect(src_path), sqlite3.connect(dest)
    try:
        src.backup(dst)                 # consistent copy even while the app is running
    finally:
        dst.close(); src.close()
    return {"name": name, "size": os.path.getsize(dest)}


def _list_backups():
    p = _sqlite_path()
    if not p:
        return []
    d = _backup_dir(p)
    out = []
    for n in os.listdir(d):
        m = re.fullmatch(r"workorders_(\d{8})_(\d{6})_(manual|auto|pre)\.db", n)
        if not m:
            continue
        full = os.path.join(d, n)
        at = datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S")
        out.append({"name": n, "size": os.path.getsize(full), "at": at.isoformat(), "kind": m.group(3)})
    return sorted(out, key=lambda x: x["at"], reverse=True)


def _prune_backups(db: Session):
    try:
        keep = max(1, int(_setting(db, BACKUP_KEYS["keep"], "14")))
    except ValueError:
        keep = 14
    p = _sqlite_path()
    for b in [x for x in _list_backups() if x["kind"] == "auto"][keep:]:
        try:
            os.remove(os.path.join(_backup_dir(p), b["name"]))
        except OSError:
            pass


def _backup_settings(db: Session) -> dict:
    return {"auto": _setting(db, BACKUP_KEYS["auto"], "0") == "1", "time": _setting(db, BACKUP_KEYS["time"], "22:00"), "keep": int(_setting(db, BACKUP_KEYS["keep"], "14") or 14)}


@app.get("/backup/status")
def backup_status(current_user: models.User = Depends(auth.require_role("super_admin")), db: Session = Depends(get_db)):
    p = _sqlite_path()
    if not p:
        return {"supported": False, "note": "This server does not use a SQLite file, so use your hosting provider's database backups."}
    items = _list_backups()
    last = datetime.fromisoformat(items[0]["at"]) if items else None
    days = (datetime.now(IST).replace(tzinfo=None) - last).days if last else None
    return {"supported": True, "db_file": os.path.basename(p), "db_size": os.path.getsize(p) if os.path.exists(p) else 0, "folder": _backup_dir(p), "backups": items,
            "last_backup": items[0]["at"] if items else None, "days_since": days, "settings": _backup_settings(db), "restore": _restore_state(db)}


@app.post("/backup/create")
def backup_create(current_user: models.User = Depends(auth.require_role("super_admin")), db: Session = Depends(get_db)):
    try:
        b = _make_backup("manual")
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    _audit(db, current_user, "Backup", "Created backup", details=b["name"])
    db.commit()
    return b


@app.put("/backup/settings")
def backup_set_settings(payload: dict, current_user: models.User = Depends(auth.require_role("super_admin")), db: Session = Depends(get_db)):
    t = str(payload.get("time", "22:00"))
    if not re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", t):
        raise HTTPException(status_code=400, detail="Time must look like 22:00")
    try:
        keep = int(payload.get("keep", 14))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Enter how many daily backups to keep")
    if keep < 1 or keep > 90:
        raise HTTPException(status_code=400, detail="Keep between 1 and 90 automatic backups")
    _set_setting(db, BACKUP_KEYS["auto"], "1" if payload.get("auto") else "0")
    _set_setting(db, BACKUP_KEYS["time"], t)
    _set_setting(db, BACKUP_KEYS["keep"], str(keep))
    _audit(db, current_user, "Backup", "Changed backup settings", details=f"Automatic {'on' if payload.get('auto') else 'off'} at {t}; keep {keep}")
    db.commit()
    return _backup_settings(db)


def _safe_backup_path(name: str) -> str:
    if not re.fullmatch(r"workorders_\d{8}_\d{6}_(manual|auto|pre)\.db", name or ""):
        raise HTTPException(status_code=404, detail="Backup not found")
    p = _sqlite_path()
    full = os.path.join(_backup_dir(p), name) if p else None
    if not full or not os.path.exists(full):
        raise HTTPException(status_code=404, detail="Backup not found")
    return full


@app.get("/backup/download/{name}")
def backup_download(name: str, current_user: models.User = Depends(auth.require_role("super_admin")), db: Session = Depends(get_db)):
    from fastapi.responses import FileResponse
    full = _safe_backup_path(name)
    _audit(db, current_user, "Backup", "Downloaded backup", details=name)
    db.commit()
    return FileResponse(full, media_type="application/octet-stream", filename=name)


@app.delete("/backup/{name}")
def backup_delete(name: str, current_user: models.User = Depends(auth.require_role("super_admin")), db: Session = Depends(get_db)):
    full = _safe_backup_path(name)
    os.remove(full)
    _audit(db, current_user, "Backup", "Deleted backup", details=name)
    db.commit()
    return {"ok": True}


# ---------------------------------------------------------------------------
# System log viewer (Super Admin): "what happened at 10:42?"
# ---------------------------------------------------------------------------
_LOG_LINE = re.compile(r"^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}) (DEBUG|INFO|WARNING|ERROR|CRITICAL) (.*)$")


def _read_log_entries(day: str) -> list:
    d = _log_dir()
    base = os.path.join(d, "workflowhub.log")
    files = [base] + [f"{base}.{i}" for i in range(1, 6)]
    entries = []
    for f in files:
        if not os.path.exists(f):
            continue
        try:
            with open(f, "r", encoding="utf-8", errors="replace") as fh:
                for line in fh:
                    m = _LOG_LINE.match(line.rstrip("\n"))
                    if m:
                        if m.group(1) == day:
                            entries.append({"at": m.group(2), "level": m.group(3), "text": m.group(4)})
                        else:
                            entries.append(None)          # a line of another day: keeps continuation lines from sticking to the wrong entry
                    elif entries and entries[-1] is not None:
                        if len(entries[-1]["text"]) < 4000:
                            entries[-1]["text"] += "\n" + line.rstrip("\n")
        except OSError:
            continue
    return [e for e in entries if e]


@app.get("/logs/system")
def system_log(day: Optional[str] = None, around: Optional[str] = None, minutes: int = 15, level: str = "all", q: str = "",
               current_user: models.User = Depends(auth.require_role("super_admin"))):
    """Log lines for one day (India time). `around` = HH:MM with +/- `minutes`; level=problems keeps warnings and errors."""
    today = datetime.now(IST).strftime("%Y-%m-%d")
    day = day or today
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day):
        raise HTTPException(status_code=400, detail="Date must look like 2026-11-09")
    items = _read_log_entries(day)
    if around:
        if not re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", around):
            raise HTTPException(status_code=400, detail="Time must look like 10:42")
        c = int(around[:2]) * 60 + int(around[3:])
        minutes = max(1, min(720, minutes))
        items = [e for e in items if abs((int(e["at"][:2]) * 60 + int(e["at"][3:5])) - c) <= minutes]
    if level == "problems":
        items = [e for e in items if e["level"] in ("WARNING", "ERROR", "CRITICAL")]
    elif level == "errors":
        items = [e for e in items if e["level"] in ("ERROR", "CRITICAL")]
    if q.strip():
        ql = q.strip().lower()
        items = [e for e in items if ql in e["text"].lower()]
    total = len(items)
    items = items[-500:]
    base = os.path.join(_log_dir(), "workflowhub.log")
    return {"day": day, "total": total, "shown": len(items), "entries": items, "log_file": base, "log_exists": os.path.exists(base)}


@app.get("/logs/system/download")
def system_log_download(current_user: models.User = Depends(auth.require_role("super_admin"))):
    from fastapi.responses import FileResponse
    f = os.path.join(_log_dir(), "workflowhub.log")
    if not os.path.exists(f):
        raise HTTPException(status_code=404, detail="No log file yet")
    return FileResponse(f, media_type="text/plain", filename="workflowhub.log")


# ---------------------------------------------------------------------------
# Restore test: prove a backup can really be restored (quarterly, automatic + on demand)
# ---------------------------------------------------------------------------
RESTORE_CHECK_DAYS = 90
_RESTORE_KEY_TABLES = ["users", "processes", "clients", "work_orders", "login_days", "audit_events"]


def _restore_state(db: Session) -> dict:
    try:
        last = json.loads(_setting(db, "restore_check", "") or "null")
    except ValueError:
        last = None
    try:
        hist = json.loads(_setting(db, "restore_check_hist", "") or "[]")
    except ValueError:
        hist = []
    days = None
    if last:
        try:
            days = (datetime.now(IST).replace(tzinfo=None) - datetime.fromisoformat(last["at"])).days
        except Exception:
            days = None
    due = last is None or days is None or days >= RESTORE_CHECK_DAYS or not last.get("ok")
    return {"last": last, "history": hist, "days_since": days, "due": due, "every_days": RESTORE_CHECK_DAYS}


def _run_restore_check(db: Session, name: Optional[str], by: str) -> dict:
    """Copies a backup into a spare folder, opens the copy and checks it (integrity, tables, row counts). Never touches the live database."""
    import sqlite3, time as _t
    src_path = _sqlite_path()
    if not src_path:
        raise RuntimeError("Restore tests work with the laptop (SQLite) version only.")
    items = _list_backups()
    if not items:
        raise RuntimeError("There is no backup to test yet. Make one first.")
    chosen = next((b for b in items if b["name"] == name), None) if name else items[0]
    if not chosen:
        raise RuntimeError("That backup was not found.")
    folder = _backup_dir(src_path)
    spare = os.path.join(folder, "_restore_test")
    shutil.rmtree(spare, ignore_errors=True)
    os.makedirs(spare, exist_ok=True)
    t0 = _t.perf_counter()
    res = {"at": datetime.now(IST).replace(tzinfo=None).isoformat(timespec="seconds"), "backup": chosen["name"], "backup_at": chosen["at"], "by": by,
           "ok": False, "integrity": None, "counts": {}, "missing_tables": [], "newest_order": None, "problems": []}
    try:
        copy = os.path.join(spare, chosen["name"])
        shutil.copy2(os.path.join(folder, chosen["name"]), copy)
        res["size"] = os.path.getsize(copy)
        con = sqlite3.connect("file:" + copy.replace("\\", "/") + "?mode=ro", uri=True)
        try:
            res["integrity"] = con.execute("PRAGMA integrity_check").fetchone()[0]
            have = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            res["missing_tables"] = sorted(set(models.Base.metadata.tables) - have)
            for t in _RESTORE_KEY_TABLES:
                if t in have:
                    b = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                    live = db.execute(sa_text(f"SELECT COUNT(*) FROM {t}")).scalar()
                    res["counts"][t] = {"backup": b, "live": live}
            if "work_orders" in have:
                r = con.execute("SELECT MAX(received_date) FROM work_orders").fetchone()
                res["newest_order"] = str(r[0]) if r and r[0] else None
        finally:
            con.close()
        if res["integrity"] != "ok":
            res["problems"].append("The database file failed its integrity check.")
        for t in ("users", "processes"):
            c = res["counts"].get(t)
            if c is None:
                res["problems"].append(f"Table '{t}' is missing from the backup.")
            elif c["backup"] == 0 and c["live"] > 0:
                res["problems"].append(f"Table '{t}' is empty in the backup but has {c['live']} rows now.")
        wo = res["counts"].get("work_orders")
        if wo and wo["backup"] == 0 and wo["live"] > 50:
            res["problems"].append("The backup holds no orders although the app does.")
        res["ok"] = not res["problems"]
    except Exception as e:
        res["problems"].append(f"Could not open the restored copy: {e}")
    finally:
        shutil.rmtree(spare, ignore_errors=True)
    res["seconds"] = round(_t.perf_counter() - t0, 1)
    _set_setting(db, "restore_check", json.dumps(res))
    hist = _restore_state(db)["history"]
    hist.insert(0, {k: res[k] for k in ("at", "backup", "ok", "by")})
    _set_setting(db, "restore_check_hist", json.dumps(hist[:8]))
    _audit(db, None, "Backup", "Restore test " + ("passed" if res["ok"] else "FAILED"), None, None, f"{res['backup']} ({by})" + ("" if res["ok"] else ": " + "; ".join(res["problems"])[:300]))
    db.commit()
    (logging.getLogger("workflowhub").info if res["ok"] else logging.getLogger("workflowhub").error)("restore test %s backup=%s by=%s %s", "OK" if res["ok"] else "FAILED", res["backup"], by, "; ".join(res["problems"]))
    return res


@app.post("/backup/verify")
def backup_verify(payload: Optional[dict] = None, current_user: models.User = Depends(auth.require_role("super_admin")), db: Session = Depends(get_db)):
    try:
        return _run_restore_check(db, (payload or {}).get("name"), current_user.full_name)
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))


def _auto_restore_check():
    """Quarterly (when never tested, older than 90 days, or last test failed): tests the newest backup. At most one try a day."""
    db = SessionLocal()
    try:
        if not _sqlite_path() or not _list_backups():
            return
        st = _restore_state(db)
        today = datetime.now(IST).date().isoformat()
        if st["due"] and _setting(db, "restore_check_try", "") != today:
            _set_setting(db, "restore_check_try", today)
            db.commit()
            _run_restore_check(db, None, "automatic")
    finally:
        db.close()


def _scheduled_jobs_loop():
    """Once a minute: automatic database backup, production summary emails."""
    import time as _t
    while True:
        _t.sleep(60)
        db = SessionLocal()
        try:
            if not _jobs_leader(db):
                continue
            now = datetime.now(IST)
            bs = _backup_settings(db)
            if bs["auto"] and _sqlite_path():
                try:
                    hh, mm = (int(x) for x in bs["time"].split(":"))
                except ValueError:
                    hh, mm = 22, 0
                if (now.hour, now.minute) >= (hh, mm) and _setting(db, BACKUP_KEYS["last_auto"], "") != now.date().isoformat():
                    _set_setting(db, BACKUP_KEYS["last_auto"], now.date().isoformat())
                    db.commit()
                    try:
                        print("[backup]", _make_backup("auto"))
                        _prune_backups(db)
                    except Exception as e:
                        logging.getLogger("workflowhub").error("backup failed: %s", e); print("[backup] failed:", e)
            ms = _mgmt_settings(db)
            if ms["smtp_configured"] and (ms["daily"] or ms["weekly"]):
                try:
                    hh, mm = (int(x) for x in ms["time"].split(":"))
                except ValueError:
                    hh, mm = 9, 0
                if (now.hour, now.minute) >= (hh, mm):
                    if ms["daily"] and ms["last_daily"] != now.date().isoformat():
                        _set_setting(db, MGMT_KEYS["last_d"], now.date().isoformat()); db.commit()
                        print("[summary daily]", run_management_summary(db, "daily"))
                    if ms["weekly"] and now.weekday() == 0 and ms["last_weekly"] != now.date().isoformat():
                        _set_setting(db, MGMT_KEYS["last_w"], now.date().isoformat()); db.commit()
                        print("[summary weekly]", run_management_summary(db, "weekly"))
            if datetime.now(IST).hour == 23 and datetime.now(IST).minute >= 30:
                try:
                    _auto_restore_check()
                except Exception as e:
                    logging.getLogger("workflowhub").error("restore test error: %s", e)
            if now.weekday() == 6 and now.hour == 2:
                try:
                    _auto_archive()
                except Exception as e:
                    logging.getLogger("workflowhub").error("auto archive error: %s", e)
            if datetime.now(IST).minute % 15 == 0:
                try:
                    _esc_auto_reminders()
                except Exception as e:
                    print("[esc reminders] error:", e)
        except Exception as e:
            logging.getLogger("workflowhub").error("jobs error: %s", e); print("[jobs] error:", e)
        finally:
            db.close()


# ======================================================================
# Workload balancing, time-per-order benchmarks, end-of-day checklist
# ======================================================================
def _median(vals):
    v = sorted(vals)
    n = len(v)
    if not n:
        return 0
    return v[n // 2] if n % 2 else (v[n // 2 - 1] + v[n // 2]) / 2


@app.get("/workload")
def workload_view(
    process_id: Optional[int] = None,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    """Each colleague's open orders, completed today and average time per order, with overloaded / idle flags and open orders ready to reassign."""
    W, U = models.WorkOrder, models.User
    if process_id is not None and current_user.role == "team_lead":
        _require_process_access(current_user, process_id)
    cq = db.query(U).options(selectinload(U.processes)).filter(U.role == "colleague", U.employment_status != "Inactive")
    if current_user.role == "team_lead":
        cq = cq.filter(U.reporting_manager == current_user.full_name)
    cols = cq.all()
    if process_id is not None:
        cols = [c for c in cols if any(p.id == process_id for p in c.processes)]
    elif current_user.role == "team_lead":
        mine = {p.id for p in current_user.processes}
        cols = [c for c in cols if any(p.id in mine for p in c.processes)]
    ids = [c.id for c in cols]
    today = _work_today()
    week_ago = today - timedelta(days=6)
    open_by, done_today, done_week = {}, {}, {}
    oq = db.query(W).filter(W.assigned_to_id.in_(ids) if ids else sa_false(), W.submitted.is_(False), func.coalesce(W.posting_status, "") != "Completed")
    if process_id is not None:
        oq = oq.filter(W.process_id == process_id)
    for o in oq.all():
        open_by.setdefault(o.assigned_to_id, []).append(o)
    dq = db.query(W).filter(W.assigned_to_id.in_(ids) if ids else sa_false(), W.posting_status == "Completed", W.posted_date >= week_ago, W.posted_date <= today)
    if process_id is not None:
        dq = dq.filter(W.process_id == process_id)
    for o in dq.all():
        done_week.setdefault(o.assigned_to_id, []).append(o)
        if o.posted_date == today:
            done_today.setdefault(o.assigned_to_id, []).append(o)
    on_leave = {u for (u,) in db.query(models.LeaveDay.user_id).filter(models.LeaveDay.leave_date == today).all()}
    cache = {}
    def avg_min(lst):
        t = [o.time_taken_seconds for o in lst if (o.time_taken_seconds or 0) > 0]
        return round(sum(t) / len(t) / 60, 1) if t else None
    rows = []
    for c in cols:
        op = open_by.get(c.id, [])
        dt, dw = done_today.get(c.id, []), done_week.get(c.id, [])
        rows.append({"id": c.id, "name": c.full_name, "team_lead": c.reporting_manager or "", "process_ids": [p.id for p in c.processes], "on_leave": c.id in on_leave,
                     "open": len(op), "in_process": sum(1 for o in op if not o.escalated and (o.posting_status or "") != "Clarification"),
                     "escalated": sum(1 for o in op if o.escalated or (o.posting_status or "") == "Clarification"),
                     "running": any(o.timer_status == "running" for o in op),
                     "done_today": len(dt), "trans_today": sum(int(o.trans_count or 0) for o in dt), "avg_today": avg_min(dt), "done_7d": len(dw), "avg_7d": avg_min(dw),
                     "orders": [{"id": o.id, "edm": o.edm or ("#%d" % o.id), "process_id": o.process_id, "status": o.posting_status or "Pending", "escalated": bool(o.escalated), "timer": o.timer_status,
                                 "age": _order_age(o.received_date, datetime.now(IST).date(), cache) if o.received_date else None} for o in op]})
    active = [r for r in rows if not r["on_leave"]]
    avg_open = (sum(r["open"] for r in active) / len(active)) if active else 0
    limit = max(3, int(avg_open * 1.5 + 0.999))
    for r in rows:
        r["flag"] = "leave" if r["on_leave"] else ("overloaded" if r["open"] >= limit else ("idle" if r["open"] == 0 else ""))
    rows.sort(key=lambda r: (-r["open"], r["name"]))
    unassigned = 0
    scoped = _orders_dash_scope(db, current_user, process_id)
    unassigned = scoped(db.query(func.count(W.id)).filter(W.assigned_to_id.is_(None), W.submitted.is_(False), func.coalesce(W.posting_status, "") != "Completed")).scalar() or 0
    procs = {p.id: p.name for p in db.query(models.Process).all()}
    return {"date": today.isoformat(), "overload_at": limit, "avg_open": round(avg_open, 1), "unassigned": unassigned, "processes": procs, "colleagues": rows,
            "totals": {"open": sum(r["open"] for r in rows), "done_today": sum(r["done_today"] for r in rows), "trans_today": sum(r["trans_today"] for r in rows),
                       "overloaded": sum(1 for r in rows if r["flag"] == "overloaded"), "idle": sum(1 for r in rows if r["flag"] == "idle")}}


@app.get("/orders/time-benchmarks")
def time_benchmarks(
    process_id: Optional[int] = None,
    period: str = "month",
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    """Completed orders' Time Taken against the process average: unusually long / short ones and timer mistakes, plus a per-colleague comparison."""
    if period not in ("week", "month", "last_month"):
        raise HTTPException(status_code=400, detail="period must be week, month or last_month")
    W = models.WorkOrder
    d1, d2, label = _period_range(period)
    scoped = _orders_dash_scope(db, current_user, process_id)
    rows = scoped(db.query(W).options(selectinload(W.process)).filter(W.posting_status == "Completed", W.posted_date >= d1, W.posted_date <= d2)).all()
    by_proc = {}
    for o in rows:
        by_proc.setdefault(o.process_id, []).append(o)
    stats = {}
    for pid, lst in by_proc.items():
        times = [o.time_taken_seconds for o in lst if (o.time_taken_seconds or 0) >= 30]
        stats[pid] = {"process": lst[0].process.name if lst[0].process else "(none)", "orders": len(lst), "timed": len(times),
                      "avg": round(sum(times) / len(times) / 60, 1) if times else None, "median": round(_median(times) / 60, 1) if times else None}
    flagged, by_col = [], {}
    for o in rows:
        t = o.time_taken_seconds or 0
        st = stats[o.process_id]
        med = (st["median"] or 0) * 60
        why = None
        if t == 0 or t < 30:
            why = "No time recorded (timer not used)"
        elif t > 8 * 3600:
            why = "Over 8 hours - timer left running?"
        elif st["timed"] >= 5 and med:
            if t >= 2 * med and t - med >= 600:
                why = "Much longer than usual"
            elif t <= 0.25 * med and med - t >= 120:
                why = "Much shorter than usual"
        name = o.employee_name or "(unknown)"
        g = by_col.setdefault((o.process_id, name), {"name": name, "process": st["process"], "orders": 0, "times": [], "flags": 0})
        g["orders"] += 1
        if t >= 30:
            g["times"].append(t)
        if why:
            g["flags"] += 1
            flagged.append({"id": o.id, "edm": o.edm or ("#%d" % o.id), "process": st["process"], "colleague": name, "posted": o.posted_date.isoformat() if o.posted_date else "",
                            "minutes": round(t / 60, 1), "process_avg": st["avg"], "process_median": st["median"], "ratio": round(t / med, 1) if med and t else None, "why": why})
    order_why = {"Over 8 hours - timer left running?": 0, "Much longer than usual": 1, "Much shorter than usual": 2, "No time recorded (timer not used)": 3}
    flagged.sort(key=lambda f: (order_why[f["why"]], -(f["minutes"] or 0)))
    cols = []
    for g in by_col.values():
        pavg = next((s["avg"] for s in stats.values() if s["process"] == g["process"]), None)
        avg = round(sum(g["times"]) / len(g["times"]) / 60, 1) if g["times"] else None
        cols.append({"name": g["name"], "process": g["process"], "orders": g["orders"], "avg": avg, "process_avg": pavg,
                     "vs_process": round((avg / pavg - 1) * 100) if avg and pavg else None, "flags": g["flags"]})
    cols.sort(key=lambda c: (c["process"], -(c["vs_process"] if c["vs_process"] is not None else -999)))
    return {"label": label, "from": d1.isoformat(), "to": d2.isoformat(), "processes": sorted(stats.values(), key=lambda s: s["process"]), "colleagues": cols,
            "flagged": flagged[:300], "flag_total": len(flagged), "orders": len(rows)}


@app.get("/orders/day-checklist")
def day_checklist(
    process_id: int,
    current_user: models.User = Depends(auth.require_role("colleague")),
    db: Session = Depends(get_db),
):
    """What a colleague may have forgotten before Submit End of Day (advisory only)."""
    _require_process_access(current_user, process_id)
    W = models.WorkOrder
    mine = db.query(W).filter(W.process_id == process_id, W.assigned_to_id == current_user.id, W.submitted.is_(False)).all()
    def item(o, extra=""):
        return {"id": o.id, "edm": o.edm or ("#%d" % o.id), "note": extra}
    unstarted, running, clar, nohours, ready = [], [], [], [], 0
    for o in mine:
        done = (o.posting_status or "") == "Completed"
        if done:
            ready += 1
            if (o.time_taken_seconds or 0) < 30:
                nohours.append(item(o, "No Time Taken recorded"))
            elif not o.trans_count:
                nohours.append(item(o, "Trans Count is empty"))
        elif o.escalated or (o.posting_status or "") == "Clarification":
            clar.append(item(o, "Waiting with " + _holder_all(o) if (o.onshore_status or _holder_all(o) != "Team Lead") else "Waiting for Team Lead"))
        elif o.timer_status == "not_started":
            unstarted.append(item(o, "Timer never started"))
        elif o.timer_status == "running":
            running.append(item(o, "Timer is still running"))
    return {"ready": ready, "unstarted": unstarted, "running": running, "clarification": clar, "missing_time": nohours,
            "issues": len(unstarted) + len(running) + len(clar) + len(nohours)}


# ======================================================================
# Process update acknowledgement tracking, new joiner view, notifications centre
# ======================================================================
def _unack_scan(db: Session, user: models.User) -> list:
    """Active, acknowledgement-required updates (last 90 days) with the colleagues in this user's scope who have not acknowledged yet."""
    since = datetime.utcnow() - timedelta(days=90)
    uq = db.query(models.ProcessUpdate).options(selectinload(models.ProcessUpdate.attachments)).filter(
        models.ProcessUpdate.ack_required.is_(True), models.ProcessUpdate.status == "Active", models.ProcessUpdate.created_at >= since)
    if user.role == "team_lead":
        mine = [p.id for p in user.processes]
        uq = uq.filter(models.ProcessUpdate.process_id.in_(mine or [-1]))
    updates = uq.order_by(models.ProcessUpdate.created_at.desc()).all()
    if not updates:
        return []
    colleagues = [u for u in _batch_visible_colleagues(db, user) if u.employment_status != "Inactive"]
    for u in colleagues:
        u.processes  # load
    ids = [u.id for u in updates]
    got = {}
    for a in db.query(models.ProcessUpdateAck.update_id, models.ProcessUpdateAck.user_id).filter(models.ProcessUpdateAck.update_id.in_(ids)).all():
        got.setdefault(a[0], set()).add(a[1])
    last_rem = {}
    for r in db.query(models.UpdateReminder).filter(models.UpdateReminder.update_id.in_(ids)).all():
        k = (r.update_id, r.user_id)
        if k not in last_rem or r.sent_at > last_rem[k]:
            last_rem[k] = r.sent_at
    procs = {p.id: p.name for p in db.query(models.Process).all()}
    out = []
    for up in updates:
        scope = [c for c in colleagues if any(p.id == up.process_id for p in c.processes)]
        pending = [c for c in scope if c.id not in got.get(up.id, set())]
        out.append({"id": up.id, "process": procs.get(up.process_id, ""), "process_id": up.process_id, "message": (up.message or "")[:160], "posted_by": up.posted_by_name,
                    "created_at": up.created_at.isoformat() + "Z" if up.created_at else "", "age_days": (datetime.utcnow() - up.created_at).days if up.created_at else 0,
                    "total": len(scope), "acked": len(scope) - len(pending),
                    "pending": [{"user_id": c.id, "name": c.full_name, "team_lead": c.reporting_manager or "", "reminded_at": last_rem[(up.id, c.id)].isoformat() + "Z" if (up.id, c.id) in last_rem else ""}
                                for c in sorted(pending, key=lambda x: x.full_name.lower())]})
    return out


@app.get("/updates/unacknowledged")
def unacknowledged_updates(current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")), db: Session = Depends(get_db)):
    rows = [r for r in _unack_scan(db, current_user) if r["pending"]]
    people = {}
    for r in rows:
        for c in r["pending"]:
            g = people.setdefault(c["user_id"], {"name": c["name"], "team_lead": c["team_lead"], "count": 0})
            g["count"] += 1
    return {"total": sum(len(r["pending"]) for r in rows), "updates": rows, "colleagues": sorted(people.values(), key=lambda g: (-g["count"], g["name"])),
            "smtp_configured": email_utils.is_configured()}


@app.post("/updates/remind")
def remind_unacknowledged(payload: dict, current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")), db: Session = Depends(get_db)):
    """Remind colleagues who have not acknowledged: one update (update_id) or all (omit). Each person is reminded at most once a day per update."""
    only = payload.get("update_id")
    now = datetime.utcnow()
    sent = skipped = emailed = 0
    users = {u.id: u for u in _batch_visible_colleagues(db, current_user)}
    per_user = {}
    for r in _unack_scan(db, current_user):
        if only is not None and r["id"] != only:
            continue
        for c in r["pending"]:
            if c["reminded_at"] and (now - datetime.fromisoformat(c["reminded_at"].rstrip("Z"))) < timedelta(hours=20):
                skipped += 1
                continue
            per_user.setdefault(c["user_id"], []).append(r)
    for uid, ups in per_user.items():
        u = users.get(uid)
        if not u:
            continue
        mail = False
        if u.email and email_utils.is_configured():
            lines = [f"- {x['process']}: {x['message']}" for x in ups]
            body = f"Hi {u.full_name},\n\n{current_user.full_name} asks you to review and acknowledge the following process update(s) in WorkFlow Hub:\n\n" + "\n".join(lines) + "\n\nOpen WorkFlow Hub > Home > Process Updates and click Acknowledge.\n"
            mail = email_utils.send_email(u.email, "Reminder: please acknowledge process update(s)", body)
            emailed += 1 if mail else 0
        for x in ups:
            db.add(models.UpdateReminder(update_id=x["id"], user_id=uid, sent_at=now, sent_by_name=current_user.full_name, emailed=mail))
        sent += 1
    _audit(db, current_user, "Update", "Sent acknowledgement reminders", details=f"{sent} colleague(s), {emailed} emailed" + (f", update #{only}" if only is not None else ""))
    db.commit()
    return {"reminded": sent, "emailed": emailed, "skipped_recent": skipped}


@app.get("/new-joiners")
def new_joiners(days: int = 60, target_orders: int = 5,
                current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")), db: Session = Depends(get_db)):
    """Ramp-up checklist per colleague who joined in the last N days (Date of Joining): processes, training updates acknowledged, first orders completed."""
    days = max(7, min(int(days), 365))
    target_orders = max(1, min(int(target_orders), 100))
    today = datetime.now(IST).date()
    cols = [u for u in _batch_visible_colleagues(db, current_user) if u.employment_status != "Inactive" and u.doj and u.doj >= today - timedelta(days=days)]
    for c in cols:
        c.processes
    ids = [c.id for c in cols]
    W = models.WorkOrder
    done = {}
    for o in db.query(W.assigned_to_id, W.process_id, W.posted_date, W.time_taken_seconds).filter(W.assigned_to_id.in_(ids or [-1]), W.posting_status == "Completed").all():
        done.setdefault(o[0], []).append(o)
    req = db.query(models.ProcessUpdate).filter(models.ProcessUpdate.ack_required.is_(True), models.ProcessUpdate.status == "Active").all()
    acks = {}
    for a in db.query(models.ProcessUpdateAck.update_id, models.ProcessUpdateAck.user_id).filter(models.ProcessUpdateAck.user_id.in_(ids or [-1])).all():
        acks.setdefault(a[1], set()).add(a[0])
    first_login = {}
    for uid, mn in db.query(models.SessionLog.user_id, func.min(models.SessionLog.login_at)).filter(models.SessionLog.user_id.in_(ids or [-1])).group_by(models.SessionLog.user_id).all():
        first_login[uid] = mn
    out = []
    for c in sorted(cols, key=lambda x: x.doj, reverse=True):
        pids = {p.id for p in c.processes}
        my_up = [u for u in req if u.process_id in pids and (u.created_at is None or u.created_at.date() >= c.doj - timedelta(days=30))]
        got = acks.get(c.id, set())
        d = done.get(c.id, [])
        times = [x[3] for x in d if (x[3] or 0) >= 30]
        steps = [
            {"key": "profile", "label": "Profile complete (email, shift, Team Lead)", "ok": bool(c.email and c.shift and c.reporting_manager), "detail": ""},
            {"key": "processes", "label": "Process assigned", "ok": bool(pids), "detail": ", ".join(sorted(p.name for p in c.processes))},
            {"key": "login", "label": "Has logged in", "ok": c.id in first_login or bool(c.last_login_date), "detail": (first_login[c.id] + timedelta(hours=5, minutes=30)).strftime("%d %b") if c.id in first_login else ""},
            {"key": "training", "label": "Training updates acknowledged", "ok": bool(my_up) and all(u.id in got for u in my_up) or not my_up,
             "detail": f"{sum(1 for u in my_up if u.id in got)} of {len(my_up)}" if my_up else "No updates to acknowledge"},
            {"key": "orders", "label": f"First {target_orders} orders completed", "ok": len(d) >= target_orders, "detail": f"{len(d)} completed" + (f", avg {round(sum(times) / len(times) / 60, 1)} min" if times else "")},
        ]
        out.append({"id": c.id, "name": c.full_name, "team_lead": c.reporting_manager or "", "doj": c.doj.isoformat(), "days": (today - c.doj).days, "steps": steps,
                    "progress": round(100 * sum(1 for s in steps if s["ok"]) / len(steps)), "first_order": min((x[2] for x in d if x[2]), default=None) and min(x[2] for x in d if x[2]).isoformat()})
    return {"days": days, "target_orders": target_orders, "joiners": out, "no_doj": sum(1 for u in _batch_visible_colleagues(db, current_user) if u.employment_status != "Inactive" and not u.doj)}


def _bell_seen(db: Session, user: models.User, kind: str):
    v = _setting(db, f"bellseen_{user.id}_{kind}", "")
    try:
        return datetime.fromisoformat(v) if v else None
    except ValueError:
        return None


@app.post("/notifications/seen")
def notifications_seen(payload: dict, current_user: models.User = Depends(auth.get_current_user), db: Session = Depends(get_db)):
    """Mark 'reply / decision' notifications as read so they stop counting on the bell."""
    for k in payload.get("kinds") or []:
        if k in ("query", "request"):
            _set_setting(db, f"bellseen_{current_user.id}_{k}", datetime.utcnow().isoformat())
    db.commit()
    return {"ok": True}


@app.get("/notifications")
def notifications(current_user: models.User = Depends(auth.get_current_user), db: Session = Depends(get_db)):
    """Everything that needs this user's attention, grouped. 'go' tells the screen where to open."""
    role = current_user.role
    items = []
    def add(kind, title, count, detail="", go=""):
        if count:
            items.append({"kind": kind, "title": title, "count": count, "detail": detail, "go": go})
    now = datetime.utcnow()
    if role == "colleague":
        pids = [p.id for p in current_user.processes]
        if pids:
            req = db.query(models.ProcessUpdate).filter(models.ProcessUpdate.process_id.in_(pids), models.ProcessUpdate.ack_required.is_(True), models.ProcessUpdate.status == "Active",
                                                        models.ProcessUpdate.created_at >= now - timedelta(days=90)).all()
            got = {a for (a,) in db.query(models.ProcessUpdateAck.update_id).filter(models.ProcessUpdateAck.user_id == current_user.id).all()}
            pend = [u for u in req if u.id not in got]
            if pend:
                rem = db.query(models.UpdateReminder).filter(models.UpdateReminder.user_id == current_user.id, models.UpdateReminder.update_id.in_([u.id for u in pend])).order_by(models.UpdateReminder.sent_at.desc()).first()
                add("ack", "Process updates waiting for your acknowledgement", len(pend), (f"Reminder from {rem.sent_by_name}. " if rem else "") + (pend[0].message or "")[:80], "home")
        since = now - timedelta(days=7)
        sq = max(since, _bell_seen(db, current_user, "query") or since)
        sr = max(since, _bell_seen(db, current_user, "request") or since)
        add("query", "Replies to your questions", db.query(models.BreakQuery).filter(models.BreakQuery.user_id == current_user.id, models.BreakQuery.status == "Resolved", models.BreakQuery.replied_at > sq).count(), "New since you last looked", "breaks")
        add("request", "Decisions on your break requests", db.query(models.BreakRequest).filter(models.BreakRequest.user_id == current_user.id, models.BreakRequest.status.in_(["Approved", "Rejected"]), models.BreakRequest.decided_at > sr).count(), "New since you last looked", "breaks")
    elif role in ("team_lead", "admin", "manager", "super_admin"):
        ids = [u.id for u in _batch_visible_colleagues(db, current_user)]
        add("request", "Break requests waiting for your decision", db.query(models.BreakRequest).filter(models.BreakRequest.user_id.in_(ids or [-1]), models.BreakRequest.status == "Pending").count(), "Approve or reject", "requests")
        add("query", "Colleague questions waiting for a reply", db.query(models.BreakQuery).filter(models.BreakQuery.user_id.in_(ids or [-1]), models.BreakQuery.status == "Open").count(), "About adjustments, leave or shift changes", "queries")
        W = models.WorkOrder
        scoped = _orders_dash_scope(db, current_user, None)
        esc = scoped(db.query(W).filter(or_(W.escalated.is_(True), W.onshore_status.in_(("red", "yellow"))), W.submitted.is_(False))).all()
        mine = [o for o in esc if "Team Lead" in _holders(o)]
        add("escalation", "Escalations waiting for a Team Lead", len(mine), "Clarification / onshore reply needs your action", "escalations")
        try:
            od = _esc_overdue_rows(db, _esc_open_orders(db, scoped), _esc_rem_settings(db)) if _esc_rem_settings(db)["enabled"] else []
            add("escalation", "Escalations waiting too long with Onshore / Recon / Calling", len(od), "Past the time limit - send a reminder", "aging")
        except Exception as e:
            print("[bell] overdue:", e)
        if role == "super_admin" and _sqlite_path() and _list_backups():
            rs = _restore_state(db)
            if rs["last"] and not rs["last"].get("ok"):
                add("backup", "Backup restore test FAILED", 1, "Open Admin, Backup to see why", "backup")
            elif rs["due"]:
                add("backup", "Backup restore test is due", 1, "Never tested" if not rs["last"] else f"Last tested {rs['days_since']} days ago", "backup")
        un = [r for r in _unack_scan(db, current_user) if r["pending"]]
        add("ack", "Process update acknowledgements pending", sum(len(r["pending"]) for r in un), f"{len(un)} update(s) - send reminders", "unack")
        warn, late = _sla_days(db)
        today = datetime.now(IST).date()
        cache = {}
        late_n = sum(1 for (rd,) in scoped(db.query(W.received_date).filter(W.received_date.isnot(None), W.submitted.is_(False), func.coalesce(W.posting_status, "") != "Completed")).all() if _order_age(rd, today, cache) >= late)
        add("aging", "Late orders", late_n, f"{late}+ business days old", "aging")
        if role == "super_admin" and _sqlite_path():
            items_b = _list_backups()
            last = datetime.fromisoformat(items_b[0]["at"]) if items_b else None
            days = (datetime.now(IST).replace(tzinfo=None) - last).days if last else None
            if days is None or days >= 7:
                add("backup", "Database backup overdue", 1, "No backup for a week" if days is not None else "No backup made yet", "backup")
    return {"total": sum(i["count"] for i in items), "items": items}


# ======================================================================
# Data clean-up: archive old orders (Super Admin)
# ======================================================================
def _archive_dir() -> str:
    p = _sqlite_path()
    base = os.path.dirname(p) if p else os.getcwd()
    d = os.path.join(base, "archives")
    os.makedirs(d, exist_ok=True)
    return d


def _archive_query(db: Session, months: int, process_id: Optional[int]):
    W = models.WorkOrder
    cutoff = datetime.utcnow() - timedelta(days=30 * months)
    q = db.query(W).filter(W.submitted.is_(True), W.submitted_at.isnot(None), W.submitted_at < cutoff)
    if process_id is not None:
        q = q.filter(W.process_id == process_id)
    return q, cutoff


def _check_months(months: int) -> int:
    if months < 3 or months > 120:
        raise HTTPException(status_code=400, detail="Archive orders older than 3 to 120 months (nothing newer than 3 months is ever archived)")
    return months


def _archive_execute(db: Session, actor, months: int, pid: Optional[int], orders: list, auto: bool = False) -> dict:
    """Safety backup, CSV file, then delete. Nothing is deleted unless both the backup and the file succeed."""
    W = models.WorkOrder
    backup = None
    if _sqlite_path():
        try:
            backup = _make_backup("pre")["name"]
        except Exception as e:
            raise HTTPException(status_code=500, detail="Could not take the safety backup, so nothing was deleted: " + str(e))
    name = f"archive_{datetime.now(IST).strftime('%Y%m%d_%H%M%S')}_{len(orders)}orders.csv"
    path = os.path.join(_archive_dir(), name)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        f.write(_archive_csv(db, orders))
    if not os.path.exists(path) or os.path.getsize(path) < 20:
        raise HTTPException(status_code=500, detail="Could not save the archive file, so nothing was deleted")
    ids = [o.id for o in orders]
    for i in range(0, len(ids), 500):
        chunk = ids[i:i + 500]
        db.query(models.Posting835Line).filter(models.Posting835Line.order_id.in_(chunk)).delete(synchronize_session=False)
        db.query(models.OnshoreAttachment).filter(models.OnshoreAttachment.order_id.in_(chunk)).delete(synchronize_session=False)
        db.query(models.OnshoreMessage).filter(models.OnshoreMessage.order_id.in_(chunk)).delete(synchronize_session=False)
        db.query(models.ClarificationDetail).filter(models.ClarificationDetail.order_id.in_(chunk)).delete(synchronize_session=False)
        db.query(models.EscalationDetail).filter(models.EscalationDetail.order_id.in_(chunk)).delete(synchronize_session=False)
        db.query(W).filter(W.id.in_(chunk)).delete(synchronize_session=False)
    _audit(db, actor, "Archive", ("Auto-archived old orders" if auto else "Archived and deleted old orders"), None, None,
           f"{len(ids)} order(s) older than {months} months" + (f" (one process)" if pid else " (all processes)") + f"; file {name}" + (f"; backup {backup}" if backup else ""))
    db.commit()
    if _sqlite_path():
        try:
            with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                conn.exec_driver_sql("VACUUM")
        except Exception as e:
            print("[archive] vacuum skipped:", e)
    return {"archived": len(ids), "file": name, "backup": backup}


ARCH_KEYS = {"auto": "archive_auto", "months": "archive_auto_months", "last": "archive_auto_last", "result": "archive_auto_result"}


def _arch_settings(db: Session) -> dict:
    try:
        months = int(_setting(db, ARCH_KEYS["months"], "12"))
    except ValueError:
        months = 12
    return {"auto": _setting(db, ARCH_KEYS["auto"], "0") == "1", "months": max(3, min(120, months)),
            "last": _setting(db, ARCH_KEYS["last"], ""), "result": _setting(db, ARCH_KEYS["result"], ""),
            "supported": bool(_sqlite_path())}


def _auto_archive(force: bool = False):
    """Weekly, leader only. Same steps as the manual archive (backup, CSV, delete); never newer than 3 months."""
    db = SessionLocal()
    try:
        st = _arch_settings(db)
        if (not st["auto"] and not force) or not st["supported"]:
            return
        today = datetime.now(IST).date().isoformat()
        if st["last"] == today:
            return
        _set_setting(db, ARCH_KEYS["last"], today); db.commit()
        q, _c = _archive_query(db, st["months"], None)
        orders = q.order_by(models.WorkOrder.id).all()
        if not orders:
            _set_setting(db, ARCH_KEYS["result"], f"{today}: nothing older than {st['months']} months"); db.commit()
            return
        try:
            res = _archive_execute(db, None, st["months"], None, orders, auto=True)
            msg = f"{today}: archived {res['archived']} orders to {res['file']}"
        except Exception as e:
            db.rollback()
            msg = f"{today}: FAILED, nothing deleted ({getattr(e, 'detail', e)})"
            logging.getLogger("workflowhub").error("auto archive failed: %s", msg)
        _set_setting(db, ARCH_KEYS["result"], msg); db.commit()
    finally:
        db.close()


@app.get("/system/health")
def system_health(current_user: models.User = Depends(auth.require_role("super_admin")), db: Session = Depends(get_db)):
    """One screen for the Super Admin: is anything going wrong? Problems are listed first, in plain words."""
    issues = []
    now = datetime.now(IST)
    p = _sqlite_path()
    # database
    dbinfo = {"kind": "SQLite file" if p else "PostgreSQL", "size": None, "free_disk": None}
    try:
        if p:
            dbinfo["size"] = os.path.getsize(p) + (os.path.getsize(p + "-wal") if os.path.exists(p + "-wal") else 0)
            dbinfo["free_disk"] = shutil.disk_usage(os.path.dirname(p)).free
            if dbinfo["free_disk"] < 1024 ** 3:
                issues.append(("red", "Disk space is low (under 1 GB free)"))
        else:
            dbinfo["size"] = db.execute(sa_text("SELECT pg_database_size(current_database())")).scalar()
    except Exception:
        pass
    t0 = time.perf_counter()
    db.execute(sa_text("SELECT 1"))
    dbinfo["ping_ms"] = round((time.perf_counter() - t0) * 1000, 1)
    M = models
    rows = {"Orders": db.query(func.count(M.WorkOrder.id)).scalar() or 0, "Users": db.query(func.count(M.User.id)).scalar() or 0,
            "Activity log entries": db.query(func.count(M.AuditEvent.id)).scalar() or 0, "Quality errors": db.query(func.count(M.QualityError.id)).scalar() or 0,
            "835 posting lines": db.query(func.count(M.Posting835Line.id)).scalar() or 0}
    cutoff = datetime.utcnow() - timedelta(minutes=5)
    online = db.query(func.count(func.distinct(M.SessionLog.user_id))).filter(M.SessionLog.logout_at.is_(None), M.SessionLog.last_seen >= cutoff).scalar() or 0
    old = db.query(func.count(M.WorkOrder.id)).filter(M.WorkOrder.submitted.is_(True), M.WorkOrder.submitted_at < datetime.utcnow() - timedelta(days=365)).scalar() or 0
    if old > 5000:
        issues.append(("amber", f"{old} submitted orders are over 12 months old - consider archiving (Admin > Data clean-up)"))
    # backups
    bk = {"supported": bool(p)}
    if p:
        items = _list_backups()
        last = datetime.fromisoformat(items[0]["at"]) if items else None
        days = (now.replace(tzinfo=None) - last).days if last else None
        rs = _restore_state(db)
        bk.update({"last": items[0]["at"] if items else None, "days_since": days, "count": len(items), "auto": _backup_settings(db)["auto"],
                   "restore_ok": (rs["last"] or {}).get("ok"), "restore_at": (rs["last"] or {}).get("at"), "restore_due": rs["due"]})
        if not items:
            issues.append(("red", "No backup has ever been made"))
        elif days is not None and days >= 2:
            issues.append(("red" if days >= 7 else "amber", f"Last backup was {days} days ago"))
        if rs["last"] and not rs["last"].get("ok"):
            issues.append(("red", "The last restore test FAILED - a backup may not be usable"))
        elif rs["due"]:
            issues.append(("amber", "A restore test is due (Admin > Backup)"))
    # email
    em = {"configured": email_utils.is_configured(), "last_ok": email_utils.last_result.get("ok_at"), "last_try": email_utils.last_result.get("at"),
          "last_ok_flag": email_utils.last_result.get("ok"), "error": email_utils.last_result.get("error", "")}
    if not em["configured"]:
        issues.append(("amber", "Email (SMTP) is not set up - no reminders or summaries will be sent"))
    elif em["last_ok_flag"] is False:
        issues.append(("red", "The last email failed: " + (em["error"] or "unknown reason")))
    # errors today / yesterday
    er = {"today": 0, "yesterday": 0, "warnings_today": 0, "last": None}
    try:
        for i, day in enumerate((now.strftime("%Y-%m-%d"), (now - timedelta(days=1)).strftime("%Y-%m-%d"))):
            ents = _read_log_entries(day)
            n = [e for e in ents if e["level"] == "ERROR"]
            er["today" if i == 0 else "yesterday"] = len(n)
            if i == 0:
                er["warnings_today"] = len([e for e in ents if e["level"] == "WARNING"])
                if n:
                    er["last"] = {"at": n[-1]["at"], "text": n[-1]["text"].split("\n")[0][:240]}
    except Exception:
        pass
    if er["today"]:
        issues.append(("red" if er["today"] >= 5 else "amber", f"{er['today']} error(s) logged today (see System log)"))
    # slowest endpoints (this server process since it started)
    eps = []
    with _EP_LOCK:
        for k, e in _EP.items():
            r = sorted(e["recent"])
            if not r:
                continue
            eps.append({"endpoint": k, "calls": e["n"], "avg_ms": round(e["total"] / e["n"]), "p95_ms": round(r[min(len(r) - 1, int(len(r) * 0.95))]),
                        "max_ms": round(e["max"]), "errors": e["errors"]})
    eps.sort(key=lambda x: x["p95_ms"], reverse=True)
    slow = [e for e in eps if e["calls"] >= 3 and e["p95_ms"] >= 1500]
    if slow:
        issues.append(("amber", f"{len(slow)} endpoint(s) are slow (95% of calls over 1.5 s): " + ", ".join(e["endpoint"] for e in slow[:3])))
    ar = _arch_settings(db)
    if ar["result"] and "FAILED" in ar["result"]:
        issues.append(("red", "Automatic archiving failed: " + ar["result"]))
    rank = {"red": 0, "amber": 1}
    issues.sort(key=lambda x: rank[x[0]])
    status = "red" if any(i[0] == "red" for i in issues) else ("amber" if issues else "green")
    return {"status": status, "issues": [{"level": a, "text": b} for a, b in issues], "time": now.strftime("%Y-%m-%d %H:%M:%S"),
            "uptime_hours": round((now - _STARTED).total_seconds() / 3600, 1), "started": _STARTED.strftime("%Y-%m-%d %H:%M"),
            "online_now": online, "database": dbinfo, "rows": rows, "backup": bk, "email": em, "errors": er,
            "slowest": eps[:12], "archive": {"auto": ar["auto"], "result": ar["result"], "supported": ar["supported"]},
            "note": "Endpoint timings are for this server process since it started (they reset on restart)."}


@app.get("/archive/auto")
def archive_auto_get(current_user: models.User = Depends(auth.require_role("super_admin")), db: Session = Depends(get_db)):
    return _arch_settings(db)


@app.put("/archive/auto")
def archive_auto_put(payload: dict, current_user: models.User = Depends(auth.require_role("super_admin")), db: Session = Depends(get_db)):
    if not _sqlite_path():
        raise HTTPException(status_code=400, detail="Automatic archiving needs the laptop (SQLite) version, because the archive file must be kept on disk")
    try:
        months = _check_months(int(payload.get("months", 12)))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid months")
    on = bool(payload.get("auto"))
    _set_setting(db, ARCH_KEYS["auto"], "1" if on else "0")
    _set_setting(db, ARCH_KEYS["months"], str(months))
    _audit(db, current_user, "Archive", "Automatic archiving " + ("turned on" if on else "turned off"), None, None, f"older than {months} months, Sundays 02:00 IST")
    db.commit()
    return _arch_settings(db)


@app.post("/archive/auto/run-now")
def archive_auto_run_now(current_user: models.User = Depends(auth.require_role("super_admin")), db: Session = Depends(get_db)):
    _set_setting(db, ARCH_KEYS["last"], ""); db.commit()
    _auto_archive(force=True)
    return _arch_settings(db)


@app.get("/archive/preview")
def archive_preview(months: int = 12, process_id: Optional[int] = None,
                    current_user: models.User = Depends(auth.require_role("super_admin")), db: Session = Depends(get_db)):
    _check_months(months)
    W = models.WorkOrder
    q, cutoff = _archive_query(db, months, process_id)
    ids_q = q.with_entities(W.id)
    total = db.query(func.count(W.id)).filter(W.id.in_(ids_q.subquery())).scalar() or 0
    by = db.query(models.Process.name, func.count(W.id), func.coalesce(func.sum(W.trans_count), 0), func.min(W.submitted_at), func.max(W.submitted_at)).join(
        W, W.process_id == models.Process.id).filter(W.id.in_(ids_q.subquery())).group_by(models.Process.name).order_by(models.Process.name).all()
    all_orders = db.query(func.count(W.id)).scalar() or 0
    p = _sqlite_path()
    return {"months": months, "cutoff": (cutoff + timedelta(hours=5, minutes=30)).date().isoformat(), "count": total, "all_orders": all_orders,
            "by_process": [{"process": n, "orders": c, "trans": int(t or 0), "from": (a + timedelta(hours=5, minutes=30)).date().isoformat() if a else "", "to": (b + timedelta(hours=5, minutes=30)).date().isoformat() if b else ""} for n, c, t, a, b in by],
            "db_size": os.path.getsize(p) if p and os.path.exists(p) else None, "sqlite": bool(p),
            "confirm_text": f"ARCHIVE {total}"}


def _archive_csv(db: Session, orders: list) -> str:
    cols = [c.name for c in models.WorkOrder.__table__.columns]
    procs = {pp.id: pp.name for pp in db.query(models.Process).all()}
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["process"] + cols)
    for o in orders:
        w.writerow([procs.get(o.process_id, "")] + [("" if getattr(o, c) is None else getattr(o, c)) for c in cols])
    return buf.getvalue()


@app.get("/archive/export")
def archive_export(months: int = 12, process_id: Optional[int] = None,
                   current_user: models.User = Depends(auth.require_role("super_admin")), db: Session = Depends(get_db)):
    """Download the orders that would be archived as a CSV (opens in Excel). Nothing is deleted."""
    _check_months(months)
    q, _c = _archive_query(db, months, process_id)
    data = _archive_csv(db, q.order_by(models.WorkOrder.id).all())
    _audit(db, current_user, "Archive", "Downloaded old-orders export", details=f"older than {months} months")
    db.commit()
    return Response(content="﻿" + data, media_type="text/csv; charset=utf-8", headers={"Content-Disposition": f'attachment; filename="old_orders_{months}m.csv"'})


@app.post("/archive/run")
def archive_run(payload: dict, current_user: models.User = Depends(auth.require_role("super_admin")), db: Session = Depends(get_db)):
    """
    Saves the old, already-in-Production orders to a CSV file, takes a database backup (laptop version), then deletes them.
    The caller must type the exact confirmation text returned by /archive/preview (e.g. "ARCHIVE 1234") for the current count.
    """
    try:
        months = int(payload.get("months", 12))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid months")
    _check_months(months)
    pid = payload.get("process_id")
    pid = int(pid) if pid not in (None, "") else None
    W = models.WorkOrder
    q, cutoff = _archive_query(db, months, pid)
    orders = q.order_by(W.id).all()
    if not orders:
        raise HTTPException(status_code=400, detail="There are no orders to archive for that selection")
    expected = f"ARCHIVE {len(orders)}"
    if str(payload.get("confirm", "")).strip() != expected:
        raise HTTPException(status_code=400, detail=f'Confirmation does not match. Type exactly: {expected} (the number of orders can change if people are working - preview again)')
    return _archive_execute(db, current_user, months, pid, orders)


@app.get("/archive/files")
def archive_files(current_user: models.User = Depends(auth.require_role("super_admin"))):
    d = _archive_dir()
    out = []
    for n in os.listdir(d):
        if re.fullmatch(r"archive_\d{8}_\d{6}_\d+orders\.csv", n):
            out.append({"name": n, "size": os.path.getsize(os.path.join(d, n)), "at": datetime.strptime(n[8:23], "%Y%m%d_%H%M%S").isoformat()})
    return {"folder": d, "files": sorted(out, key=lambda x: x["at"], reverse=True)}


@app.get("/archive/download/{name}")
def archive_download(name: str, current_user: models.User = Depends(auth.require_role("super_admin"))):
    from fastapi.responses import FileResponse
    if not re.fullmatch(r"archive_\d{8}_\d{6}_\d+orders\.csv", name or ""):
        raise HTTPException(status_code=404, detail="File not found")
    full = os.path.join(_archive_dir(), name)
    if not os.path.exists(full):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(full, media_type="text/csv", filename=name)


# ======================================================================
# Festival banner for the lock screen (holiday today, or the working day before a holiday)
# ======================================================================
# (key, name pattern, title, wish, motif, colour 1, colour 2) - first match wins. ASCII only (the PC helper reads it as plain text).
_FESTIVALS = [
    ("diwali", r"diwali|deepavali|deepawali|dipavali", "Happy Diwali!", "May the festival of lights fill your home with joy, prosperity and good health.", "lamp", "#7b2d00", "#f26a21"),
    ("karthigai", r"karthigai|karthikai|kartik purnima|dev deepawali", "Happy Karthigai Deepam!", "May the glow of the lamps bring light and happiness to your home.", "lamp", "#4a148c", "#ff8f00"),
    ("pongal", r"pongal|thai pongal|mattu pongal|uzhavar", "Happy Pongal!", "Wishing you a harvest of happiness, prosperity and good health. Pongalo Pongal!", "pot", "#8d4a00", "#f4b400"),
    ("sankranti", r"sankranti|uttarayan|makara", "Happy Makar Sankranti!", "May the sun bring warmth, success and new beginnings.", "sun", "#e65100", "#ffb300"),
    ("lohri", r"lohri|bhogi", "Happy Bhogi / Lohri!", "Wishing you warmth, joy and a prosperous season.", "sun", "#bf360c", "#ff9800"),
    ("tamil_ny", r"tamil new year|puthandu|varusha pirappu|chithirai", "Happy Tamil New Year!", "Iniya Tamil Puthandu Nalvazhthukkal! Wishing you a year of health and happiness.", "flower", "#1b5e20", "#c0a000"),
    ("vishu", r"vishu", "Happy Vishu!", "Wishing you a bright and prosperous year ahead.", "flower", "#f57f17", "#2e7d32"),
    ("ugadi", r"ugadi|gudi padwa|yugadi", "Happy Ugadi!", "Wishing you a new year full of fresh beginnings and happiness.", "flower", "#2e7d32", "#fbc02d"),
    ("newyear", r"new year", "Happy New Year!", "Wishing you a year full of health, happiness and success.", "burst", "#0b2545", "#7b1fa2"),
    ("republic", r"republic day", "Happy Republic Day!", "Proud to be Indian. Wishing you a joyful Republic Day.", "flag", "#e65100", "#1b7f3b"),
    ("independence", r"independence day", "Happy Independence Day!", "Wishing you a proud and joyful Independence Day.", "flag", "#e65100", "#1b7f3b"),
    ("gandhi", r"gandhi jayanti|gandhi", "Gandhi Jayanti", "Remembering Mahatma Gandhi - truth, peace and non-violence.", "star", "#37474f", "#78909c"),
    ("labour", r"labou?r day|may day|workers", "Happy Labour Day!", "Thank you for your hard work and dedication.", "star", "#0d47a1", "#42a5f5"),
    ("holi", r"\bholi\b|dhuleti", "Happy Holi!", "May your life be filled with colours of joy and happiness.", "flower", "#ad1457", "#ffb300"),
    ("navaratri", r"navaratri|navratri|durga", "Happy Navaratri!", "Wishing you nine days of devotion, joy and blessings.", "lamp", "#880e4f", "#ff7043"),
    ("pooja", r"ayudha|saraswati|vijayadashami|dussehra|dasara|dasera", "Happy Dussehra / Ayudha Pooja!", "Wishing you success and prosperity in everything you do.", "lamp", "#6a1b9a", "#ff8f00"),
    ("ganesh", r"ganesh|vinayaka|vinayagar", "Happy Vinayaka Chaturthi!", "May Lord Ganesha bless you and remove all obstacles.", "lamp", "#e65100", "#ffca28"),
    ("janmashtami", r"janmashtami|krishna jayanth|gokulashtami", "Happy Krishna Janmashtami!", "May Lord Krishna's blessings be with you and your family.", "lamp", "#0d47a1", "#4fc3f7"),
    ("shivratri", r"shivaratri|shivratri", "Maha Shivaratri", "Om Namah Shivaya. Wishing you peace and blessings.", "lamp", "#1a237e", "#7986cb"),
    ("rakhi", r"raksha bandhan|rakhi", "Happy Raksha Bandhan!", "Celebrating the bond of love between brothers and sisters.", "flower", "#c2185b", "#f8bbd0"),
    ("onam", r"onam|thiruvonam", "Happy Onam!", "Wishing you a joyful Onam filled with prosperity and happiness.", "flower", "#f9a825", "#2e7d32"),
    ("christmas", r"christmas|xmas", "Merry Christmas!", "Wishing you peace, joy and happiness this Christmas.", "tree", "#1b5e20", "#c62828"),
    ("goodfriday", r"good friday", "Good Friday", "Wishing you a peaceful day of reflection.", "star", "#37474f", "#90a4ae"),
    ("easter", r"easter", "Happy Easter!", "Wishing you a joyful and peaceful Easter.", "flower", "#6a1b9a", "#f48fb1"),
    ("bakrid", r"bakrid|bakri id|eid.?al.?adha|eid.?ul.?adha|id.?ul.?zuha", "Eid Mubarak!", "Wishing you peace, happiness and blessings on Bakrid.", "crescent", "#004d40", "#26a69a"),
    ("eid", r"\beid\b|ramzan|ramadan|id.?ul.?fitr|milad|muharram", "Eid Mubarak!", "Wishing you peace, happiness and prosperity.", "crescent", "#004d40", "#26a69a"),
    ("thanksgiving", r"thanksgiving", "Happy Thanksgiving!", "Grateful for you and your work. Wishing you a warm Thanksgiving.", "sun", "#bf360c", "#ff8f00"),
    ("memorial", r"memorial day", "Memorial Day", "Remembering and honouring those who served.", "flag", "#b71c1c", "#0d47a1"),
    ("juneteenth", r"juneteenth", "Juneteenth", "Celebrating freedom and reflection.", "star", "#4e342e", "#a1887f"),
]


def _festival_for(name: str, d: date) -> dict:
    n = name or ""
    if re.search(r"independence day", n, re.I) and d.month == 7:
        return {"key": "independence_us", "title": "Happy Independence Day!", "message": "Wishing you a safe and joyful Fourth of July.", "motif": "burst", "c1": "#b71c1c", "c2": "#0d47a1"}
    if re.search(r"labor day", n, re.I) and d.month == 9:
        return {"key": "laborday", "title": "Happy Labor Day!", "message": "Thank you for your hard work. Enjoy your well-earned rest.", "motif": "star", "c1": "#0d47a1", "c2": "#42a5f5"}
    for key, pat, title, msg, motif, c1, c2 in _FESTIVALS:
        if re.search(pat, n, re.I):
            return {"key": key, "title": title, "message": msg, "motif": motif, "c1": c1, "c2": c2}
    return {"key": "holiday", "title": "Happy Holiday!", "message": "Enjoy your day off and come back refreshed.", "motif": "star", "c1": "#0b2545", "c2": "#1c7ed6"}


def _banner_for_day(db: Session, today: date):
    """The festival banner for the lock screen: today if it is a company holiday, or on the last working day before a holiday."""
    hol = {h.holiday_date: h.name for h in db.query(models.Holiday).filter(models.Holiday.holiday_date >= today, models.Holiday.holiday_date <= today + timedelta(days=12)).all()}
    pick, when = None, ""
    if today in hol:
        pick, when = today, "today"
    elif not _is_off_day(today, set(hol)):
        d = today + timedelta(days=1)
        run = []
        while _is_off_day(d, set(hol)) and len(run) < 10:
            run.append(d)
            d += timedelta(days=1)
        first = next((x for x in run if x in hol), None)
        if first:
            pick, when = first, ("tomorrow" if first == today + timedelta(days=1) else "ahead")
    if not pick:
        return None
    b = _festival_for(hol[pick], pick)
    b["holiday"] = hol[pick]
    b["date"] = pick.isoformat()
    b["when"] = when
    b["note"] = ("Office holiday today" if when == "today" else "Holiday tomorrow - enjoy the break" if when == "tomorrow" else "Holiday on " + pick.strftime("%a %d %b") + " - enjoy the break")
    return b


@app.get("/breaks/banner")
def lock_banner(current_user: models.User = Depends(auth.get_current_user), db: Session = Depends(get_db)):
    return {"banner": _banner_for_day(db, datetime.now(IST).date())}


# ---------------------------------------------------------------------------
# Error tracking & rework (feeds the Quality part of Performance)
# ---------------------------------------------------------------------------
QERR_LOG_ROLES = ("team_lead", "admin", "manager", "quality", "super_admin")
QERR_VIEW_ROLES = ("colleague", "team_lead", "admin", "manager", "quality", "super_admin")
QERR_DEFAULT_TYPES = ["Wrong amount", "Wrong account / patient", "Wrong payment code", "Missed posting",
                      "Duplicate posting", "Wrong adjustment", "Not per guidelines", "Missing note / comment", "Other"]
QERR_SEVERITIES = ("Minor", "Major", "Critical")


def _qerr_types(db: Session) -> list:
    try:
        v = json.loads(_setting(db, "qerr_types", "") or "[]")
        if isinstance(v, list) and v:
            return [str(x) for x in v]
    except ValueError:
        pass
    return list(QERR_DEFAULT_TYPES)


def _qerr_scope_ids(db: Session, user: models.User) -> list:
    return [u.id for u in _batch_visible_colleagues(db, user)]


def _qerr_row(e: models.QualityError, names: dict, pnames: dict) -> dict:
    return {
        "id": e.id, "user_id": e.user_id, "colleague": names.get(e.user_id, "?"),
        "process_id": e.process_id, "process": pnames.get(e.process_id, ""),
        "order_id": e.order_id, "order_ref": e.order_ref or "", "error_date": e.error_date.isoformat() if e.error_date else None,
        "error_type": e.error_type, "severity": e.severity or "", "notes": e.notes or "",
        "rework_count": e.rework_count or 0, "rework_done": bool(e.rework_done),
        "logged_by": e.logged_by_name or "",
    }


@app.get("/quality/error-types")
def quality_error_types(current_user: models.User = Depends(auth.require_role(*QERR_VIEW_ROLES)), db: Session = Depends(get_db)):
    return {"types": _qerr_types(db), "severities": list(QERR_SEVERITIES)}


@app.put("/quality/error-types")
def quality_set_error_types(payload: dict, current_user: models.User = Depends(auth.require_role("admin", "manager", "super_admin")),
                            db: Session = Depends(get_db)):
    types = []
    for t in payload.get("types") or []:
        t = " ".join(str(t).split())[:80]
        if t and t not in types:
            types.append(t)
    if not types:
        raise HTTPException(status_code=400, detail="Keep at least one error type")
    _set_setting(db, "qerr_types", json.dumps(types))
    _audit(db, current_user, "Quality", "Changed error types", None, None, ", ".join(types)[:300])
    db.commit()
    return {"types": types}


@app.get("/quality/orders/lookup")
def quality_order_lookup(q: str, current_user: models.User = Depends(auth.require_role(*QERR_LOG_ROLES)), db: Session = Depends(get_db)):
    """Find an order by EDM / batch text so an error can be linked to it."""
    q = (q or "").strip()
    if len(q) < 2:
        return []
    query = db.query(models.WorkOrder).filter(models.WorkOrder.edm.ilike("%" + q + "%"))
    if current_user.role != "super_admin":
        pids = [p.id for p in current_user.processes]
        query = query.filter(models.WorkOrder.process_id.in_(pids or [0]))
    pn = {p.id: p.name for p in db.query(models.Process).all()}
    un = {u.id: u.full_name for u in db.query(models.User.id, models.User.full_name).all()}
    return [{"id": o.id, "edm": o.edm, "process_id": o.process_id, "process": pn.get(o.process_id, ""),
             "user_id": o.assigned_to_id, "colleague": un.get(o.assigned_to_id) or o.employee_name or "",
             "date": (o.posted_date or o.received_date).isoformat() if (o.posted_date or o.received_date) else None}
            for o in query.order_by(models.WorkOrder.id.desc()).limit(15).all()]


@app.get("/quality/errors/summary")
def quality_errors_summary(start_date: Optional[date] = None, end_date: Optional[date] = None, process_id: Optional[int] = None,
                           current_user: models.User = Depends(auth.require_role(*QERR_VIEW_ROLES)), db: Session = Depends(get_db)):
    ids = _qerr_scope_ids(db, current_user)
    q = db.query(models.QualityError).filter(models.QualityError.user_id.in_(ids or [0]))
    if start_date:
        q = q.filter(models.QualityError.error_date >= start_date)
    if end_date:
        q = q.filter(models.QualityError.error_date <= end_date)
    if process_id:
        q = q.filter(models.QualityError.process_id == process_id)
    rows = q.all()
    names = {u.id: u.full_name for u in db.query(models.User.id, models.User.full_name).filter(models.User.id.in_(ids or [0])).all()}
    by_type, by_col, by_proc = {}, {}, {}
    pn = {p.id: p.name for p in db.query(models.Process).all()}
    for e in rows:
        t = by_type.setdefault(e.error_type, {"type": e.error_type, "errors": 0, "rework": 0})
        t["errors"] += 1; t["rework"] += e.rework_count or 0
        c = by_col.setdefault(e.user_id, {"user_id": e.user_id, "colleague": names.get(e.user_id, "?"), "errors": 0, "rework": 0,
                                           "rework_open": 0, "critical": 0, "types": {}})
        c["errors"] += 1; c["rework"] += e.rework_count or 0
        if (e.rework_count or 0) > 0 and not e.rework_done:
            c["rework_open"] += 1
        if e.severity == "Critical":
            c["critical"] += 1
        c["types"][e.error_type] = c["types"].get(e.error_type, 0) + 1
        p = by_proc.setdefault(e.process_id, {"process": pn.get(e.process_id, "-"), "errors": 0})
        p["errors"] += 1
    # accounts audited in the same window (from the Team Lead's batch entries), to give an error rate
    aq = db.query(models.DailyBatchStat).filter(models.DailyBatchStat.user_id.in_(ids or [0]))
    if start_date:
        aq = aq.filter(models.DailyBatchStat.work_date >= start_date)
    if end_date:
        aq = aq.filter(models.DailyBatchStat.work_date <= end_date)
    if process_id:
        aq = aq.filter(models.DailyBatchStat.process_id == process_id)
    audited, entered = {}, {}
    for s_ in aq.all():
        audited[s_.user_id] = audited.get(s_.user_id, 0) + (s_.accounts_audited or 0)
        entered[s_.user_id] = entered.get(s_.user_id, 0) + (s_.errors or 0)
    cols = []
    for c in by_col.values():
        c["top_type"] = max(c["types"].items(), key=lambda kv: kv[1])[0] if c["types"] else ""
        c["audited"] = audited.get(c["user_id"], 0)
        c["entered_errors"] = entered.get(c["user_id"], 0)
        c["error_rate"] = round(c["errors"] * 100 / c["audited"], 2) if c["audited"] else None
        # logged detail vs the figure the Team Lead entered in the Batch dashboard
        c["gap"] = c["entered_errors"] - c["errors"]
        del c["types"]
        cols.append(c)
    cols.sort(key=lambda c: (-c["errors"], c["colleague"]))
    return {
        "total_errors": len(rows), "total_rework": sum(e.rework_count or 0 for e in rows),
        "rework_open": sum(1 for e in rows if (e.rework_count or 0) > 0 and not e.rework_done),
        "by_type": sorted(by_type.values(), key=lambda t: -t["errors"]),
        "by_colleague": cols, "by_process": sorted(by_proc.values(), key=lambda p: -p["errors"]),
    }


@app.get("/quality/errors")
def quality_errors_list(start_date: Optional[date] = None, end_date: Optional[date] = None, user_id: Optional[int] = None,
                        process_id: Optional[int] = None, error_type: Optional[str] = None, rework_open: bool = False,
                        limit: int = 200, offset: int = 0, response: Response = None,
                        current_user: models.User = Depends(auth.require_role(*QERR_VIEW_ROLES)), db: Session = Depends(get_db)):
    ids = _qerr_scope_ids(db, current_user)
    q = db.query(models.QualityError).filter(models.QualityError.user_id.in_(ids or [0]))
    if user_id:
        q = q.filter(models.QualityError.user_id == user_id)
    if start_date:
        q = q.filter(models.QualityError.error_date >= start_date)
    if end_date:
        q = q.filter(models.QualityError.error_date <= end_date)
    if process_id:
        q = q.filter(models.QualityError.process_id == process_id)
    if error_type:
        q = q.filter(models.QualityError.error_type == error_type)
    if rework_open:
        q = q.filter(models.QualityError.rework_count > 0, models.QualityError.rework_done == False)  # noqa: E712
    total = q.order_by(None).count()
    rows = q.order_by(models.QualityError.error_date.desc(), models.QualityError.id.desc()).offset(max(0, offset)).limit(max(1, min(limit, 500))).all()
    if response is not None:
        response.headers["X-Total-Count"] = str(total)
    names = {u.id: u.full_name for u in db.query(models.User.id, models.User.full_name).all()}
    pn = {p.id: p.name for p in db.query(models.Process).all()}
    return [_qerr_row(e, names, pn) for e in rows]


def _qerr_apply(e: models.QualityError, payload: dict, db: Session, user: models.User, creating: bool):
    types = _qerr_types(db)
    if "error_type" in payload or creating:
        t = " ".join(str(payload.get("error_type") or "").split())
        if not t:
            raise HTTPException(status_code=400, detail="Choose an error type")
        if t not in types:
            raise HTTPException(status_code=400, detail="Unknown error type")
        e.error_type = t
    if "severity" in payload:
        sv = payload.get("severity") or None
        if sv and sv not in QERR_SEVERITIES:
            raise HTTPException(status_code=400, detail="Severity must be Minor, Major or Critical")
        e.severity = sv
    if "notes" in payload:
        e.notes = (str(payload.get("notes") or "").strip()[:1000]) or None
    if "rework_count" in payload:
        try:
            n = int(payload.get("rework_count") or 0)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="Rework count must be a whole number")
        if not (0 <= n <= 50):
            raise HTTPException(status_code=400, detail="Rework count must be between 0 and 50")
        e.rework_count = n
    if "rework_done" in payload:
        e.rework_done = bool(payload.get("rework_done"))
    if "error_date" in payload and payload.get("error_date"):
        try:
            d = date.fromisoformat(str(payload["error_date"])[:10])
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date")
        if d > _work_today():
            raise HTTPException(status_code=400, detail="Error date cannot be in the future")
        e.error_date = d


@app.post("/quality/errors")
def quality_error_create(payload: dict, current_user: models.User = Depends(auth.require_role(*QERR_LOG_ROLES)), db: Session = Depends(get_db)):
    order = None
    if payload.get("order_id"):
        order = db.query(models.WorkOrder).filter(models.WorkOrder.id == int(payload["order_id"])).first()
        if not order:
            raise HTTPException(status_code=404, detail="Order not found")
        _require_process_access(current_user, order.process_id)
    uid = payload.get("user_id") or (order.assigned_to_id if order else None)
    if not uid:
        raise HTTPException(status_code=400, detail="Choose the colleague who made the error")
    if int(uid) not in _qerr_scope_ids(db, current_user):
        raise HTTPException(status_code=403, detail="That colleague is not in your team")
    pid = order.process_id if order else payload.get("process_id")
    if pid:
        _require_process_access(current_user, int(pid))
    e = models.QualityError(user_id=int(uid), process_id=int(pid) if pid else None,
                            order_id=order.id if order else None, order_ref=(order.edm if order else (str(payload.get("order_ref") or "").strip()[:120] or None)),
                            error_date=_work_today(), logged_by_id=current_user.id, logged_by_name=current_user.full_name, rework_count=0, rework_done=False)
    _qerr_apply(e, payload, db, current_user, True)
    db.add(e)
    _audit(db, current_user, "Quality", "Logged an error", db.query(models.User).filter(models.User.id == e.user_id).first(), e.error_date,
           f"{e.error_type}" + (f", order {e.order_ref}" if e.order_ref else "") + f", rework {e.rework_count}")
    db.commit()
    return {"id": e.id}


def _qerr_get(db: Session, user: models.User, eid: int) -> models.QualityError:
    e = db.query(models.QualityError).filter(models.QualityError.id == eid).first()
    if not e:
        raise HTTPException(status_code=404, detail="Not found")
    if e.user_id not in _qerr_scope_ids(db, user):
        raise HTTPException(status_code=403, detail="Not allowed")
    return e


@app.patch("/quality/errors/{eid}")
def quality_error_update(eid: int, payload: dict, current_user: models.User = Depends(auth.require_role(*QERR_LOG_ROLES)), db: Session = Depends(get_db)):
    e = _qerr_get(db, current_user, eid)
    _qerr_apply(e, payload, db, current_user, False)
    e.updated_at = datetime.utcnow()
    _audit(db, current_user, "Quality", "Updated an error", None, e.error_date, f"#{e.id} {e.error_type}, rework {e.rework_count}{' (done)' if e.rework_done else ''}")
    db.commit()
    return {"ok": True}


@app.delete("/quality/errors/{eid}")
def quality_error_delete(eid: int, current_user: models.User = Depends(auth.require_role(*QERR_LOG_ROLES)), db: Session = Depends(get_db)):
    e = _qerr_get(db, current_user, eid)
    _audit(db, current_user, "Quality", "Deleted an error entry", None, e.error_date, f"#{e.id} {e.error_type}, order {e.order_ref or '-'}")
    db.delete(e)
    db.commit()
    return {"deleted": 1}


# ---------------------------------------------------------------------------
# Forecast against daily targets (today's pace + projected month-end)
# ---------------------------------------------------------------------------
FC_ROLES = ("team_lead", "admin", "manager", "super_admin")


def _fc_shift(db: Session):
    start = (_setting(db, "forecast_shift_start", "09:00") or "09:00").strip()
    try:
        hh, mm = [int(x) for x in start.split(":")[:2]]
        assert 0 <= hh < 24 and 0 <= mm < 60
    except Exception:
        hh, mm, start = 9, 0, "09:00"
    try:
        hours = float(_setting(db, "forecast_shift_hours", "") or STANDARD_SHIFT_HOURS)
        assert 1 <= hours <= 16
    except Exception:
        hours = STANDARD_SHIFT_HOURS
    return start, hh, mm, hours


@app.put("/settings/forecast")
def set_forecast_settings(payload: dict, current_user: models.User = Depends(auth.require_role("admin", "manager", "super_admin")),
                          db: Session = Depends(get_db)):
    start = str(payload.get("start") or "").strip()
    try:
        hh, mm = [int(x) for x in start.split(":")[:2]]
        assert 0 <= hh < 24 and 0 <= mm < 60
        hours = float(payload.get("hours"))
        assert 1 <= hours <= 16
    except Exception:
        raise HTTPException(status_code=400, detail="Enter the shift start as HH:MM and the shift length as hours (1-16)")
    _set_setting(db, "forecast_shift_start", "%02d:%02d" % (hh, mm))
    _set_setting(db, "forecast_shift_hours", str(hours))
    _audit(db, current_user, "Settings", "Changed forecast shift", None, None, f"start {hh:02d}:{mm:02d}, {hours} h")
    db.commit()
    return {"ok": True}


def _fc_workdays(d1: date, d2: date, hol: set) -> list:
    out, d = [], d1
    while d <= d2:
        if not _is_off_day(d, hol):
            out.append(d)
        d += timedelta(days=1)
    return out


@app.get("/forecast")
def forecast_view(process_id: Optional[int] = None,
                  current_user: models.User = Depends(auth.require_role(*FC_ROLES)), db: Session = Depends(get_db)):
    """Per process: is the team on pace today against the daily target, and where is the month heading."""
    today = _work_today()
    now_ist = datetime.now(IST).replace(tzinfo=None)
    start_txt, hh, mm, shift_hours = _fc_shift(db)
    shift_start = datetime(today.year, today.month, today.day, hh, mm)
    frac = max(0.0, min(1.0, (now_ist - shift_start).total_seconds() / 3600.0 / shift_hours))
    hours_left = max(0.0, shift_hours * (1 - frac))
    m1 = today.replace(day=1)
    m2 = (date(today.year + (today.month == 12), (today.month % 12) + 1, 1) - timedelta(days=1))
    hol = _hol_set(db, m1, m2)
    work_all = _fc_workdays(m1, m2, hol)
    past = [d for d in work_all if d < today]
    future = [d for d in work_all if d > today]
    today_is_work = not _is_off_day(today, hol)

    colleagues = [u for u in _batch_visible_colleagues(db, current_user) if u.employment_status != "Inactive"]
    all_access = current_user.role in ("admin", "manager", "super_admin")
    procs = db.query(models.Process).order_by(models.Process.name).all()
    if not all_access:
        mine = {p.id for p in current_user.processes}
        procs = [p for p in procs if p.id in mine]
    if process_id:
        procs = [p for p in procs if p.id == process_id]

    cids = [u.id for u in colleagues]
    login = {}
    for uid, d in db.query(models.LoginDay.user_id, models.LoginDay.login_date).filter(
            models.LoginDay.user_id.in_(cids or [0]), models.LoginDay.login_date >= m1, models.LoginDay.login_date <= m2).all():
        login.setdefault(uid, set()).add(_as_date(d))
    leave = {}
    for uid, d in db.query(models.LeaveDay.user_id, models.LeaveDay.leave_date).filter(
            models.LeaveDay.user_id.in_(cids or [0]), models.LeaveDay.leave_date >= m1, models.LeaveDay.leave_date <= m2).all():
        leave.setdefault(uid, set()).add(_as_date(d))
    done = {}      # (process, user, date) -> trans   (summed in the database)
    for pid_, uid_, pd_, tc_ in db.query(models.WorkOrder.process_id, models.WorkOrder.assigned_to_id, models.WorkOrder.posted_date,
                                         func.coalesce(func.sum(models.WorkOrder.trans_count), 0)).filter(
            models.WorkOrder.posting_status == "Completed", models.WorkOrder.posted_date >= m1, models.WorkOrder.posted_date <= m2,
            models.WorkOrder.assigned_to_id.in_(cids or [0])).group_by(models.WorkOrder.process_id, models.WorkOrder.assigned_to_id, models.WorkOrder.posted_date).all():
        done[(pid_, uid_, _as_date(pd_))] = int(tc_ or 0)

    out = []
    for p in procs:
        team = [u for u in colleagues if any(x.id == p.id for x in u.processes)]
        tgt = p.daily_target
        row = {"process_id": p.id, "process": p.name, "target": tgt, "team_size": len(team)}
        if not team:
            row["status"] = "No colleagues"
            out.append(row); continue
        present_today = [u for u in team if today in login.get(u.id, ()) and today not in leave.get(u.id, ())]
        today_actual = sum(done.get((p.id, u.id, today), 0) for u in team)
        # month so far (before today)
        pres_by_day = {d: sum(1 for u in team if d in login.get(u.id, ()) and d not in leave.get(u.id, ())) for d in past}
        mtd_before = sum(done.get((p.id, u.id, d), 0) for u in team for d in past)
        # work done on off days still counts toward the month
        off_done = sum(v for (pid, uid, d), v in done.items() if pid == p.id and uid in {u.id for u in team} and d < today and _is_off_day(d, hol))
        mtd_before += off_done
        row.update({"today_actual": today_actual, "present_today": len(present_today), "on_leave_today": sum(1 for u in team if today in leave.get(u.id, ()))})
        if not tgt:
            row["status"] = "No daily target set"
            row.update({"month_actual": mtd_before + today_actual})
            out.append(row); continue
        today_target = tgt * len(present_today) if today_is_work else 0
        expected_now = today_target * frac
        ratio = (today_actual / expected_now) if expected_now > 0 else None
        if not today_is_work:
            status = "Off day"
        elif not present_today:
            status = "Nobody logged in yet"
        elif frac <= 0:
            status = "Shift not started"
        elif ratio is None:
            status = "On pace"
        elif ratio >= 0.95:
            status = "On pace"
        elif ratio >= 0.8:
            status = "Slightly behind"
        else:
            status = "Behind"
        need_per_hour = round((today_target - today_actual) / max(hours_left, 0.25), 1) if today_target > today_actual and frac > 0 else 0
        run_rate = (today_actual / (frac * shift_hours)) if frac * shift_hours >= 0.5 else None
        today_proj = round(today_actual / frac) if frac >= 0.25 and today_actual else None
        # month
        days_with_people = [d for d in past if pres_by_day[d] > 0]
        avg_pres = (sum(pres_by_day[d] for d in days_with_people) / len(days_with_people)) if days_with_people else len(present_today) or len(team)
        past_target = sum(tgt * pres_by_day[d] for d in past)
        month_target = past_target + (tgt * len(present_today) if today_is_work else 0) + tgt * avg_pres * len(future)
        avg_daily = (mtd_before / len(days_with_people)) if days_with_people else None
        if today_is_work:
            t_est = max(today_actual, today_proj) if today_proj else max(today_actual, avg_daily or 0)
        else:
            t_est = today_actual
        projected = mtd_before + t_est + (avg_daily or 0) * len(future)
        row.update({
            "status": status, "today_target": today_target, "expected_now": round(expected_now), "pace_pct": round(ratio * 100) if ratio is not None else None,
            "need_per_hour": need_per_hour, "run_rate": round(run_rate, 1) if run_rate is not None else None, "today_projected": today_proj,
            "month_actual": mtd_before + today_actual, "month_target": round(month_target), "month_projected": round(projected),
            "month_projected_pct": round(projected * 100 / month_target) if month_target else None,
            "workdays_left": len(future), "workdays_done": len(past),
            "people": sorted([{"id": u.id, "name": u.full_name, "done": done.get((p.id, u.id, today), 0), "expected_now": round(tgt * frac),
                               "target": tgt, "present": u in present_today,
                               "pace_pct": round(done.get((p.id, u.id, today), 0) * 100 / (tgt * frac)) if (tgt * frac) > 0 and u in present_today else None}
                              for u in team], key=lambda r: (not r["present"], r["pace_pct"] if r["pace_pct"] is not None else 999, r["name"])),
        })
        out.append(row)
    return {"as_of": now_ist.strftime("%Y-%m-%d %H:%M"), "today": today.isoformat(), "shift_start": start_txt, "shift_hours": shift_hours,
            "shift_elapsed_pct": round(frac * 100), "month": today.strftime("%B %Y"), "processes": out,
            "can_edit_shift": current_user.role in ("admin", "manager", "super_admin")}


# ---------------------------------------------------------------------------
# Client-wise dashboard: volume, turnaround, escalation rate, effort, inactive-date tracking
# ---------------------------------------------------------------------------
@app.get("/clients/dashboard")
def clients_dashboard(process_id: Optional[int] = None, period: str = "month",
                      current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
                      db: Session = Depends(get_db)):
    today = _work_today()
    if period == "week":
        d1, label = today - timedelta(days=today.weekday()), "This week"
    elif period == "last_month":
        last = today.replace(day=1) - timedelta(days=1)
        d1, today_end, label = last.replace(day=1), last, last.strftime("%B %Y")
    elif period == "quarter":
        d1, label = today - timedelta(days=90), "Last 90 days"
    elif period == "month":
        d1, label = today.replace(day=1), today.strftime("%B %Y") + " (so far)"
    else:
        raise HTTPException(status_code=400, detail="period must be week, month, last_month or quarter")
    d2 = today_end if period == "last_month" else today
    W = models.WorkOrder
    scoped = _orders_dash_scope(db, current_user, process_id)
    clients = db.query(models.Client).all()
    by_no = {_norm_facility(c.facility_no): c for c in clients}
    nc = {}
    for c in clients:
        nc[_norm_text(c.client_name)] = nc.get(_norm_text(c.client_name), 0) + 1
    by_name = {_norm_text(c.client_name): c for c in clients if nc[_norm_text(c.client_name)] == 1}
    cache = {}

    def client_of(div, dname):
        k = (div or "", dname or "")
        if k not in cache:
            c = _client_for_row(div, by_no, by_name)
            if c is None and dname:
                c = _client_for_row(dname, by_no, by_name)
            cache[k] = c
        return cache[k]

    inactive_dates = {c.id: c.inactive_date for c in clients if c.status == "Inactive" and c.inactive_date}
    Ev = models.EscalationEvent
    esc_sub = db.query(Ev.order_id).distinct().subquery()
    completed = case((W.posting_status == "Completed", 1), else_=0)
    base = db.query(W.division, W.division_name, func.count(W.id), func.sum(completed), func.coalesce(func.sum(W.trans_count), 0),
                    func.coalesce(func.sum(W.tat_days), 0), func.count(W.tat_days), func.coalesce(func.sum(W.time_taken_seconds), 0),
                    func.count(esc_sub.c.order_id)).outerjoin(esc_sub, esc_sub.c.order_id == W.id).filter(
        W.received_date >= d1, W.received_date <= d2)
    grouped = scoped(base).group_by(W.division, W.division_name).all()
    # most recent order per client (all time, same scope) for dormancy
    last_seen = {}
    for div, dn, mx in scoped(db.query(W.division, W.division_name, func.max(W.received_date)).group_by(W.division, W.division_name)).all():
        c = client_of(div, dn)
        if c is not None and mx and (c.id not in last_seen or mx > last_seen[c.id]):
            last_seen[c.id] = _as_date(mx)

    agg = {}
    def slot(c):
        key = c.id if c else 0
        return agg.setdefault(key, {"client": c, "orders": 0, "completed": 0, "open": 0, "trans": 0, "tat_sum": 0, "tat_n": 0, "esc": 0, "secs": 0, "after_inactive": 0})
    for div, dn, n, comp, trans, tat_s, tat_n, secs, esc in grouped:
        c = client_of(div, dn)
        g = slot(c)
        g["orders"] += n; g["completed"] += int(comp or 0); g["open"] += n - int(comp or 0)
        g["trans"] += int(trans or 0); g["tat_sum"] += tat_s or 0; g["tat_n"] += tat_n or 0
        g["secs"] += int(secs or 0); g["esc"] += esc or 0
    # orders received after a client's inactive date (only the few inactive clients are looked at)
    if inactive_dates:
        for div, dn, n, mx in scoped(db.query(W.division, W.division_name, func.count(W.id), func.max(W.received_date)).filter(
                W.received_date >= d1, W.received_date <= d2)).group_by(W.division, W.division_name).all():
            c = client_of(div, dn)
            if c is not None and c.id in inactive_dates:
                cnt = scoped(db.query(func.count(W.id)).filter(W.division == div, W.division_name == dn if dn is not None else W.division_name.is_(None),
                                                               W.received_date >= d1, W.received_date <= d2, W.received_date > inactive_dates[c.id])).scalar() or 0
                slot(c)["after_inactive"] += cnt
    total_secs = sum(g["secs"] for g in agg.values()) or 0
    total_orders = sum(g["orders"] for g in agg.values()) or 0
    rows = []
    for key, g in agg.items():
        c = g["client"]
        rows.append({
            "client_id": c.id if c else None, "facility_no": c.facility_no if c else "", "client": c.client_name if c else "(not in client list)",
            "status": c.status if c else "", "inactive_date": c.inactive_date.isoformat() if c and c.inactive_date else None,
            "orders": g["orders"], "completed": g["completed"], "open": g["open"], "trans": g["trans"],
            "avg_tat": round(g["tat_sum"] / g["tat_n"], 1) if g["tat_n"] else None,
            "escalated": g["esc"], "esc_rate": round(g["esc"] * 100 / g["orders"], 1) if g["orders"] else 0,
            "effort_hours": round(g["secs"] / 3600, 1), "effort_share": round(g["secs"] * 100 / total_secs, 1) if total_secs else None,
            "order_share": round(g["orders"] * 100 / total_orders, 1) if total_orders else None,
            "min_per_order": round(g["secs"] / 60 / g["completed"], 1) if g["completed"] and g["secs"] else None,
            "orders_after_inactive": g["after_inactive"],
            "last_order": last_seen.get(c.id).isoformat() if c and last_seen.get(c.id) else None,
        })
    rows.sort(key=lambda r: (-r["effort_hours"], -r["orders"], r["client"]))
    # inactive-date tracking (independent of the period's volume)
    tracking = []
    seen_ids = {r["client_id"] for r in rows}
    for c in clients:
        ls = last_seen.get(c.id)
        days = (today - ls).days if ls else None
        if c.status == "Inactive":
            after = next((r["orders_after_inactive"] for r in rows if r["client_id"] == c.id), 0)
            if after or (ls and c.inactive_date and ls > c.inactive_date):
                tracking.append({"client_id": c.id, "facility_no": c.facility_no, "client": c.client_name, "kind": "Inactive but still sending work",
                                 "inactive_date": c.inactive_date.isoformat() if c.inactive_date else None, "last_order": ls.isoformat() if ls else None,
                                 "days": days, "detail": f"{after} order(s) received after the inactive date in this period" if after else "Orders received after the inactive date"})
        else:
            if days is not None and days >= 30:
                tracking.append({"client_id": c.id, "facility_no": c.facility_no, "client": c.client_name, "kind": "Active but no work for 30+ days",
                                 "inactive_date": None, "last_order": ls.isoformat(), "days": days, "detail": "Consider marking the client Inactive if it has stopped sending work"})
    tracking.sort(key=lambda r: (r["kind"], -(r["days"] or 0)))
    tot = {"orders": total_orders, "completed": sum(g["completed"] for g in agg.values()), "escalated": sum(g["esc"] for g in agg.values()),
           "effort_hours": round(total_secs / 3600, 1), "clients": len([r for r in rows if r["client_id"]])}
    tot["esc_rate"] = round(tot["escalated"] * 100 / total_orders, 1) if total_orders else 0
    return {"label": label, "from": d1.isoformat(), "to": d2.isoformat(), "rows": rows, "tracking": tracking, "totals": tot}


# ---------------------------------------------------------------------------
# Escalation reasons report + auto-reminders for teams that are taking too long
# ---------------------------------------------------------------------------
ESC_TEAMS = ("onshore", "recon", "calling")
ESC_TEAM_LABEL = {"onshore": "Onshore", "recon": "Recon", "calling": "Calling"}


def _client_resolver(db: Session):
    clients = db.query(models.Client).all()
    by_no = {_norm_facility(c.facility_no): c for c in clients}
    nc = {}
    for c in clients:
        nc[_norm_text(c.client_name)] = nc.get(_norm_text(c.client_name), 0) + 1
    by_name = {_norm_text(c.client_name): c for c in clients if nc[_norm_text(c.client_name)] == 1}
    cache = {}

    def resolve(div, dname=None):
        k = (div or "", dname or "")
        if k not in cache:
            c = _client_for_row(div, by_no, by_name)
            if c is None and dname:
                c = _client_for_row(dname, by_no, by_name)
            cache[k] = c
        return cache[k]
    return resolve


@app.get("/escalations/reasons")
def escalation_reasons(process_id: Optional[int] = None, period: str = "month",
                       current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
                       db: Session = Depends(get_db)):
    """Escalations raised in the period counted by category, client and process, with the repeat problems (same client + category, same order)."""
    today = _work_today()
    if period == "week":
        d1, d2, label = today - timedelta(days=today.weekday()), today, "This week"
    elif period == "last_month":
        last = today.replace(day=1) - timedelta(days=1)
        d1, d2, label = last.replace(day=1), last, last.strftime("%B %Y")
    elif period == "quarter":
        d1, d2, label = today - timedelta(days=90), today, "Last 90 days"
    elif period == "month":
        d1, d2, label = today.replace(day=1), today, today.strftime("%B %Y") + " (so far)"
    else:
        raise HTTPException(status_code=400, detail="period must be week, month, last_month or quarter")
    E = models.EscalationEvent
    q = db.query(E).filter(E.raised_date >= d1, E.raised_date <= d2)
    if process_id is not None:
        q = q.filter(E.process_id == process_id)
    if current_user.role == "team_lead":
        q = q.filter(E.team_lead_id == current_user.id)
        mine = [p.id for p in current_user.processes]
        q = q.filter(E.process_id.in_(mine or [0]))
    events = q.all()
    oids = list({e.order_id for e in events})
    info = {}
    for i in range(0, len(oids), 800):
        for o in db.query(models.WorkOrder.id, models.WorkOrder.edm, models.WorkOrder.division, models.WorkOrder.division_name).filter(models.WorkOrder.id.in_(oids[i:i + 800])).all():
            info[o.id] = o
    resolve = _client_resolver(db)
    pn = {p.id: p.name for p in db.query(models.Process).all()}
    cats, clients, procs, combos, orders = {}, {}, {}, {}, {}
    for e, cat in [(e, c) for e in events for c in _event_cats(e)]:
        o = info.get(e.order_id)
        c = resolve(o.division, o.division_name) if o else None
        ckey = (c.facility_no, c.client_name) if c else ("", "(unknown / order removed)" if not o else "(not in client list)")
        g = cats.setdefault(cat, {"category": cat, "count": 0, "open": 0, "days": 0, "closed": 0})
        g["count"] += 1
        if e.closed_date:
            g["closed"] += 1
            g["days"] += _count_business_days_ex_holidays(e.raised_date, e.closed_date)
        else:
            g["open"] += 1
        cl = clients.setdefault(ckey, {"facility_no": ckey[0], "client": ckey[1], "count": 0, "cats": {}})
        cl["count"] += 1
        cl["cats"][cat] = cl["cats"].get(cat, 0) + 1
        pr = procs.setdefault(e.process_id, {"process": pn.get(e.process_id, "-"), "count": 0, "cats": {}})
        pr["count"] += 1
        pr["cats"][cat] = pr["cats"].get(cat, 0) + 1
        k = (ckey, cat)
        combos[k] = combos.get(k, 0) + 1
        orders.setdefault(e.order_id, []).append(cat)
    total = sum(g["count"] for g in cats.values()) or 0       # an escalation with two reasons counts under both
    cat_rows = [{"category": g["category"], "count": g["count"], "share": round(g["count"] * 100 / total, 1) if total else 0, "open": g["open"],
                 "avg_days": round(g["days"] / g["closed"], 1) if g["closed"] else None} for g in cats.values()]
    cat_rows.sort(key=lambda r: -r["count"])
    top = lambda d: max(d.items(), key=lambda kv: kv[1])[0] if d else ""
    cli_rows = [{"facility_no": v["facility_no"], "client": v["client"], "count": v["count"], "share": round(v["count"] * 100 / total, 1) if total else 0,
                 "top_category": top(v["cats"]), "top_count": max(v["cats"].values())} for v in clients.values()]
    cli_rows.sort(key=lambda r: (-r["count"], r["client"]))
    proc_rows = [{"process": v["process"], "count": v["count"], "top_category": top(v["cats"])} for v in procs.values()]
    proc_rows.sort(key=lambda r: -r["count"])
    repeats = [{"facility_no": k[0][0], "client": k[0][1], "category": k[1], "count": n} for k, n in combos.items() if n >= 2]
    repeats.sort(key=lambda r: (-r["count"], r["client"]))
    again = []
    for oid, cl_ in orders.items():
        if len(cl_) >= 2:
            o = info.get(oid)
            again.append({"order_id": oid if o else None, "edm": (o.edm if o else "") or "", "times": len(cl_), "categories": ", ".join(sorted(set(cl_)))})
    again.sort(key=lambda r: -r["times"])
    return {"label": label, "from": d1.isoformat(), "to": d2.isoformat(), "total": total, "orders_escalated": len(orders),
            "by_category": cat_rows, "by_client": cli_rows[:50], "by_process": proc_rows, "repeat_problems": repeats[:30], "orders_escalated_again": again[:30]}


ESC_REM_DEFAULTS = {"enabled": "1", "auto_email": "0", "email_team": "1", "email_tl": "1", "onshore": "24", "recon": "24", "calling": "24", "repeat": "24"}


def _esc_rem_settings(db: Session) -> dict:
    out = {}
    for k, dv in ESC_REM_DEFAULTS.items():
        v = _setting(db, "escrem_" + k, dv)
        out[k] = (v == "1") if k in ("enabled", "auto_email", "email_team", "email_tl") else float(v or dv)
    out["smtp_configured"] = email_utils.is_configured()
    return out


@app.get("/settings/esc-reminders")
def get_esc_reminder_settings(current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")), db: Session = Depends(get_db)):
    d = _esc_rem_settings(db)
    d["can_edit"] = current_user.role in ("admin", "manager", "super_admin")
    return d


@app.put("/settings/esc-reminders")
def set_esc_reminder_settings(payload: dict, current_user: models.User = Depends(auth.require_role("admin", "manager", "super_admin")), db: Session = Depends(get_db)):
    for k in ("enabled", "auto_email", "email_team", "email_tl"):
        if k in payload:
            _set_setting(db, "escrem_" + k, "1" if payload[k] else "0")
    for k in ("onshore", "recon", "calling", "repeat"):
        if k in payload:
            try:
                v = float(payload[k])
                assert 1 <= v <= 720
            except Exception:
                raise HTTPException(status_code=400, detail="Hours must be between 1 and 720")
            _set_setting(db, "escrem_" + k, str(v))
    _audit(db, current_user, "Settings", "Changed escalation reminder settings", None, None, json.dumps({k: payload[k] for k in payload if k in ESC_REM_DEFAULTS})[:300])
    db.commit()
    return _esc_rem_settings(db)


def _esc_overdue_rows(db: Session, orders, cfg: dict) -> list:
    now_ist = datetime.now(IST).replace(tzinfo=None)
    ids = [o.id for o in orders]
    last_msg, last_rem = {}, {}
    for i in range(0, len(ids), 800):
        chunk = ids[i:i + 800]
        for m in db.query(models.OnshoreMessage).filter(models.OnshoreMessage.order_id.in_(chunk)).order_by(models.OnshoreMessage.id).all():
            last_msg[(m.order_id, m.track or None)] = m.created_at
        for r in db.query(models.EscalationReminder).filter(models.EscalationReminder.order_id.in_(chunk)).order_by(models.EscalationReminder.id).all():
            last_rem[(r.order_id, r.track or None)] = r
    pn = {p.id: p.name for p in db.query(models.Process).all()}
    tls = {u.id: u for u in db.query(models.User).filter(models.User.role == "team_lead").all()}
    rows = []
    for o in orders:
        if o.onshore_status not in ("with_onshore", "blue", "orange"):
            continue
        team = o.onshore_team if o.onshore_team in ESC_TEAMS else "onshore"
        trk = getattr(o, "track", None)
        since = last_msg.get((o.id, trk)) or o.onshore_sent_at
        if not since:
            continue
        hrs = (now_ist - since).total_seconds() / 3600
        limit = cfg[team]
        if hrs < limit:
            continue
        r = last_rem.get((o.id, trk))
        tl = tls.get(o.team_lead_id)
        rows.append({"id": o.id, "edm": o.edm or ("#%d" % o.id), "process": pn.get(o.process_id, ""), "process_id": o.process_id, "category": o.escalation_category or "",
                     "track": trk, "team": team, "team_label": ESC_TEAM_LABEL[team], "waiting_hours": round(hrs, 1), "limit_hours": limit, "over_by": round(hrs - limit, 1),
                     "since": since.strftime("%Y-%m-%d %H:%M"), "team_lead": tl.full_name if tl else "", "team_lead_email": tl.email if tl else None,
                     "reminded_at": r.sent_at.strftime("%Y-%m-%d %H:%M") if r else None, "reminded_hours_ago": round((now_ist - r.sent_at).total_seconds() / 3600, 1) if r else None,
                     "reminders_sent": 0})
    cnt = {}
    if rows:
        for oid, tk, n in db.query(models.EscalationReminder.order_id, models.EscalationReminder.track, func.count(models.EscalationReminder.id)).filter(
                models.EscalationReminder.order_id.in_([r["id"] for r in rows])).group_by(models.EscalationReminder.order_id, models.EscalationReminder.track).all():
            cnt[(oid, tk or None)] = n
        for r in rows:
            r["reminders_sent"] = cnt.get((r["id"], r["track"]), 0)
    rows.sort(key=lambda r: -r["over_by"])
    return rows


def _esc_open_orders(db: Session, scoped=None):
    W = models.WorkOrder
    q = db.query(W).options(selectinload(W.escalation_detail)).filter(
        or_(W.onshore_status.in_(("with_onshore", "blue", "orange")), W.id.in_(_has_extra_state(db))), W.submitted.is_(False))
    if scoped is not None:
        q = scoped(q)
    return [t for o in q.all() for t in _order_tracks(o) if t.onshore_status in _WAITING]


@app.get("/escalations/overdue")
def escalations_overdue(process_id: Optional[int] = None,
                        current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")), db: Session = Depends(get_db)):
    cfg = _esc_rem_settings(db)
    scoped = _orders_dash_scope(db, current_user, process_id)
    rows = _esc_overdue_rows(db, _esc_open_orders(db, scoped), cfg)
    return {"settings": cfg, "rows": rows, "by_team": {ESC_TEAM_LABEL[t]: sum(1 for r in rows if r["team"] == t) for t in ESC_TEAMS}}


def _esc_send(db: Session, rows: list, cfg: dict, actor_name: str, auto: bool) -> dict:
    """Emails one digest per receiving team member (and per Team Lead) and records a reminder per order. Flags work even without SMTP."""
    now_ist = datetime.now(IST).replace(tzinfo=None)
    smtp = email_utils.is_configured()
    sent = failed = 0
    for team in ESC_TEAMS:
        part = [r for r in rows if r["team"] == team]
        if not part:
            continue
        lines = "\n".join(f"- {r['edm']} ({r['process']}{', ' + r['category'] if r['category'] else ''}) waiting {r['waiting_hours']} h, limit {r['limit_hours']:g} h" for r in part)
        body = f"{len(part)} escalation(s) have been waiting with {ESC_TEAM_LABEL[team]} longer than the agreed time:\n\n{lines}\n\nPlease reply in WorkFlow Hub."
        n_rec = 0
        if smtp and cfg["email_team"]:
            for u in db.query(models.User).filter(models.User.role == team, models.User.email.isnot(None)).all():
                if u.employment_status == "Inactive":
                    continue
                if email_utils.send_email(u.email, f"Reminder: {len(part)} escalation(s) waiting for {ESC_TEAM_LABEL[team]}", body):
                    n_rec += 1; sent += 1
                else:
                    failed += 1
        if smtp and cfg["email_tl"]:
            by_tl = {}
            for r in part:
                if r["team_lead_email"]:
                    by_tl.setdefault(r["team_lead_email"], []).append(r)
            for mail, rs in by_tl.items():
                tl_lines = "\n".join(f"- {r['edm']} ({r['process']}) waiting {r['waiting_hours']} h with {ESC_TEAM_LABEL[team]}" for r in rs)
                if email_utils.send_email(mail, f"FYI: {len(rs)} escalation(s) overdue with {ESC_TEAM_LABEL[team]}", f"These escalations of your team are past the {ESC_TEAM_LABEL[team]} time limit:\n\n{tl_lines}"):
                    n_rec += 1; sent += 1
                else:
                    failed += 1
        for r in part:
            db.add(models.EscalationReminder(order_id=r["id"], track=r.get("track"), team=team, sent_at=now_ist, hours_waiting=r["waiting_hours"], recipients=n_rec,
                                             emailed=n_rec > 0, auto=auto, sent_by_name=actor_name))
    db.commit()
    return {"orders": len(rows), "emails_sent": sent, "emails_failed": failed, "smtp_configured": smtp}


@app.post("/escalations/overdue/remind")
def escalations_overdue_remind(payload: dict, process_id: Optional[int] = None,
                               current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")), db: Session = Depends(get_db)):
    cfg = _esc_rem_settings(db)
    scoped = _orders_dash_scope(db, current_user, process_id)
    rows = _esc_overdue_rows(db, _esc_open_orders(db, scoped), cfg)
    want = payload.get("order_ids")
    if want:
        want = {int(x) for x in want}
        rows = [r for r in rows if r["id"] in want]
    if not rows:
        raise HTTPException(status_code=400, detail="Nothing is overdue")
    res = _esc_send(db, rows, cfg, current_user.full_name, False)
    _audit(db, current_user, "Escalation", "Sent escalation reminders", None, None, f"{res['orders']} order(s), {res['emails_sent']} email(s)")
    db.commit()
    return res


def _esc_auto_reminders():
    db = SessionLocal()
    try:
        cfg = _esc_rem_settings(db)
        if not (cfg["enabled"] and cfg["auto_email"] and cfg["smtp_configured"]):
            return
        now = datetime.now(IST)
        if _is_off_day(now.date(), _hol_set(db, now.date(), now.date())):
            return
        rows = _esc_overdue_rows(db, _esc_open_orders(db), cfg)
        rows = [r for r in rows if r["reminded_hours_ago"] is None or r["reminded_hours_ago"] >= cfg["repeat"]]
        if rows:
            print("[esc reminders]", _esc_send(db, rows, cfg, "Auto reminder", True))
    finally:
        db.close()


@app.get("/breaks/audit")
def break_audit_log(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    category: Optional[str] = None,
    q: Optional[str] = None,
    limit: int = 200,
    offset: int = 0,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    """Audit log (newest first), one page at a time (limit/offset). Admin / Manager / Super Admin: everything. Team Lead: their own actions and anything about their team."""
    today = datetime.now(IST).date()
    try:
        d1 = date.fromisoformat(date_from) if date_from else today - timedelta(days=30)
        d2 = date.fromisoformat(date_to) if date_to else today
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date")
    lo, hi = _ist_bounds(d1, d2)
    qq = db.query(models.AuditEvent).filter(models.AuditEvent.at >= lo, models.AuditEvent.at < hi)
    if current_user.role == "team_lead":
        ids = [u.id for u in _batch_visible_colleagues(db, current_user)]
        qq = qq.filter(or_(models.AuditEvent.actor_id == current_user.id, models.AuditEvent.target_user_id.in_(ids or [-1])))
    if category:
        qq = qq.filter(models.AuditEvent.category == category)
    needle = (q or "").strip()
    if needle:
        like = "%" + needle.replace("%", "").replace("_", "") + "%"
        A = models.AuditEvent
        qq = qq.filter(or_(A.actor_name.ilike(like), A.category.ilike(like), A.action.ilike(like), A.target_name.ilike(like), A.details.ilike(like), A.ref_date.ilike(like)))
    total = qq.order_by(None).count()
    page = max(1, min(limit, 500))
    rows = qq.order_by(models.AuditEvent.id.desc()).offset(max(0, offset)).limit(page).all()
    out = [{"id": e.id, "at": e.at.isoformat() + "Z", "actor": e.actor_name or "", "role": e.actor_role or "", "category": e.category, "action": e.action,
            "target": e.target_name or "", "ref_date": e.ref_date or "", "details": e.details or ""} for e in rows]
    cats = [c for (c,) in db.query(models.AuditEvent.category).distinct().all()]
    return {"from": d1.isoformat(), "to": d2.isoformat(), "categories": sorted(cats), "rows": out, "total": total, "offset": max(0, offset), "has_more": max(0, offset) + len(out) < total}


@app.get("/breaks/calendar")
def break_calendar(
    month: Optional[str] = None,
    current_user: models.User = Depends(auth.require_role("colleague", "team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    Attendance calendar: one row per colleague, one cell per day of the month.
    ok = logged in, nothing flagged; warn = 1 exception; bad = 2 or more; off = weekend / holiday; none = no login on a working day.
    Colleague: own row. Team Lead: their team. Admin / Super Admin: everyone.
    """
    today = datetime.now(IST).date()
    try:
        y, mo = (int(x) for x in month.split("-")) if month else (today.year, today.month)
        first = date(y, mo, 1)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=400, detail="month must look like 2026-10")
    last = date(y + (mo == 12), mo % 12 + 1, 1) - timedelta(days=1)
    upto = min(last, today)
    ndays = last.day
    if current_user.role == "colleague":
        users = [current_user]
    else:
        users = [u for u in _batch_visible_colleagues(db, current_user) if u.employment_status != "Inactive"]
    users.sort(key=lambda u: ((u.reporting_manager or ""), u.full_name))
    hol = _hol_set(db, first, last)
    days = _collect_days(db, [u.id for u in users], first, upto) if upto >= first else {}
    lvmap = {(lv.user_id, lv.leave_date): lv for lv in db.query(models.LeaveDay).filter(models.LeaveDay.leave_date >= first, models.LeaveDay.leave_date <= last, models.LeaveDay.user_id.in_([u.id for u in users] or [-1])).all()}
    scs = db.query(models.ShiftChange).filter(models.ShiftChange.date_from <= last, models.ShiftChange.date_to >= first, models.ShiftChange.user_id.in_([u.id for u in users] or [-1])).order_by(models.ShiftChange.id).all()
    out = []
    for u in users:
        cells = {}
        mysc = [x for x in scs if x.user_id == u.id]
        for i in range(ndays):
            d = first + timedelta(days=i)
            iso = d.isoformat()
            off = _is_off_day(d, hol)
            v = days.get((u.id, d))
            lv = lvmap.get((u.id, d))
            if v and v["first_login"] is not None and d <= today:
                flags, _late = _day_flags(v, u.shift, d, off, today)
                cells[iso] = {"s": "ok" if not flags else "warn" if len(flags) == 1 else "bad", "f": flags, "adj": bool(v["adj"]), "in": _ist_dt(v["first_login"]).strftime("%I:%M %p")}
            elif lv:
                cells[iso] = {"s": "leave", "k": lv.kind, "n": lv.note or "", "id": lv.id}
            elif d > today:
                cells[iso] = {"s": "future"}
            elif off:
                cells[iso] = {"s": "off"}
            else:
                cells[iso] = {"s": "pending" if d == today else "none"}
            if lv and cells[iso]["s"] != "leave":
                cells[iso]["k"] = lv.kind; cells[iso]["id"] = lv.id
            for x in mysc:
                if x.date_from <= d <= x.date_to:
                    cells[iso]["sc"] = x.shift.split(" (")[0] + (" - " + x.note if x.note else "") + (" (set by " + x.created_by_name + ")" if x.created_by_name else "")
        out.append({"user_id": u.id, "employee": u.full_name, "team_lead": u.reporting_manager or "", "cells": cells})
    return {"month": first.strftime("%B %Y"), "first": first.isoformat(), "days": ndays, "today": today.isoformat(), "rows": out,
            "holidays": sorted(x.isoformat() for x in hol)}


@app.get("/breaks/live")
def break_live(
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    """Today, right now: who is working, on a break, logged out or not logged in yet. Team Lead -> team, Admin / Super Admin -> all."""
    now = datetime.utcnow()
    today = datetime.now(IST).date()
    now_ist = datetime.now(IST)
    users = [u for u in _batch_visible_colleagues(db, current_user) if u.employment_status != "Inactive"]
    ids = [u.id for u in users]
    days = _collect_days(db, ids, today, today)
    away = {a.user_id: a for a in db.query(models.AwayNow).filter(models.AwayNow.user_id.in_(ids or [-1])).all()}
    off = _is_off_day(today, _hol_set(db, today, today))
    ovm = {c_.user_id: c_.shift for c_ in db.query(models.ShiftChange).filter(models.ShiftChange.date_from <= today, models.ShiftChange.date_to >= today, models.ShiftChange.user_id.in_(ids or [-1])).order_by(models.ShiftChange.id).all()}
    leaves = {lv.user_id: lv.kind for lv in db.query(models.LeaveDay).filter(models.LeaveDay.leave_date == today, models.LeaveDay.user_id.in_(ids or [-1])).all()}
    out = []
    for u in users:
        d = days.get((u.id, today))
        a = away.get(u.id)
        eff = ovm.get(u.id) or u.shift
        st = _shift_start_minutes(eff)
        lv = leaves.get(u.id)
        late = None if (off or lv or not d or "late" in d["excused"]) else _late_minutes(d["first_login"], eff)
        if a:
            status = "on_break"
        elif d and d["live"]:
            status = "working"
        elif d:
            status = "logged_out"
        elif lv:
            status = "on_leave"
        else:
            status = "not_logged_in"
        overdue = None      # not logged in yet although the shift has started
        if status == "not_logged_in" and not off and st is not None:
            over = now_ist.hour * 60 + now_ist.minute - st
            if over > LATE_GRACE_MIN:
                overdue = over
        out.append({
            "user_id": u.id, "employee": u.full_name, "employee_id": u.employee_id, "team_lead": u.reporting_manager or "",
            "shift": eff or "", "shift_temp": bool(ovm.get(u.id)), "status": status, "leave_kind": lv,
            "login": (d["first_login"].isoformat() + "Z") if d and d["first_login"] else None,
            "late_minutes": late, "not_logged_in_overdue": overdue,
            "away_since": (a.started_at.isoformat() + "Z") if a else None,
            "away_minutes": round((now - a.started_at).total_seconds() / 60, 1) if a else None,
            "away_no_signal": round((now - a.updated_at).total_seconds() / 60, 1) if a else None,
            "tea_lunch": round(d["tea_lunch"], 1) if d else 0, "other": round(d["other"], 1) if d else 0,
            "meeting": round(d["meeting"], 1) if d else 0, "breaks": d["breaks"] if d else 0,
            "clocked": d["clocked"] if d else 0, "production": round(d["production"], 1) if d else 0,
        })
    return {"date": today.isoformat(), "off_day": off, "grace": LATE_GRACE_MIN, "rows": out}


@app.get("/breaks/my-month")
def break_my_month(
    current_user: models.User = Depends(auth.require_role("colleague")),
    db: Session = Depends(get_db),
):
    """A colleague's own numbers: today against the limits, and the month so far."""
    today = datetime.now(IST).date()
    first = today.replace(day=1)
    days = _collect_days(db, [current_user.id], first, today)
    hol = _hol_set(db, first, today)
    t = days.get((current_user.id, today))
    my_leaves = {lv.leave_date: lv.kind for lv in db.query(models.LeaveDay).filter(models.LeaveDay.user_id == current_user.id, models.LeaveDay.leave_date >= first, models.LeaveDay.leave_date <= today).all()}
    work_days = sum(1 for i in range((today - first).days + 1) if not _is_off_day(first + timedelta(days=i), hol) and (first + timedelta(days=i)) not in my_leaves)
    m = {"present": 0, "done": 0, "clocked": 0.0, "production": 0.0, "breaks": 0.0, "late_dates": [], "over_dates": [], "under_clocked": 0, "under_prod": 0}
    for (uid, d), v in sorted(days.items(), key=lambda x: x[0][1]):
        off = _is_off_day(d, hol)
        flags, late = _day_flags(v, current_user.shift, d, off, today)
        m["breaks"] += v["brk_total"]
        if not off and v["first_login"] is not None:
            m["present"] += 1
            if not (d >= today and v["live"]):       # today's unfinished day would drag the averages down
                m["done"] += 1
                m["clocked"] += v["clocked"]; m["production"] += v["production"]
        if late is not None:
            m["late_dates"].append(d.isoformat())
        if any(x in flags for x in ("Tea + Lunch over 1h", "Other break over 30m", "Single break over 2h")):
            m["over_dates"].append(d.isoformat())
        if "Clocked under 9h" in flags:
            m["under_clocked"] += 1
        if "Production under 7:30h" in flags:
            m["under_prod"] += 1
    return {
        "month": today.strftime("%B %Y"), "date": today.isoformat(), "off_day": _is_off_day(today, hol), "grace": LATE_GRACE_MIN,
        "shift": current_user.shift or "", "work_days": work_days, "leave_days": len(my_leaves),
        "today": {"tea_lunch": round(t["tea_lunch"], 1) if t else 0, "other": round(t["other"], 1) if t else 0,
                  "meeting": round(t["meeting"], 1) if t else 0, "clocked": t["clocked"] if t else 0, "production": round(t["production"], 1) if t else 0,
                  "login": (t["first_login"].isoformat() + "Z") if t and t["first_login"] else None},
        "limits": {"tea_lunch": LIM_TEA_LUNCH, "other": LIM_OTHER, "clocked": LIM_CLOCKED, "production": LIM_PRODUCTION},
        "month_stats": {"present": m["present"], "clocked": round(m["clocked"], 1), "production": round(m["production"], 1), "breaks": round(m["breaks"], 1),
                        "avg_clocked": round(m["clocked"] / m["done"], 1) if m["done"] else 0,
                        "avg_production": round(m["production"] / m["done"], 1) if m["done"] else 0,
                        "late_days": len(m["late_dates"]), "late_dates": m["late_dates"], "over_days": len(m["over_dates"]), "over_dates": m["over_dates"],
                        "under_clocked": m["under_clocked"], "under_prod": m["under_prod"]},
    }


@app.get("/breaks/month-pack")
def break_month_pack(
    month: Optional[str] = None,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    """Month-end pack for HR: one row per colleague with attendance, leave, breaks by type, hours and adjustments."""
    today = datetime.now(IST).date()
    try:
        y, mo = (int(x) for x in month.split("-")) if month else (today.year, today.month)
        first = date(y, mo, 1)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=400, detail="month must look like 2026-10")
    last_full = date(y + (mo == 12), mo % 12 + 1, 1) - timedelta(days=1)
    last = min(last_full, today)
    if last < first:
        return {"month": first.strftime("%B %Y"), "rows": []}
    users = [u for u in _batch_visible_colleagues(db, current_user) if u.employment_status != "Inactive"]
    ids = [u.id for u in users]
    days = _collect_days(db, ids, first, last)
    hol = _hol_set(db, first, last)
    lv = {}
    for x in db.query(models.LeaveDay).filter(models.LeaveDay.leave_date >= first, models.LeaveDay.leave_date <= last, models.LeaveDay.user_id.in_(ids or [-1])).all():
        lv.setdefault(x.user_id, {})[x.leave_date] = x.kind
    adjn = {}
    for a in db.query(models.BreakAdjustment).filter(models.BreakAdjustment.adj_date >= first, models.BreakAdjustment.adj_date <= last, models.BreakAdjustment.user_id.in_(ids or [-1])).all():
        adjn[a.user_id] = adjn.get(a.user_id, 0) + 1
    workdays = [first + timedelta(days=i) for i in range((last - first).days + 1)]
    workdays = [d for d in workdays if not _is_off_day(d, hol)]
    rows = []
    for u in sorted(users, key=lambda x: ((x.reporting_manager or ""), x.full_name)):
        mine = {d: v for (uid, d), v in days.items() if uid == u.id and v["first_login"] is not None}
        ul = lv.get(u.id, {})
        r = {"employee": u.full_name, "employee_id": u.employee_id or "", "team_lead": u.reporting_manager or "", "shift": u.shift or "",
             "working_days": sum(1 for d in workdays if d not in ul), "days_present": 0, "leave": 0, "sick": 0, "training": 0, "other_leave": 0,
             "no_login_days": 0, "late_days": 0, "tea": [0, 0], "lunch": [0, 0], "other": [0, 0], "meeting": [0, 0],
             "break_limit_days": 0, "long_break_days": 0, "no_logout_days": 0, "short_hours_days": 0, "exception_days": 0, "left_early_days": 0, "overtime": 0.0,
             "clocked": 0.0, "production": 0.0, "done": 0, "adjustments": adjn.get(u.id, 0)}
        for k in ul.values():
            key = {"Leave": "leave", "Sick": "sick", "Training": "training"}.get(k, "other_leave")
            r[key] += 1
        for d in workdays:
            if d not in ul and d not in mine and d < today:
                r["no_login_days"] += 1
        for d, v in sorted(mine.items()):
            flags, late = _day_flags(v, u.shift, d, _is_off_day(d, hol), today)
            if not (_is_off_day(d, hol) or d in ul):
                r["days_present"] += 1
            if "Late login" in flags: r["late_days"] += 1
            if any(x in flags for x in ("Tea + Lunch over 1h", "Other break over 30m")): r["break_limit_days"] += 1
            if "Single break over 2h" in flags: r["long_break_days"] += 1
            if "No logout recorded" in flags: r["no_logout_days"] += 1
            if "Clocked under 9h" in flags or "Production under 7:30h" in flags: r["short_hours_days"] += 1
            if "Left early" in flags: r["left_early_days"] += 1
            if flags: r["exception_days"] += 1
            if not (_is_off_day(d, hol) or d in ul or (d >= today and v["live"])):
                r["overtime"] += max(0.0, v["clocked"] - LIM_CLOCKED)
            for bt, (c, m) in v["bt"].items():
                key = {"Tea Break": "tea", "Lunch Break": "lunch", "Dinner Break": "lunch", "Other Break": "other"}.get(bt, "meeting")
                r[key][0] += c; r[key][1] += round(m)
            if not (_is_off_day(d, hol) or d in ul or (d >= today and v["live"])):
                r["done"] += 1; r["clocked"] += v["clocked"]; r["production"] += v["production"]
        r["overtime_h"] = round(r["overtime"] / 60, 1)
        r["total_clocked_h"] = round(r["clocked"] / 60, 1)
        r["total_production_h"] = round(r["production"] / 60, 1)
        r["avg_clocked_h"] = round(r["clocked"] / r["done"] / 60, 2) if r["done"] else 0
        r["avg_production_h"] = round(r["production"] / r["done"] / 60, 2) if r["done"] else 0
        rows.append(r)
    return {"month": first.strftime("%B %Y"), "through": last.isoformat(), "rows": rows}


@app.get("/breaks/exceptions")
def break_exceptions(
    month: Optional[str] = None,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    Monthly exception report: every person-day that broke a rule (late login, Tea + Lunch over 1h,
    Other over 30m, clocked under 9h, production under 7:30h) with the break reasons given.
    month = YYYY-MM (default: the current IST month).
    """
    today = datetime.now(IST).date()
    try:
        y, mo = (int(x) for x in month.split("-")) if month else (today.year, today.month)
        first = date(y, mo, 1)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=400, detail="month must look like 2026-10")
    last = (date(y + (mo == 12), mo % 12 + 1, 1) - timedelta(days=1))
    last = min(last, today)
    if last < first:
        return {"month": first.strftime("%B %Y"), "rows": [], "summary": []}
    users = {u.id: u for u in _batch_visible_colleagues(db, current_user)}
    ids = None if current_user.role in ("admin", "manager", "super_admin") else list(users)
    days = _collect_days(db, ids, first, last)
    hol = _hol_set(db, first, last)
    if ids is None:
        users = {u.id: u for u in db.query(models.User).filter(models.User.id.in_({k[0] for k in days} or {-1})).all()}
    rows, summ = [], {}
    for (uid, d), v in sorted(days.items(), key=lambda x: (x[0][1], (users.get(x[0][0]).full_name if users.get(x[0][0]) else ""))):
        u = users.get(uid)
        if not u:
            continue
        off = _is_off_day(d, hol)
        flags, late = _day_flags(v, u.shift, d, off, today)
        if not flags:
            continue
        rows.append({
            "date": d.isoformat(), "employee": u.full_name, "employee_id": u.employee_id or "", "team_lead": u.reporting_manager or "",
            "shift": u.shift or "", "login": _ist_dt(v["first_login"]).strftime("%H:%M") if v["first_login"] else "",
            "logout": _ist_dt(v["last_logout"]).strftime("%H:%M") if v["last_logout"] else "", "late_minutes": late or 0,
            "tea_lunch": round(v["tea_lunch"]), "other": round(v["other"]), "meeting": round(v["meeting"]),
            "clocked": round(v["clocked"]), "production": round(v["production"]),
            "flags": ", ".join(flags), "reasons": "; ".join(v["reasons"]),
            "adjusted": "; ".join(v["adj"]),
        })
        sm = summ.setdefault(uid, {"employee": u.full_name, "team_lead": u.reporting_manager or "", "shift": u.shift or "", "exception_days": 0,
                                   "late": 0, "tea_lunch": 0, "other": 0, "clocked": 0, "production": 0, "long": 0, "no_logout": 0, "early": 0})
        sm["exception_days"] += 1
        sm["late"] += "Late login" in flags
        sm["tea_lunch"] += "Tea + Lunch over 1h" in flags
        sm["other"] += "Other break over 30m" in flags
        sm["clocked"] += "Clocked under 9h" in flags
        sm["production"] += "Production under 7:30h" in flags
        sm["long"] += "Single break over 2h" in flags
        sm["no_logout"] += "No logout recorded" in flags
        sm["early"] += "Left early" in flags
    adjs = []
    for a in db.query(models.BreakAdjustment).filter(models.BreakAdjustment.adj_date >= first, models.BreakAdjustment.adj_date <= last).order_by(models.BreakAdjustment.adj_date, models.BreakAdjustment.id).all():
        u = users.get(a.user_id)
        if u:
            adjs.append({"date": a.adj_date.isoformat(), "employee": u.full_name, "team_lead": u.reporting_manager or "", "excused": a.excused or "",
                         "retype": (a.old_type + " -> " + a.new_type) if a.new_type else "", "note": a.note, "by": a.created_by_name or ""})
    return {"month": first.strftime("%B %Y"), "rows": rows, "adjustments": adjs, "summary": sorted(summ.values(), key=lambda x: (-x["exception_days"], x["employee"]))}


@app.get("/auth/me/photo")
def get_my_photo(current_user: models.User = Depends(auth.get_current_user)):
    return {"photo": current_user.photo or ""}


@app.put("/auth/me/photo")
def set_my_photo(payload: schemas.PhotoIn, current_user: models.User = Depends(auth.get_current_user), db: Session = Depends(get_db)):
    """Profile picture (the browser shrinks it to about 160x160 first). Empty value removes it."""
    ph = (payload.photo or "").strip()
    if ph and (not ph.startswith("data:image/") or len(ph) > 250_000):
        raise HTTPException(status_code=400, detail="Please choose a smaller picture (JPG or PNG).")
    current_user.photo = ph or None
    db.commit()
    return {"ok": True}


def _away_clear(db: Session, user_id: int):
    db.query(models.AwayNow).filter(models.AwayNow.user_id == user_id).delete()


@app.post("/breaks/away")
def away_start(payload: schemas.AwayIn, current_user: models.User = Depends(auth.require_role("colleague")), db: Session = Depends(get_db)):
    """The colleague's screen just locked for a break (shown on Away Now)."""
    now = datetime.utcnow()
    started = now
    if payload.started_at:
        try:
            dt = datetime.fromisoformat(payload.started_at.replace("Z", "+00:00"))
            started = dt.astimezone(timezone.utc).replace(tzinfo=None) if dt.tzinfo else dt
        except ValueError:
            started = now
    if started > now or now - started > timedelta(hours=16):
        started = now
    row = db.query(models.AwayNow).filter(models.AwayNow.user_id == current_user.id).first()
    if row:
        row.started_at, row.source, row.updated_at = started, "manual" if payload.source == "manual" else "auto", now
    else:
        db.add(models.AwayNow(user_id=current_user.id, started_at=started, source="manual" if payload.source == "manual" else "auto", updated_at=now))
    db.commit()
    return {"ok": True}


@app.get("/breaks/away")
def away_list(
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    """Colleagues away right now: Team Lead -> their team, Admin / Super Admin -> everyone."""
    ids = [u.id for u in _batch_visible_colleagues(db, current_user)]
    rows = db.query(models.AwayNow).filter(models.AwayNow.user_id.in_(ids or [-1])).all()
    now = datetime.utcnow()
    users = {u.id: u for u in db.query(models.User).filter(models.User.id.in_([r.user_id for r in rows] or [-1])).all()}
    out = []
    for r in rows:
        u = users.get(r.user_id)
        if not u:
            continue
        out.append({
            "user_id": r.user_id, "employee": u.full_name, "employee_id": u.employee_id, "team_lead": u.reporting_manager or "",
            "photo": u.photo or "", "shift": u.shift or "", "started_at": r.started_at.isoformat() + "Z", "source": r.source,
            "minutes": round((now - r.started_at).total_seconds() / 60, 1),
            "no_signal_minutes": round((now - r.updated_at).total_seconds() / 60, 1),
        })
    out.sort(key=lambda x: -x["minutes"])
    return out


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
    current_user: models.User = Depends(auth.require_role("admin", "manager", "team_lead", "super_admin")),
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


def _send_account_email(user: models.User, temp_password: str):
    """(sent?, why not) — so the screen can say what actually happened instead of guessing."""
    if not user.email:
        return False, "this profile has no email address"
    if not email_utils.is_configured():
        return False, "email is not configured on the server"
    if email_utils.send_new_account_email(user.email, user.full_name, user.username, temp_password):
        return True, None
    return False, "the email server refused or could not be reached \u2014 see the Render logs"


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
    if payload.role not in ("colleague", "team_lead", "admin", "manager", "quality", "onshore", "recon", "calling", "super_admin"):
        raise HTTPException(status_code=400, detail="role must be 'colleague', 'team_lead', 'admin', 'manager', 'quality', 'onshore', 'recon', 'calling', or 'super_admin'")
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
        shift=_clean_shift(payload.shift),
        employment_status=payload.employment_status,
        employee_id=payload.employee_id,
    )
    if payload.process_ids:
        user.processes = db.query(models.Process).filter(models.Process.id.in_(payload.process_ids)).all()
    db.add(user)
    db.flush()
    _audit(db, current_user, "Profile", "Created profile", user, details=f"Role {user.role}; status {user.employment_status}; shift {user.shift or 'not set'}; Team Lead {user.reporting_manager or '-'}")
    db.commit()
    db.refresh(user)
    sent, note = _send_account_email(user, temp_password)
    return {"user": user, "temporary_password": temp_password, "email_sent": sent, "email_note": note}


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
    if payload.role not in ("colleague", "team_lead", "admin", "manager", "quality", "onshore", "recon", "calling", "super_admin"):
        raise HTTPException(status_code=400, detail="role must be 'colleague', 'team_lead', 'admin', 'manager', 'quality', 'onshore', 'recon', 'calling', or 'super_admin'")

    _before = {"name": user.full_name, "role": user.role, "email": user.email or "", "designation": user.designation or "", "Team Lead": user.reporting_manager or "",
               "shift": user.shift or "", "status": user.employment_status, "employee id": user.employee_id or "", "processes": ", ".join(sorted(p.name for p in user.processes))}
    user.full_name = payload.full_name
    user.role = payload.role
    user.email = payload.email
    user.dob = payload.dob
    user.doj = payload.doj
    user.anniversary_date = payload.anniversary_date
    user.designation = payload.designation
    user.reporting_manager = payload.reporting_manager
    user.shift = _clean_shift(payload.shift)
    if payload.employment_status == "Inactive" and user.id == current_user.id:
        raise HTTPException(status_code=400, detail="You cannot make your own profile Inactive")
    if payload.employment_status == "Inactive" and user.employment_status != "Inactive":
        # Lock the profile: end any open session and clear the Away Now entry.
        now = datetime.utcnow()
        for sl in db.query(models.SessionLog).filter(models.SessionLog.user_id == user.id, models.SessionLog.logout_at.is_(None)).all():
            sl.logout_at = min(now, sl.last_seen) if sl.last_seen else now
        _away_clear(db, user.id)
    user.employment_status = payload.employment_status
    user.employee_id = payload.employee_id
    user.processes = db.query(models.Process).filter(models.Process.id.in_(payload.process_ids)).all()
    _after = {"name": user.full_name, "role": user.role, "email": user.email or "", "designation": user.designation or "", "Team Lead": user.reporting_manager or "",
              "shift": user.shift or "", "status": user.employment_status, "employee id": user.employee_id or "", "processes": ", ".join(sorted(p.name for p in user.processes))}
    _diff = [f"{k}: {_before[k] or '-'} -> {_after[k] or '-'}" for k in _after if _before[k] != _after[k]]
    if _diff:
        _audit(db, current_user, "Profile", "Edited profile", user, details="; ".join(_diff))
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
    _audit(db, current_user, "Profile", "Reset password", user, details="Temporary password issued")
    db.commit()
    sent, note = _send_account_email(user, temp_password)
    return {"username": user.username, "temporary_password": temp_password, "email_sent": sent, "email_note": note}

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

# Night shift: the business date rolls over at a configurable time (default 3:30 AM IST, set by the
# Super Admin under Admin > Business Day), so work done between midnight and that time still belongs
# to the day the shift started on.
BUSINESS_DAY_SETTING_KEY = "business_day_rollover_minutes"      # minutes after midnight IST
DEFAULT_ROLLOVER_MINUTES = 210                                    # 03:30
_rollover_cache = {"minutes": DEFAULT_ROLLOVER_MINUTES, "at": 0.0}


def _rollover_minutes() -> int:
    import time as _time
    if _time.time() - _rollover_cache["at"] > 30:                # re-read now and then (other workers may change it)
        db = SessionLocal()
        try:
            row = db.query(models.AppSetting).filter(models.AppSetting.key == BUSINESS_DAY_SETTING_KEY).first()
            _rollover_cache["minutes"] = int(row.value) if row and str(row.value).isdigit() else DEFAULT_ROLLOVER_MINUTES
        except Exception:
            pass
        finally:
            db.close()
        _rollover_cache["at"] = _time.time()
    return _rollover_cache["minutes"]


def _work_today() -> date:
    """Today's business date (IST, rolling over at the configured time instead of midnight)."""
    return (datetime.now(IST) - timedelta(minutes=_rollover_minutes())).date()


def _fmt_hhmm(minutes: int) -> str:
    return f"{minutes // 60:02d}:{minutes % 60:02d}"


@app.get("/settings/business-day", response_model=schemas.BusinessDaySetting)
def get_business_day(current_user: models.User = Depends(auth.get_current_user)):
    """Any logged-in user can read this: the screens use it to work out 'today'."""
    return {"rollover_time": _fmt_hhmm(_rollover_minutes())}


@app.put("/settings/business-day", response_model=schemas.BusinessDaySetting)
def set_business_day(
    payload: schemas.BusinessDaySetting,
    current_user: models.User = Depends(auth.require_role("super_admin")),
    db: Session = Depends(get_db),
):
    try:
        hh, mm = payload.rollover_time.split(":")
        minutes = int(hh) * 60 + int(mm)
        assert 0 <= int(hh) <= 23 and 0 <= int(mm) <= 59
    except Exception:
        raise HTTPException(status_code=400, detail="Enter the time as HH:MM, e.g. 03:30")
    if minutes > 12 * 60:
        raise HTTPException(status_code=400, detail="The day change time must be between 00:00 and 12:00")
    row = db.query(models.AppSetting).filter(models.AppSetting.key == BUSINESS_DAY_SETTING_KEY).first()
    _audit(db, current_user, "Setting", "Changed business day change time", details=f"{_fmt_hhmm(int(row.value)) if row and (row.value or '').isdigit() else 'default'} -> {_fmt_hhmm(minutes)}")
    if row:
        row.value = str(minutes)
    else:
        db.add(models.AppSetting(key=BUSINESS_DAY_SETTING_KEY, value=str(minutes)))
    db.commit()
    _rollover_cache.update(minutes=minutes, at=__import__("time").time())
    return {"rollover_time": _fmt_hhmm(minutes)}


# ---------------------------------------------------------------------
# Holidays: Super Admin maintains the list, everybody can view it. Holidays
# (that fall on a weekday) are not counted as business days in TAT.
# ---------------------------------------------------------------------
_holiday_cache = {"dates": frozenset(), "at": 0.0}


def _holiday_dates() -> frozenset:
    import time as _time
    if _time.time() - _holiday_cache["at"] > 30:
        db = SessionLocal()
        try:
            _holiday_cache["dates"] = frozenset(r[0] for r in db.query(models.Holiday.holiday_date).all())
        except Exception:
            pass
        finally:
            db.close()
        _holiday_cache["at"] = _time.time()
    return _holiday_cache["dates"]


def _count_business_days_ex_holidays(start: date, end: date) -> int:
    """_count_business_days minus the holidays that fall on a counted weekday."""
    n = _count_business_days(start, end)
    if n <= 0:
        return n
    skipped = sum(1 for h in _holiday_dates() if start < h <= end and h.weekday() < 5)
    return max(0, n - skipped)


@app.get("/holidays", response_model=List[schemas.HolidayOut])
def list_holidays(year: Optional[int] = None, current_user: models.User = Depends(auth.get_current_user),
                  db: Session = Depends(get_db)):
    q = db.query(models.Holiday)
    if year:
        q = q.filter(models.Holiday.holiday_date >= date(year, 1, 1), models.Holiday.holiday_date <= date(year, 12, 31))
    return q.order_by(models.Holiday.holiday_date).all()


@app.post("/holidays", response_model=schemas.HolidayOut)
def add_holiday(payload: schemas.HolidayIn, current_user: models.User = Depends(auth.require_role("super_admin")),
                db: Session = Depends(get_db)):
    name = (payload.name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Enter the holiday name")
    if db.query(models.Holiday).filter(models.Holiday.holiday_date == payload.holiday_date).first():
        raise HTTPException(status_code=409, detail="A holiday is already listed on that date")
    h = models.Holiday(holiday_date=payload.holiday_date, name=name, created_by_name=current_user.full_name,
                       created_at=datetime.now(IST).replace(tzinfo=None))
    db.add(h)
    _audit(db, current_user, "Holiday", "Added holiday", ref_date=payload.holiday_date, details=name)
    db.commit()
    db.refresh(h)
    _holiday_cache["at"] = 0.0
    return h


@app.put("/holidays/{holiday_id}", response_model=schemas.HolidayOut)
def update_holiday(holiday_id: int, payload: schemas.HolidayIn,
                   current_user: models.User = Depends(auth.require_role("super_admin")), db: Session = Depends(get_db)):
    h = db.query(models.Holiday).filter(models.Holiday.id == holiday_id).first()
    if not h:
        raise HTTPException(status_code=404, detail="Holiday not found")
    name = (payload.name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Enter the holiday name")
    clash = db.query(models.Holiday).filter(models.Holiday.holiday_date == payload.holiday_date, models.Holiday.id != holiday_id).first()
    if clash:
        raise HTTPException(status_code=409, detail="A holiday is already listed on that date")
    _audit(db, current_user, "Holiday", "Changed holiday", ref_date=payload.holiday_date, details=f"{h.holiday_date} {h.name} -> {payload.holiday_date} {name}")
    h.holiday_date = payload.holiday_date
    h.name = name
    db.commit()
    db.refresh(h)
    _holiday_cache["at"] = 0.0
    return h


@app.delete("/holidays/{holiday_id}")
def delete_holiday(holiday_id: int, current_user: models.User = Depends(auth.require_role("super_admin")),
                   db: Session = Depends(get_db)):
    h = db.query(models.Holiday).filter(models.Holiday.id == holiday_id).first()
    if not h:
        raise HTTPException(status_code=404, detail="Holiday not found")
    _audit(db, current_user, "Holiday", "Removed holiday", ref_date=h.holiday_date, details=h.name)
    db.delete(h)
    db.commit()
    _holiday_cache["at"] = 0.0
    return {"ok": True}


# ---------------------------------------------------------------------
# Performance Evaluation (Team Lead / Admin / Super Admin): Production,
# Quality and Attendance come from the database; four more ratings are
# typed in by the supervisor.
#   Production: Target = daily target x hours worked / 8 (Batch Count Dashboard),
#               Achieved = transactions on those days.
#   Quality:    accounts audited / errors from the Batch Count Dashboard.
#   Attendance: Work Days = weekdays minus holidays, Worked Days = days the
#               colleague logged in (tracked from the day this feature went live).
# ---------------------------------------------------------------------
def _record_login_day(db: Session, user_id: int):
    try:
        d = _work_today()
        if not db.query(models.LoginDay).filter(models.LoginDay.user_id == user_id, models.LoginDay.login_date == d).first():
            db.add(models.LoginDay(user_id=user_id, login_date=d))
            db.commit()
    except Exception:
        db.rollback()


def _open_session(db: Session, user_id: int):
    """Start a clocked session at login; sessions left open by a closed browser end at their last heartbeat."""
    try:
        now = datetime.utcnow()
        for s in db.query(models.SessionLog).filter(models.SessionLog.user_id == user_id, models.SessionLog.logout_at.is_(None)).all():
            s.logout_at = s.last_seen
            s.auto_closed = True              # never logged out: the browser was closed or the PC went off
        db.add(models.SessionLog(user_id=user_id, login_at=now, last_seen=now))
        db.commit()
    except Exception:
        db.rollback()


@app.post("/sessions/heartbeat")
def session_heartbeat(current_user: models.User = Depends(auth.require_role("colleague")), db: Session = Depends(get_db)):
    s = db.query(models.SessionLog).filter(models.SessionLog.user_id == current_user.id, models.SessionLog.logout_at.is_(None)) \
        .order_by(models.SessionLog.id.desc()).first()
    if s is None:
        _open_session(db, current_user.id)
    else:
        s.last_seen = datetime.utcnow()
        db.commit()
    away = db.query(models.AwayNow).filter(models.AwayNow.user_id == current_user.id).first()
    if away:
        away.updated_at = datetime.utcnow()
        db.commit()
    return {"ok": True}


@app.post("/sessions/end")
def session_end(current_user: models.User = Depends(auth.require_role("colleague")), db: Session = Depends(get_db)):
    now = datetime.utcnow()
    open_rows = db.query(models.SessionLog).filter(models.SessionLog.user_id == current_user.id, models.SessionLog.logout_at.is_(None)) \
        .order_by(models.SessionLog.id.desc()).all()
    for i, s in enumerate(open_rows):
        if i == 0:                       # the current session ends now
            s.last_seen = now
            s.logout_at = now
        else:                            # older ones were left open by a closed browser: they ended at their last heartbeat
            s.logout_at = s.last_seen
            s.auto_closed = True
    _away_clear(db, current_user.id)
    db.commit()
    return {"ok": True}


@app.get("/clocked")
def clocked_hours(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    current_user: models.User = Depends(auth.require_role("colleague", "team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    Clocked time per colleague per IST day = login to logout (overlapping sessions
    counted once; a session that was never logged out ends at its last heartbeat).
    Scope: colleague own / Team Lead team / Admin + Super Admin everyone.
    """
    today = datetime.now(IST).date()
    try:
        d1 = date.fromisoformat(date_from) if date_from else today.replace(day=1)
        d2 = date.fromisoformat(date_to) if date_to else today
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date")
    if (d2 - d1).days > 92:
        raise HTTPException(status_code=400, detail="Choose a range of 92 days or less")
    def _utc(d):
        return datetime(d.year, d.month, d.day, tzinfo=IST).astimezone(timezone.utc).replace(tzinfo=None)
    q = db.query(models.SessionLog).filter(models.SessionLog.login_at >= _utc(d1), models.SessionLog.login_at < _utc(d2 + timedelta(days=1)))
    if current_user.role == "colleague":
        q = q.filter(models.SessionLog.user_id == current_user.id)
    elif current_user.role == "team_lead":
        ids = [u.id for u in _batch_visible_colleagues(db, current_user)]
        q = q.filter(models.SessionLog.user_id.in_(ids or [-1]))
    by = {}
    now = datetime.utcnow()
    for s in q.all():
        end = s.logout_at or s.last_seen
        if end < s.login_at:
            end = s.login_at
        ist_day = (s.login_at.replace(tzinfo=timezone.utc).astimezone(IST)).date().isoformat()
        live = s.logout_at is None and (now - s.last_seen) < timedelta(minutes=5)
        by.setdefault((s.user_id, ist_day), []).append((s.login_at, end, live, bool(s.auto_closed) or (s.logout_at is None and not live)))
    users = {u.id: u for u in db.query(models.User).filter(models.User.id.in_({k[0] for k in by} or {-1})).all()}
    out = []
    for (uid, day), ivs in by.items():
        ivs.sort()
        merged = []
        for a, b, _, _nl in ivs:
            if merged and a <= merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], b)
            else:
                merged.append([a, b])
        u = users.get(uid)
        out.append({
            "user_id": uid, "employee": u.full_name if u else "", "team_lead": (u.reporting_manager if u else None) or "",
            "date": day, "first_login": ivs[0][0].isoformat(), "last_logout": merged[-1][1].isoformat(),
            "minutes": round(sum((b - a).total_seconds() for a, b in merged) / 60, 2), "ongoing": any(x[2] for x in ivs),
            "no_logout": any(x[3] for x in ivs) and day < today.isoformat(),
        })
    return out


PERF_ROLES = ("colleague", "team_lead", "admin", "manager", "super_admin")   # data: colleagues see only themselves
PERF_RATE_ROLES = ("team_lead", "admin", "manager", "super_admin")
PERF_FIELDS = ("pk", "shift", "flex", "me")


def _perf_prev_month():
    t = _work_today()
    return (t.year - 1, 11) if t.month == 1 else (t.year, t.month - 2)   # (year, month 0-11)


def _perf_work_days(start: date, end: date, holidays: frozenset) -> int:
    if end < start:
        return 0
    n, d = 0, start
    while d <= end:
        if d.weekday() < 5 and d not in holidays:
            n += 1
        d += timedelta(days=1)
    return n


def _as_date(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    try:
        return date.fromisoformat(str(v)[:10])
    except Exception:
        return None


@app.get("/performance/data")
def performance_data(current_user: models.User = Depends(auth.require_role(*PERF_ROLES)), db: Session = Depends(get_db)):
    try:
        return _performance_data(current_user, db)
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Performance data error: {type(e).__name__}: {e}")


def _performance_data(current_user: models.User, db: Session):
    colleagues = _batch_visible_colleagues(db, current_user)
    ids = [u.id for u in colleagues]
    out = {"employees": [], "rows": [], "ratings": {}, "can_rate": current_user.role in PERF_RATE_ROLES, "role": "admin" if current_user.role == "manager" else current_user.role,
           "rate_year": _perf_prev_month()[0], "rate_month": _perf_prev_month()[1], "attendance_since": None}
    if not ids:
        return out
    tl_email = {u.full_name: u.email for u in db.query(models.User).filter(models.User.role == "team_lead").all()}
    for u in colleagues:
        out["employees"].append({
            "id": u.id, "emp_id": (u.employee_id or u.username or str(u.id)).strip(), "name": u.full_name,
            "tl": u.reporting_manager or "No Team Lead", "email": u.email, "tl_email": tl_email.get(u.reporting_manager),
            "active": u.employment_status != "Inactive",
        })
    months = {}          # (uid, y, m) -> record

    def rec(uid, y, m):
        return months.setdefault((uid, y, m), {"emp": uid, "year": y, "m": m, "pt": None, "pa": None, "qa": None, "qd": None, "aw": None, "awd": None, "qe": None, "qr": None})

    # Production + Quality from the Batch Count Dashboard rows
    for r in _batch_base_rows(db, current_user, None, None, None, None):
        d = _as_date(r["work_date"])
        if d is None:
            continue
        x = rec(r["user_id"], d.year, d.month - 1)
        exp = r.get("_expected")
        if exp:
            x["pt"] = (x["pt"] or 0) + exp
            x["pa"] = (x["pa"] or 0) + (r["total_trans_count"] or 0)
        if r["accounts_audited"] and r["accounts_audited"] > 0 and r["errors"] is not None:
            x["qa"] = (x["qa"] or 0) + r["accounts_audited"]
            x["qd"] = (x["qd"] or 0) + r["errors"]

    # Logged error detail (type / rework) per month, shown beside the Quality figures
    for e in db.query(models.QualityError).filter(models.QualityError.user_id.in_(ids)).all():
        d = _as_date(e.error_date)
        if d is None:
            continue
        x = rec(e.user_id, d.year, d.month - 1)
        x["qe"] = (x["qe"] or 0) + 1
        x["qr"] = (x["qr"] or 0) + (e.rework_count or 0)

    # Attendance from login days
    first = _as_date(db.query(func.min(models.LoginDay.login_date)).scalar())
    if first:
        out["attendance_since"] = first.isoformat()
        today = _work_today()
        holidays = _holiday_dates()
        logins = {}
        for uid, d in db.query(models.LoginDay.user_id, models.LoginDay.login_date).filter(models.LoginDay.user_id.in_(ids)).all():
            d = _as_date(d)
            if d:
                logins.setdefault(uid, set()).add(d)
        for u in colleagues:
            doj = _as_date(u.doj)
            start = max(first, doj) if doj else first
            y, m = start.year, start.month
            while (y, m) <= (today.year, today.month):
                m_start = date(y, m, 1)
                m_end = (date(y + (m == 12), (m % 12) + 1, 1) - timedelta(days=1))
                a, b = max(m_start, start), min(m_end, today)
                wd = _perf_work_days(a, b, holidays)
                if wd > 0:
                    worked = sum(1 for d in logins.get(u.id, ()) if a <= d <= b)
                    x = rec(u.id, y, m - 1)
                    x["aw"], x["awd"] = wd, worked   # not capped: logins on holidays/weekends count
                y, m = (y + 1, 1) if m == 12 else (y, m + 1)

    out["rows"] = [v for v in months.values() if any(v[k] for k in ("pt", "qa", "aw", "qe"))]
    for r in db.query(models.PerformanceRating).filter(models.PerformanceRating.user_id.in_(ids)).all():
        out["ratings"].setdefault(str(r.user_id), {})[f"{r.year}-{r.month}"] = {k: getattr(r, k) for k in PERF_FIELDS}
    return out


@app.put("/performance/ratings")
def save_performance_rating(payload: dict, current_user: models.User = Depends(auth.require_role(*PERF_RATE_ROLES)),
                            db: Session = Depends(get_db)):
    try:
        uid, year, month = int(payload["user_id"]), int(payload["year"]), int(payload["month"])
    except Exception:
        raise HTTPException(status_code=400, detail="user_id, year and month are required")
    if not (0 <= month <= 11):
        raise HTTPException(status_code=400, detail="month must be 0-11")
    if uid not in [u.id for u in _batch_visible_colleagues(db, current_user)]:
        raise HTTPException(status_code=403, detail="You can only rate your own team")
    is_lead = current_user.role == "team_lead"
    if is_lead and (year, month) != _perf_prev_month():
        raise HTTPException(status_code=403, detail="Ratings can only be given for the previous month")
    row = db.query(models.PerformanceRating).filter_by(user_id=uid, year=year, month=month).first()
    if row and is_lead:
        for k in PERF_FIELDS:
            if k in payload and getattr(row, k) is not None:
                raise HTTPException(status_code=409, detail="This rating is already given and locked")
    if not row:
        row = models.PerformanceRating(user_id=uid, year=year, month=month)
        db.add(row)
    for k in PERF_FIELDS:
        if k in payload:
            v = payload[k]
            if v in (None, ""):
                setattr(row, k, None)
            else:
                try:
                    v = int(v)
                except Exception:
                    raise HTTPException(status_code=400, detail=f"{k} must be 1-5")
                if not (1 <= v <= 5):
                    raise HTTPException(status_code=400, detail=f"{k} must be 1-5")
                setattr(row, k, v)
    row.updated_by = current_user.full_name
    row.updated_at = datetime.now(IST).replace(tzinfo=None)
    db.commit()
    return {k: getattr(row, k) for k in PERF_FIELDS}


# ---------------------------------------------------------------------------
# 835 Postings: details form (several lines) for EDM orders with 835 Posted $ above zero
# ---------------------------------------------------------------------------
def _p835_out(r, tl_by_user=None, order=None):
    return {
        "id": r.id, "order_id": r.order_id, "line_no": r.line_no, "edm": r.edm, "division": r.division, "exchange": r.exchange,
        "check_number_edm": r.check_number_edm, "check_number_835": r.check_number_835, "amount": r.amount,
        "edm_description": r.edm_description, "deposit_date": r.deposit_date.isoformat() if r.deposit_date else None,
        "payor_name": r.payor_name, "worked_by": r.worked_by,
        "team_lead": (tl_by_user or {}).get(r.worked_by_id, ""),
        "date_updated": r.updated_at.isoformat() if r.updated_at else None,
        "order_amount": order.amount if order else None,
        "manual_posted": order.posted_amount if order else None,
        "posted_835": order.posted_835_amount if order else None,
    }


def _p835_lines_total(db, order_id) -> float:
    return round(sum(float(x.amount or 0) for x in db.query(models.Posting835Line).filter_by(order_id=order_id).all()), 2)


@app.get("/orders/{order_id}/posting-835")
def get_posting_835(order_id: int, current_user: models.User = Depends(auth.require_role("colleague", "team_lead", "admin", "manager", "super_admin")),
                    db: Session = Depends(get_db)):
    order = db.query(models.WorkOrder).filter(models.WorkOrder.id == order_id).first()
    if not order or (current_user.role == "colleague" and order.assigned_to_id != current_user.id):
        raise HTTPException(status_code=404, detail="Order not found")
    rows = db.query(models.Posting835Line).filter_by(order_id=order_id).order_by(models.Posting835Line.line_no, models.Posting835Line.id).all()
    return [_p835_out(r) for r in rows]


@app.put("/orders/{order_id}/posting-835")
def save_posting_835(order_id: int, payload: dict, current_user: models.User = Depends(auth.require_role("colleague")),
                     db: Session = Depends(get_db)):
    """Replaces the lines of the 835 form. payload: {"total": <835 Posted $>, "lines": [{exchange, check_number_edm, check_number_835, amount, payor_name}, ...]}"""
    order = db.query(models.WorkOrder).filter(models.WorkOrder.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    if order.assigned_to_id != current_user.id:
        raise HTTPException(status_code=403, detail="This order is not assigned to you")
    if order.escalated or order.posting_status == "Completed":
        raise HTTPException(status_code=403, detail="This order is locked")
    lines = payload.get("lines") or []
    if not lines:
        raise HTTPException(status_code=400, detail="Add at least one line")
    clean, total = [], 0.0
    for i, ln in enumerate(lines, 1):
        vals = {k: str(ln.get(k) or "").strip() for k in ("exchange", "check_number_edm", "check_number_835", "payor_name")}
        missing = [lbl for k, lbl in (("exchange", "Exchange"), ("check_number_edm", "Check Number as per EDM"),
                                      ("check_number_835", "Check Number as per 835_push"), ("payor_name", "Payor name")) if not vals[k]]
        if missing:
            raise HTTPException(status_code=400, detail=f"Line {i}: required " + ", ".join(missing))
        try:
            amt = float(ln.get("amount"))
        except Exception:
            raise HTTPException(status_code=400, detail=f"Line {i}: Amount must be a number")
        if amt <= 0:
            raise HTTPException(status_code=400, detail=f"Line {i}: Amount must be greater than 0")
        total += amt
        clean.append((vals, amt))
    total = round(total, 2)
    try:
        expected = round(float(payload.get("total")), 2)
    except Exception:
        expected = None
    if expected is not None and total > expected + 0.005:
        raise HTTPException(status_code=400, detail=f"The line amounts add up to {total:.2f}, which is more than 835 Posted $ ({expected:.2f})")
    db.query(models.Posting835Line).filter_by(order_id=order_id).delete()
    now = datetime.now(IST).replace(tzinfo=None)
    for i, (vals, amt) in enumerate(clean, 1):
        db.add(models.Posting835Line(
            order_id=order_id, process_id=order.process_id, line_no=i, edm=order.edm, division=order.division,
            edm_description=order.description, deposit_date=order.deposit_date, worked_by=current_user.full_name,
            worked_by_id=current_user.id, amount=amt, updated_at=now, **vals))
    db.commit()
    return {"lines": len(clean), "total": total}


@app.get("/posting-835")
def list_posting_835(process_id: Optional[int] = None, date_from: Optional[date] = None, date_to: Optional[date] = None,
                     current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
                     db: Session = Depends(get_db)):
    L = models.Posting835Line
    q = db.query(L)
    if process_id is not None:
        q = q.filter(L.process_id == process_id)
    if current_user.role == "team_lead":
        ids = [u.id for u in _batch_visible_colleagues(db, current_user)]
        q = q.filter(L.worked_by_id.in_(ids or [-1]))
    if date_from:
        q = q.filter(L.updated_at >= datetime.combine(date_from, datetime.min.time()))
    if date_to:
        q = q.filter(L.updated_at < datetime.combine(date_to + timedelta(days=1), datetime.min.time()))
    rows = q.order_by(L.updated_at.desc(), L.order_id, L.line_no).limit(5000).all()
    uids = {r.worked_by_id for r in rows if r.worked_by_id}
    tl_by_user = {u.id: (u.reporting_manager or "") for u in db.query(models.User).filter(models.User.id.in_(uids or [-1])).all()}
    orders = {o.id: o for o in db.query(models.WorkOrder).filter(models.WorkOrder.id.in_({r.order_id for r in rows} or [-1])).all()}
    return [_p835_out(r, tl_by_user, orders.get(r.order_id)) for r in rows]


# Reactions a person can leave on a celebration comment (clicking again removes it).
REACTION_TYPES = ("like", "heart")
CELEBRATION_IMAGE_MAX_BYTES = 2 * 1024 * 1024
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
                "has_image": bool(c.image_b64),
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
    message = (payload.message or "").strip()
    image_b64, image_type = None, None
    if payload.image:
        m = re.match(r"^data:(image/(?:gif|png|jpeg|webp));base64,([A-Za-z0-9+/=\s]+)$", payload.image.strip())
        if not m:
            raise HTTPException(status_code=400, detail="Only GIF, PNG, JPEG or WebP pictures can be attached")
        try:
            raw = base64.b64decode(m.group(2))
        except Exception:
            raise HTTPException(status_code=400, detail="The picture could not be read")
        if len(raw) > CELEBRATION_IMAGE_MAX_BYTES:
            raise HTTPException(status_code=400, detail="The picture is too large (2 MB max)")
        magic_ok = {"image/gif": raw[:4] == b"GIF8", "image/png": raw[:8] == b"\x89PNG\r\n\x1a\n",
                    "image/jpeg": raw[:3] == b"\xff\xd8\xff", "image/webp": raw[:4] == b"RIFF" and raw[8:12] == b"WEBP"}[m.group(1)]
        if not magic_ok:
            raise HTTPException(status_code=400, detail="That file is not a valid picture")
        image_b64, image_type = base64.b64encode(raw).decode(), m.group(1)
    if not message and not image_b64:
        raise HTTPException(status_code=400, detail="Comment can't be empty")
    target = db.query(models.User).filter(models.User.id == target_user_id).first()
    if not target:
        raise HTTPException(status_code=404, detail="User not found")
    today_ist = datetime.now(IST).date()
    # The same person can't post the same comment twice on the same celebration (case/space-insensitive).
    norm = " ".join(message.split()).lower()
    for prev in db.query(models.CelebrationComment.message, models.CelebrationComment.image_b64).filter(
        models.CelebrationComment.target_user_id == target_user_id,
        models.CelebrationComment.posted_by_id == current_user.id,
        models.CelebrationComment.occasion_date == today_ist,
    ).all():
        if " ".join((prev.message or "").split()).lower() == norm and (prev.image_b64 or None) == image_b64:
            raise HTTPException(status_code=409, detail="You have already posted this comment. Please write something different.")
    comment = models.CelebrationComment(
        target_user_id=target_user_id,
        occasion_date=today_ist,
        message=message,
        image_b64=image_b64, image_type=image_type,
        posted_by_id=current_user.id,
        posted_by_name=current_user.full_name,
    )
    db.add(comment)
    db.commit()
    db.refresh(comment)
    return {
        "id": comment.id, "target_user_id": comment.target_user_id, "message": comment.message,
        "posted_by_name": comment.posted_by_name, "created_at": comment.created_at,
        "has_image": bool(image_b64),
        "reaction_counts": {t: 0 for t in REACTION_TYPES}, "my_reactions": [],
    }


@app.get("/celebrations/comments/{comment_id}/image")
def get_celebration_comment_image(
    comment_id: int,
    current_user: models.User = Depends(auth.get_current_user),
    db: Session = Depends(get_db),
):
    """The picture / GIF / poster on a celebration comment (any logged-in user; fetched as a blob via authFetch)."""
    c = db.query(models.CelebrationComment).filter(models.CelebrationComment.id == comment_id).first()
    if not c or not c.image_b64:
        raise HTTPException(status_code=404, detail="No picture on this comment")
    return Response(content=base64.b64decode(c.image_b64), media_type=c.image_type or "image/png",
                    headers={"Cache-Control": "private, max-age=86400"})


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
    rows = (
        db.query(models.ProcessUpdate)
        .filter(models.ProcessUpdate.process_id == process_id)
        .order_by(models.ProcessUpdate.created_at.desc())
        .limit(20)
        .all()
    )
    _attach_ack_info(db, current_user, process_id, rows)
    return rows


def _ack_scope_colleagues(db: Session, current_user: models.User, process_id: int):
    """Colleagues of this process whose acknowledgement is tracked (Team Lead: their team; Admin / Super Admin: everyone)."""
    return [u for u in _batch_visible_colleagues(db, current_user)
            if u.role == "colleague" and u.employment_status != "Inactive" and any(p.id == process_id for p in u.processes)]


def _attach_ack_info(db: Session, current_user: models.User, process_id: int, rows):
    ids = [r.id for r in rows]
    acks = db.query(models.ProcessUpdateAck).filter(models.ProcessUpdateAck.update_id.in_(ids or [-1])).all()
    by_update = {}
    for a in acks:
        by_update.setdefault(a.update_id, {})[a.user_id] = a
    scope = _ack_scope_colleagues(db, current_user, process_id) if current_user.role in ("team_lead", "admin", "manager", "super_admin") else []
    scope_ids = {u.id for u in scope}
    for r in rows:
        got = by_update.get(r.id, {})
        if current_user.role == "colleague":
            mine = got.get(current_user.id)
            r.acknowledged = bool(mine)
            r.acknowledged_at = mine.acknowledged_at if mine else None
        elif r.ack_required:
            r.ack_count = len([1 for uid in got if uid in scope_ids])
            r.ack_total = len(scope_ids)


@app.post("/process-updates/{update_id}/acknowledge")
def acknowledge_process_update(update_id: int, current_user: models.User = Depends(auth.require_role("colleague")),
                               db: Session = Depends(get_db)):
    upd = db.query(models.ProcessUpdate).filter(models.ProcessUpdate.id == update_id).first()
    if not upd:
        raise HTTPException(status_code=404, detail="Update not found")
    _require_process_access(current_user, upd.process_id)
    if not db.query(models.ProcessUpdateAck).filter_by(update_id=update_id, user_id=current_user.id).first():
        db.add(models.ProcessUpdateAck(update_id=update_id, user_id=current_user.id, user_name=current_user.full_name,
                                       acknowledged_at=datetime.utcnow()))
        db.commit()
    return {"status": "acknowledged"}


@app.get("/process-updates/{update_id}/acknowledgements")
def process_update_acknowledgements(update_id: int, current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
                                    db: Session = Depends(get_db)):
    """Who has / hasn't acknowledged an update (Team Lead: their team; Admin / Super Admin: all colleagues of the process)."""
    upd = db.query(models.ProcessUpdate).filter(models.ProcessUpdate.id == update_id).first()
    if not upd:
        raise HTTPException(status_code=404, detail="Update not found")
    _require_process_access(current_user, upd.process_id)
    scope = _ack_scope_colleagues(db, current_user, upd.process_id)
    got = {a.user_id: a for a in db.query(models.ProcessUpdateAck).filter_by(update_id=update_id).all()}
    done, pending = [], []
    for u in sorted(scope, key=lambda x: x.full_name.lower()):
        row = {"user_id": u.id, "name": u.full_name, "emp_id": (u.employee_id or u.username or "").strip(), "team_lead": u.reporting_manager or ""}
        a = got.get(u.id)
        if a:
            row["acknowledged_at"] = a.acknowledged_at.isoformat() if a.acknowledged_at else None
            done.append(row)
        else:
            pending.append(row)
    return {"update_id": update_id, "ack_required": bool(upd.ack_required), "acknowledged": done, "pending": pending}


@app.post("/process-updates", response_model=List[schemas.ProcessUpdateOut])
def create_process_update(
    payload: schemas.ProcessUpdateCreate,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
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
            ack_required=(payload.status == "Active"),
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
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
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
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
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
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
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
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
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
    _old_name = order.employee_name or "unassigned"

    order.assigned_to_id = new_colleague.id
    order.assigned_date = _work_today()
    order.employee_id = new_colleague.employee_id or new_colleague.username
    order.employee_name = new_colleague.full_name
    order.last_edited_by = current_user.username
    _start_timer(order)
    if reasons:
        _log_order_change(db, current_user, order, "reassign", reasons, before)
    _audit_order(db, current_user, "Reassigned order", order, f"{_old_name} -> {new_colleague.full_name}")
    db.commit()
    db.refresh(order)
    return order


@app.post("/orders/transfer")
def transfer_orders(
    process_id: int,
    payload: schemas.TransferOrdersRequest,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
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
            raise HTTPException(status_code=400, detail="from_team_lead_id is required for an Admin / Super Admin transfer")
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

    if orders:
        _audit(db, current_user, "Order", "Transferred orders to another Team Lead", None, None,
               f"{len(orders)} order(s) moved to {to_lead.full_name} (from Team Lead id {from_team_lead_id}); e.g. " + ", ".join(str(o.edm or o.id) for o in orders[:5]))
    db.commit()
    _auto_assign_open_slots(db, process_id)
    return {"transferred": len(orders)}


@app.delete("/orders/all")
def delete_all_orders(
    process_id: int,
    current_user: models.User = Depends(auth.require_role("super_admin")),
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
    _n_del = db.query(models.WorkOrder).filter(models.WorkOrder.process_id == process_id).count()
    _pname = db.query(models.Process.name).filter(models.Process.id == process_id).scalar()
    if _n_del:
        _audit(db, current_user, "Order", "Deleted ALL orders of a process", None, None, f"{_n_del} order(s) deleted from {_pname}")
    # A bulk .delete() skips the ORM cascade, so the child rows (clarification /
    # escalation details) have to go first or Postgres rejects the delete with a
    # foreign-key error (the "Internal Server Error").
    ids = db.query(models.WorkOrder.id).filter(models.WorkOrder.process_id == process_id)
    db.query(models.EscalationEvent).filter(models.EscalationEvent.order_id.in_(ids)).delete(synchronize_session=False)
    db.query(models.OnshoreAttachment).filter(models.OnshoreAttachment.order_id.in_(ids)).delete(synchronize_session=False)
    db.query(models.OnshoreMessage).filter(models.OnshoreMessage.order_id.in_(ids)).delete(synchronize_session=False)
    db.query(models.ClarificationDetail).filter(models.ClarificationDetail.order_id.in_(ids)).delete(synchronize_session=False)
    db.query(models.EscalationDetail).filter(models.EscalationDetail.order_id.in_(ids)).delete(synchronize_session=False)
    deleted_count = db.query(models.WorkOrder).filter(models.WorkOrder.process_id == process_id).delete(synchronize_session=False)
    db.commit()
    return {"deleted": deleted_count}


_ORDER_EDIT_FIELDS = ("edm", "status", "created", "received_date", "deposit_date", "amount", "def_doc_type", "description", "division",
                      "division_name", "run", "inits", "ecom_type", "eobs", "checks", "not_posted", "not_posted_amount", "offset_amount")


def _edit_order_fields(order, data: dict, current_user, db: Session):
    """Shared by the single edit and the bulk edit. Raises HTTPException(400/403)."""
    if order.submitted:
        raise HTTPException(status_code=403, detail="Submitted to Production - edit it from the Production screen")
    data = {k: v for k, v in data.items() if k in _ORDER_EDIT_FIELDS}
    if "edm" in data:
        data["edm"] = (data["edm"] or "").strip()
        if not data["edm"]:
            raise HTTPException(status_code=400, detail="EDM / Batch cannot be empty")
        if _norm_edm(data["edm"]) != _norm_edm(order.edm):
            ecom = ((order.process.name if order.process else "") or "").strip().upper() == "ECOM"
            run_v = data.get("run", order.run)
            for o in db.query(models.WorkOrder).filter(models.WorkOrder.process_id == order.process_id, models.WorkOrder.id != order.id).all():
                if _dup_key(o.edm, o.run, ecom) == _dup_key(data["edm"], run_v, ecom):
                    raise HTTPException(status_code=400, detail=f"{data['edm']} already exists in this process")
    if "created" in data and data["created"] is not None:
        data["created"] = datetime.combine(data["created"], datetime.min.time())
    before = _order_snapshot(order)
    for k, v in data.items():
        if isinstance(v, str):
            v = v.strip() or None
        setattr(order, k, v)
    order.last_edited_by = current_user.username
    _recalc_pending(order)
    d = _order_diff_text(before, order)
    if d:
        _audit_order(db, current_user, "Edited order details", order, d)
    return bool(d)


@app.patch("/orders/{order_id}/edit", response_model=schemas.WorkOrderOut)
def edit_order(
    order_id: int,
    payload: schemas.OrderEdit,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    """Correct the import details (EDM, amount, division, dates...) of one order that is not yet in Production."""
    order = db.query(models.WorkOrder).filter(models.WorkOrder.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    _require_process_access(current_user, order.process_id)
    _edit_order_fields(order, payload.dict(exclude_unset=True), current_user, db)
    db.commit()
    db.refresh(order)
    return order


@app.post("/orders/bulk")
def bulk_orders(
    payload: schemas.OrderBulk,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    """Reassign, delete or edit several orders in one go. Each order is handled on its own; failures are listed, the rest go through."""
    ids = list(dict.fromkeys(payload.ids))
    if not ids:
        raise HTTPException(status_code=400, detail="No orders selected")
    if len(ids) > 500:
        raise HTTPException(status_code=400, detail="Select at most 500 orders at a time")
    if payload.action not in ("reassign", "delete", "edit"):
        raise HTTPException(status_code=400, detail="action must be reassign, delete or edit")
    changes = payload.changes.dict(exclude_unset=True) if payload.changes else {}
    if payload.action == "edit" and not changes:
        raise HTTPException(status_code=400, detail="Nothing to change")
    if payload.action == "edit" and "edm" in changes and len(ids) > 1:
        raise HTTPException(status_code=400, detail="EDM / Batch is unique per order - edit it on one order at a time")
    if payload.action == "reassign" and not payload.assigned_to_id:
        raise HTTPException(status_code=400, detail="Choose a colleague")
    done, failed = 0, []
    for oid in ids:
        try:
            if payload.action == "reassign":
                reassign_order(oid, schemas.ReassignRequest(assigned_to_id=payload.assigned_to_id), current_user, db)
            elif payload.action == "delete":
                delete_one_order(oid, current_user, db)
            else:
                order = db.query(models.WorkOrder).filter(models.WorkOrder.id == oid).first()
                if not order:
                    raise HTTPException(status_code=404, detail="Order not found")
                _require_process_access(current_user, order.process_id)
                _edit_order_fields(order, dict(changes), current_user, db)
                db.commit()
            done += 1
        except HTTPException as e:
            db.rollback()
            o = db.query(models.WorkOrder.edm).filter(models.WorkOrder.id == oid).first()
            failed.append({"id": oid, "edm": o[0] if o else None, "error": str(e.detail)})
    return {"done": done, "failed": failed}


@app.delete("/orders/{order_id}")
def delete_one_order(
    order_id: int,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
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
    _audit_order(db, current_user, "Deleted order", order, f"status {order.posting_status or 'Pending'}, amount {order.amount}")
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
CB_UNMATCHED_ALLOWED_DOC_TYPES = {"unmatched eob"}
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
    # ECOM active-queue headers
    "division #": "division", "division no": "division", "division name": "division_name",
    "run": "run", "inits": "inits", "type": "ecom_type",
    "bank deposit date": "deposit_date", "create date": "created",
    "eobs": "eobs", "check(s)": "checks", "checks": "checks", "check": "checks",
    "notposted": "not_posted", "not posted": "not_posted",
    "notposted$": "not_posted_amount", "not posted $": "not_posted_amount", "not posted$": "not_posted_amount",
    "offset $": "offset_amount", "offset$": "offset_amount", "offset": "offset_amount",
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


class _InvRows(list):
    """List of inventory rows that also remembers which internal fields the header mapped to."""
    fields: set = set()


def _read_inventory_rows(raw: bytes, filename: str, ecom: bool = False):
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

    rows = _InvRows()
    rows.fields = set(mapping.values())
    for r in matrix[header_idx + 1:]:
        if not any(c for c in r):
            continue
        d = {f: (r[j] if j < len(r) else "") for j, f in mapping.items()}
        desc = d.get("description") or ""
        if "_" in desc and not ecom:
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


def _is_ecom_process(process) -> bool:
    return bool(process and (process.name or "").strip().upper() == "ECOM")


def _pending_base(order):
    """What Posted $ is measured against: Amount, or for ECOM NotPosted$ + Offset $."""
    if _is_ecom_process(order.process):
        if order.not_posted_amount is None and order.offset_amount is None:
            return None
        return float(order.not_posted_amount or 0) + float(order.offset_amount or 0)
    return None if order.amount is None else float(order.amount)


def _recalc_pending(order):
    base = _pending_base(order)
    if base is not None:
        order.pending_amount = round(base - float(order.posted_amount or 0) - float(order.posted_835_amount or 0), 2)


def _dup_key(edm, run, ecom: bool) -> str:
    """Duplicate key: EDM / Batch for EDM; Run + Batch for ECOM."""
    return f"{_norm_edm(run)}|{_norm_edm(edm)}" if ecom else _norm_edm(edm)


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


def _tol(fn, v):
    """Parse an imported cell; junk like '-', 'N/A' becomes blank instead of failing the whole import."""
    try:
        return fn(v)
    except (ValueError, TypeError, OverflowError):
        return None


def _order_from_inventory_row(process_id, team_lead_id, received, row, username):
    """One inventory line -> an unassigned WorkOrder for the given Team Lead."""
    return models.WorkOrder(
        process_id=process_id,
        team_lead_id=team_lead_id,
        received_date=received,
        edm=row.get("edm") or None,
        status=row.get("status") or None,
        created=_tol(_parse_dt, row.get("created")),
        image_count=_tol(_parse_int, row.get("image_count")),
        doc_count=_tol(_parse_int, row.get("doc_count")),
        def_doc_type=row.get("def_doc_type") or None,
        amount=_tol(_parse_float, row.get("amount")),
        description=row.get("description") or None,
        division=row.get("division") or None,
        deposit_date=_safe_date(row.get("deposit_date")),
        division_name=row.get("division_name") or None,
        run=row.get("run") or None,
        inits=row.get("inits") or None,
        ecom_type=row.get("ecom_type") or None,
        eobs=_tol(_parse_int, row.get("eobs")),
        checks=_tol(_parse_int, row.get("checks")),
        not_posted=_tol(_parse_int, row.get("not_posted")),
        not_posted_amount=_tol(_parse_float, row.get("not_posted_amount")),
        offset_amount=_tol(_parse_float, row.get("offset_amount")),
        last_edited_by=username,
    )


@app.post("/orders/import")
def import_inventory(
    process_id: int,
    file: UploadFile = File(...),
    on_behalf_of_team_lead_id: Optional[int] = Form(None),
    current_user: models.User = Depends(auth.require_role("team_lead", "super_admin", "admin", "manager")),
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
          under Import Review Queue for the Team Lead, Admin and Super Admin.
          A repeat of an EDM inside the same file is skipped too.
    Orders imported for a Team Lead are tagged to them, so auto-assignment
    only offers them to colleagues reporting to that Team Lead.
    """
    _require_process_access(current_user, process_id)

    fixed_team_lead_id = None       # set when the whole file is imported for one Team Lead
    if current_user.role == "team_lead":
        fixed_team_lead_id = current_user.id
    elif current_user.role in ("admin", "manager") and on_behalf_of_team_lead_id is not None:
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
    is_cb_unmatched = bool(process and process.name.strip().lower() == "cb unmatched posting")
    is_ecom = _is_ecom_process(process)

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
    today = _work_today()
    existing = {}
    if is_edm or is_ecom:
        cutoff = today - relativedelta(months=DUPLICATE_LOOKBACK_MONTHS)
        for o in db.query(
            models.WorkOrder.id, models.WorkOrder.edm, models.WorkOrder.posting_status,
            models.WorkOrder.posted_date, models.WorkOrder.employee_name, models.WorkOrder.run,
        ).filter(
            models.WorkOrder.process_id == process_id,
            models.WorkOrder.received_date >= cutoff,
            models.WorkOrder.edm.isnot(None),
        ).all():
            existing.setdefault(_dup_key(o.edm, o.run, is_ecom), []).append(o)

    reader = _read_inventory_rows(file.file.read(), file.filename, ecom=is_ecom)
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
            row_json=json.dumps({k: v for k, v in row.items() if v not in (None, "")}),
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
        if is_cb_unmatched and _norm_text(row.get("def_doc_type")) not in CB_UNMATCHED_ALLOWED_DOC_TYPES:
            counts["skipped_doc_type"] += 1
            continue

        # 2) the client must be assigned to a Team Lead for this process
        client = _client_for_row(row.get("division"), by_no, by_name)
        if client is None and is_ecom and row.get("division_name"):
            client = _client_for_row(row.get("division_name"), by_no, by_name)
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
        edm_key = _dup_key(row.get("edm"), row.get("run"), is_ecom)
        if (is_edm or is_ecom) and _norm_edm(row.get("edm")):
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
                    log_exception("duplicate_completed", row, client, ("Run + Batch already Completed" if is_ecom else "EDM already Completed"), row_tl, existing_order=latest)
                else:
                    counts["skipped_duplicate_open"] += 1     # In-Process / Clarification / blank: already in the system
                continue

        db.add(_order_from_inventory_row(process_id, row_tl, today, row, current_user.username))
        counts["imported"] += 1

    # One exception per document: skip any EDM already logged for this process recently.
    # (ECOM: a document is Run + Batch, so the same Batch under another Run is kept.)
    def _exc_key(e):
        run = None
        if is_ecom and e.row_json:
            try:
                run = json.loads(e.row_json).get("run")
            except ValueError:
                pass
        return (e.kind, _dup_key(e.edm, run, is_ecom))

    if exceptions:
        recent = set()
        for e in db.query(models.ImportException).filter(
            models.ImportException.process_id == process_id,
            models.ImportException.created_at >= now_ist - timedelta(days=EXCEPTION_DEDUPE_DAYS),
            models.ImportException.edm.isnot(None),
        ).all():
            recent.add(_exc_key(e))
        logged = set()
        for e in exceptions:
            key = _exc_key(e)
            if e.edm and (key in recent or key in logged):
                continue
            logged.add(key)
            db.add(e)
    _pn = db.query(models.Process.name).filter(models.Process.id == process_id).scalar()
    _audit(db, current_user, "Order", "Imported inventory file", None, None,
           f"{_pn}: file {(file.filename or '')[:60]} - {counts['imported']} imported of {counts['total_rows']} rows ({counts['skipped_duplicate_open'] + counts['skipped_duplicate_completed'] + counts['skipped_duplicate_in_file']} duplicates skipped)")
    db.commit()
    _auto_assign_open_slots(db, process_id)
    return counts


@app.post("/orders/import-check")
def import_check(
    process_id: int,
    file: UploadFile = File(...),
    on_behalf_of_team_lead_id: Optional[int] = Form(None),
    current_user: models.User = Depends(auth.require_role("team_lead", "super_admin", "admin", "manager")),
    db: Session = Depends(get_db),
):
    """
    Dry run of /orders/import: nothing is saved. Reports missing columns, odd dates / amounts,
    how many rows have no assigned client, and what the real import would skip (with samples).
    """
    _require_process_access(current_user, process_id)
    fixed_tl = None
    if current_user.role == "team_lead":
        fixed_tl = current_user.id
    elif current_user.role in ("admin", "manager") and on_behalf_of_team_lead_id is not None:
        fixed_tl = on_behalf_of_team_lead_id
    process = db.query(models.Process).filter(models.Process.id == process_id).first()
    is_edm = bool(process and process.name.strip().upper() == "EDM")
    is_cb = bool(process and process.name.strip().lower() == "cb unmatched posting")
    is_ecom = _is_ecom_process(process)

    try:
        rows = _read_inventory_rows(file.file.read(), file.filename, ecom=is_ecom)
    except HTTPException as e:
        return {"can_import": False, "fatal": e.detail, "warnings": [], "counts": {}, "samples": {}, "missing_columns": [], "total_rows": 0}

    fields = set(rows.fields)
    label = {"edm": "Batch / EDM", "created": "Created", "def_doc_type": "Def Doc Type", "amount": "Amount",
             "description": "Description", "run": "Run", "not_posted_amount": "NotPosted$", "offset_amount": "Offset $",
             "division": "Division"}
    required = ["edm", "division"] if is_ecom else ["edm"]
    recommended = (["run", "not_posted_amount", "offset_amount"] if is_ecom
                   else ["created", "def_doc_type", "amount", "description"])
    miss_req = [label[f] for f in required if f not in fields and not (f == "division" and ("description" in fields or "division_name" in fields))]
    miss_rec = [label[f] for f in recommended if f not in fields]
    if is_edm and "def_doc_type" not in fields and "Def Doc Type" not in miss_rec:
        miss_rec.append("Def Doc Type")

    clients = db.query(models.Client).all()
    by_no = {_norm_facility(c.facility_no): c for c in clients}
    nc = {}
    for c in clients:
        nc[_norm_text(c.client_name)] = nc.get(_norm_text(c.client_name), 0) + 1
    by_name = {_norm_text(c.client_name): c for c in clients if nc[_norm_text(c.client_name)] == 1}
    client_tl = {a.client_id: a.team_lead_id for a in db.query(models.ClientProcessTeamLead).filter(
        models.ClientProcessTeamLead.process_id == process_id).all()}
    today = _work_today()
    existing = {}
    if is_edm or is_ecom:
        cutoff = today - relativedelta(months=DUPLICATE_LOOKBACK_MONTHS)
        for o in db.query(models.WorkOrder.edm, models.WorkOrder.posting_status, models.WorkOrder.run).filter(
            models.WorkOrder.process_id == process_id, models.WorkOrder.received_date >= cutoff,
            models.WorkOrder.edm.isnot(None)).all():
            existing.setdefault(_dup_key(o.edm, o.run, is_ecom), []).append(o)

    counts = {"total_rows": 0, "will_import": 0, "skipped_bar_grp": 0, "skipped_doc_type": 0,
              "skipped_other_team_lead": 0, "skipped_unassigned_client": 0, "skipped_duplicate_open": 0,
              "skipped_duplicate_in_file": 0, "skipped_duplicate_completed": 0,
              "blank_edm": 0, "bad_dates": 0, "bad_amounts": 0, "negative_amounts": 0, "zero_amounts": 0,
              "future_dates": 0, "old_dates": 0, "unknown_clients": 0, "no_team_lead_clients": 0}
    samples = {}
    unassigned_vals = {}
    amounts = []

    def sample(kind, n, row, why):
        lst = samples.setdefault(kind, [])
        if len(lst) < 8:
            lst.append({"row": n, "edm": row.get("edm") or "", "division": row.get("division") or "", "why": why})

    seen = set()
    for n, row in enumerate(rows, start=1):
        counts["total_rows"] += 1
        if not _norm_edm(row.get("edm")):
            counts["blank_edm"] += 1
            sample("blank_edm", n, row, "Batch / EDM is empty")
        # odd dates
        for col, nm in (("created", "Created"), ("deposit_date", "Deposit Date")):
            v = (row.get(col) or "").strip()
            if not v:
                continue
            try:
                d = (_parse_dt(v) if col == "created" else _parse_date(v))
                dd = d.date() if isinstance(d, datetime) else d
                if dd > today + timedelta(days=1):
                    counts["future_dates"] += 1; sample("future_dates", n, row, f"{nm} {v} is in the future")
                elif dd < today - timedelta(days=400):
                    counts["old_dates"] += 1; sample("old_dates", n, row, f"{nm} {v} is over a year old")
            except (ValueError, OverflowError, TypeError):
                counts["bad_dates"] += 1; sample("bad_dates", n, row, f"{nm} '{v}' is not a date")
        # odd amounts
        for col, nm in (("amount", "Amount"), ("not_posted_amount", "NotPosted$"), ("offset_amount", "Offset $")):
            v = (row.get(col) or "").strip()
            if not v:
                continue
            try:
                a = float(_clean_number(v))
                if col == "amount":
                    amounts.append((n, a, row))
                    if a < 0:
                        counts["negative_amounts"] += 1; sample("negative_amounts", n, row, f"{nm} {v} is negative")
                    elif a == 0:
                        counts["zero_amounts"] += 1; sample("zero_amounts", n, row, f"{nm} is zero")
            except (ValueError, TypeError):
                counts["bad_amounts"] += 1; sample("bad_amounts", n, row, f"{nm} '{v}' is not a number")

        # same skip rules as the real import
        if _norm_text(row.get("bar_grp")) in SKIP_BAR_GRPS:
            counts["skipped_bar_grp"] += 1; sample("skipped_bar_grp", n, row, "Bar Grp is excluded"); continue
        if is_edm and _norm_text(row.get("def_doc_type")) not in EDM_ALLOWED_DOC_TYPES:
            counts["skipped_doc_type"] += 1; sample("skipped_doc_type", n, row, f"Def Doc Type '{row.get('def_doc_type') or ''}' not imported"); continue
        if is_cb and _norm_text(row.get("def_doc_type")) not in CB_UNMATCHED_ALLOWED_DOC_TYPES:
            counts["skipped_doc_type"] += 1; sample("skipped_doc_type", n, row, f"Def Doc Type '{row.get('def_doc_type') or ''}' not imported"); continue
        client = _client_for_row(row.get("division"), by_no, by_name)
        if client is None and is_ecom and row.get("division_name"):
            client = _client_for_row(row.get("division_name"), by_no, by_name)
        row_tl = client_tl.get(client.id) if client else None
        if row_tl is None:
            counts["skipped_unassigned_client"] += 1
            if client is None:
                counts["unknown_clients"] += 1
            else:
                counts["no_team_lead_clients"] += 1
            key = (row.get("division") or row.get("division_name") or "(blank)").strip() or "(blank)"
            unassigned_vals[key] = unassigned_vals.get(key, 0) + 1
            sample("skipped_unassigned_client", n, row, "Client not in client list" if client is None else f"{client.client_name}: no Team Lead for this process")
            continue
        if fixed_tl is not None and row_tl != fixed_tl:
            counts["skipped_other_team_lead"] += 1; continue
        k = _dup_key(row.get("edm"), row.get("run"), is_ecom)
        if (is_edm or is_ecom) and _norm_edm(row.get("edm")):
            if k in seen:
                counts["skipped_duplicate_in_file"] += 1; sample("skipped_duplicate_in_file", n, row, "Repeated in this file"); continue
            seen.add(k)
            prior = existing.get(k)
            if prior:
                if any(o.posting_status == "Completed" for o in prior):
                    counts["skipped_duplicate_completed"] += 1; sample("skipped_duplicate_completed", n, row, "Already Completed")
                else:
                    counts["skipped_duplicate_open"] += 1; sample("skipped_duplicate_open", n, row, "Already in the system")
                continue
        counts["will_import"] += 1

    # amount outliers: > 20x the median of positive amounts
    pos = sorted(a for _, a, _ in amounts if a > 0)
    outliers = 0
    if len(pos) >= 10:
        med = pos[len(pos) // 2]
        for n, a, row in amounts:
            if med > 0 and a > med * 20:
                outliers += 1
                sample("amount_outliers", n, row, f"Amount {a:,.2f} is over 20x the typical {med:,.2f}")
    counts["amount_outliers"] = outliers

    total = max(counts["total_rows"], 1)
    warnings = []
    def warn(level, text):
        warnings.append({"level": level, "text": text})
    if miss_req:
        warn("error", "Missing required column(s): " + ", ".join(miss_req) + ". Check the header row of the file.")
    if miss_rec:
        warn("warn", "Column(s) not found: " + ", ".join(miss_rec) + ". Those fields will be blank on the imported orders.")
    if counts["total_rows"] == 0:
        warn("error", "The file has a header but no data rows.")
    if counts["blank_edm"]:
        warn("warn", f"{counts['blank_edm']} row(s) have no Batch / EDM.")
    if counts["bad_dates"]:
        warn("warn", f"{counts['bad_dates']} date value(s) cannot be read and will be left blank.")
    if counts["future_dates"]:
        warn("warn", f"{counts['future_dates']} date(s) are in the future.")
    if counts["old_dates"]:
        warn("info", f"{counts['old_dates']} date(s) are more than a year old.")
    if counts["bad_amounts"]:
        warn("warn", f"{counts['bad_amounts']} amount(s) are not numbers and will be left blank.")
    if counts["negative_amounts"]:
        warn("info", f"{counts['negative_amounts']} negative amount(s).")
    if counts["zero_amounts"]:
        warn("info", f"{counts['zero_amounts']} zero amount(s).")
    if outliers:
        warn("warn", f"{outliers} amount(s) look unusually large compared with the rest of the file.")
    un = counts["skipped_unassigned_client"]
    if un:
        pct = round(un * 100 / total)
        warn("error" if pct >= 20 else "warn", f"{un} row(s) ({pct}%) belong to a client with no Team Lead for this process and will be skipped"
             + (f" - e.g. {', '.join(list(unassigned_vals)[:5])}" if unassigned_vals else "") + ".")
    if counts["will_import"] == 0 and counts["total_rows"] > 0:
        warn("error", "Nothing in this file would be imported.")
    top_un = sorted(unassigned_vals.items(), key=lambda kv: -kv[1])[:10]
    fatal = bool(miss_req) or counts["total_rows"] == 0
    return {"can_import": not fatal, "fatal": None, "warnings": warnings, "counts": counts, "samples": samples,
            "missing_columns": miss_req + miss_rec, "unassigned_clients": [{"value": k, "rows": v} for k, v in top_un],
            "total_rows": counts["total_rows"], "will_import": counts["will_import"]}


# Import Exceptions list: Team Leads see their own "already completed" notices;
# Admin and Super Admin see everything.
@app.get("/import-exceptions", response_model=List[schemas.ImportExceptionOut])
def list_import_exceptions(
    kind: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    process_id: Optional[int] = None,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
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
        item.details = _exception_row(r)
        out.append(item)
    return out


def _exception_row(e: models.ImportException) -> dict:
    """The inventory row behind an exception (older entries only kept a few fields)."""
    if e.row_json:
        try:
            return json.loads(e.row_json)
        except ValueError:
            pass
    return {
        "edm": e.edm, "division": e.division_raw, "def_doc_type": e.def_doc_type,
        "amount": "" if e.amount is None else str(e.amount),
    }


@app.post("/import-exceptions/delete")
def delete_import_exceptions(
    payload: schemas.ImportExceptionIds,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    """Removes the selected exception entries. A Team Lead may only remove their own 'already completed' notices."""
    q = db.query(models.ImportException).filter(models.ImportException.id.in_(payload.ids or [0]))
    if current_user.role == "team_lead":
        q = q.filter(models.ImportException.kind == "duplicate_completed", models.ImportException.team_lead_id == current_user.id)
    n = 0
    for e in q.all():
        db.delete(e)
        n += 1
    db.commit()
    return {"deleted": n}


@app.post("/import-exceptions/release")
def release_import_exceptions(
    payload: schemas.ImportExceptionRelease,
    current_user: models.User = Depends(auth.require_role("super_admin")),
    db: Session = Depends(get_db),
):
    """
    Unassigned Clients: assigns each selected row's client to the chosen Team Lead
    (for that row's process, unless the client already has one there) and moves the
    row into that Team Lead's Active Queue. Other unassigned rows of the same client
    in the same process move too. Rows whose client isn't in the client list can't
    be assigned and stay listed.
    """
    tl = _eligible_team_lead(db, payload.team_lead_id)
    selected = db.query(models.ImportException).filter(
        models.ImportException.id.in_(payload.ids or [0]), models.ImportException.kind == "unassigned_client",
    ).all()
    if not selected:
        raise HTTPException(status_code=400, detail="Select at least one Unassigned Clients row")

    clients = db.query(models.Client).all()
    by_no = {_norm_facility(c.facility_no): c for c in clients}
    name_counts = {}
    for c in clients:
        name_counts[_norm_text(c.client_name)] = name_counts.get(_norm_text(c.client_name), 0) + 1
    by_name = {_norm_text(c.client_name): c for c in clients if name_counts[_norm_text(c.client_name)] == 1}

    def client_of(e):
        if e.facility_no and _norm_facility(e.facility_no) in by_no:
            return by_no[_norm_facility(e.facility_no)]
        row = _exception_row(e)
        c = _client_for_row(e.division_raw or row.get("division"), by_no, by_name)
        if c is None and row.get("division_name"):
            c = _client_for_row(row.get("division_name"), by_no, by_name)
        return c

    result = {"clients_assigned": 0, "moved": 0, "already_in_system": 0, "no_client": 0, "other_process": []}
    tl_procs = {p.id for p in tl.processes}
    assignment = {}          # (client_id, process_id) -> team_lead_id in force
    for e in selected:
        c = client_of(e)
        if not c or e.process_id is None:
            result["no_client"] += 1
            continue
        key = (c.id, e.process_id)
        if key in assignment:
            continue
        cur = db.query(models.ClientProcessTeamLead).filter_by(client_id=c.id, process_id=e.process_id).first()
        if cur:
            assignment[key] = cur.team_lead_id
        elif e.process_id not in tl_procs:
            result["other_process"].append(f"{c.client_name} ({(db.query(models.Process).get(e.process_id) or models.Process(name='?')).name})")
        else:
            db.add(models.ClientProcessTeamLead(client_id=c.id, process_id=e.process_id, team_lead_id=tl.id))
            assignment[key] = tl.id
            result["clients_assigned"] += 1
    db.flush()

    # every unassigned exception of an assigned client + process moves, not just the ticked ones
    today = _work_today()
    cutoff = today - relativedelta(months=DUPLICATE_LOOKBACK_MONTHS)
    process_ids = {k[1] for k in assignment}
    pool = db.query(models.ImportException).filter(
        models.ImportException.kind == "unassigned_client", models.ImportException.process_id.in_(process_ids or [0]),
    ).all()
    seen = set()
    for e in pool:
        c = client_of(e)
        if not c or (c.id, e.process_id) not in assignment:
            continue
        row = _exception_row(e)
        process = db.query(models.Process).filter(models.Process.id == e.process_id).first()
        is_edm = bool(process and process.name.strip().upper() == "EDM")
        is_ecom = _is_ecom_process(process)
        edm_key = _dup_key(row.get("edm"), row.get("run"), is_ecom)
        dup = False
        if _norm_edm(row.get("edm")) and (e.process_id, edm_key) in seen:
            dup = True
        elif _norm_edm(row.get("edm")) and (is_edm or is_ecom):
            q = db.query(models.WorkOrder.id).filter(
                models.WorkOrder.process_id == e.process_id, models.WorkOrder.received_date >= cutoff,
                func.upper(models.WorkOrder.edm) == _norm_edm(row.get("edm")),
            )
            if is_ecom:
                q = q.filter(func.upper(func.coalesce(models.WorkOrder.run, "")) == _norm_edm(row.get("run")))
            dup = q.first() is not None
        seen.add((e.process_id, edm_key))
        if dup:
            result["already_in_system"] += 1
        else:
            db.add(_order_from_inventory_row(e.process_id, assignment[(c.id, e.process_id)], today, row, current_user.username))
            result["moved"] += 1
        db.delete(e)
    db.commit()
    for pid in process_ids:
        _auto_assign_open_slots(db, pid)
    return result


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

def _clean_number(v) -> str:
    """'$1,234.50' -> '1234.50'; '(25.00)' / '25.00-' -> negative."""
    t = str(v).strip().replace("$", "").replace(",", "").replace(" ", "")
    if t.startswith("(") and t.endswith(")"):
        t = "-" + t[1:-1]
    if t.endswith("-") and len(t) > 1:
        t = "-" + t[:-1]
    return t


def _parse_int(v: Optional[str]) -> Optional[int]:
    if v in (None, ""):
        return None
    n = int(float(_clean_number(v)))
    if abs(n) > 2_147_483_647:          # would overflow an INTEGER column (e.g. a check/ID number in a count column)
        raise ValueError("number too large for a count")
    return n


def _parse_float(v: Optional[str]) -> Optional[float]:
    return float(_clean_number(v)) if v not in (None, "") else None


# ---------------------------------------------------------------------------
# Assignment logic — one open file per colleague at a time (round-robin)
# ---------------------------------------------------------------------------

# ---- EDM division hold: some divisions are not handed to colleagues until N business days after their Created date ----
HOLD_KEYS = {"divs": "edm_hold_divisions", "days": "edm_hold_days"}


def _hold_default_days(db: Session) -> int:
    try:
        return max(1, min(30, int(_setting(db, HOLD_KEYS["days"], "5"))))
    except ValueError:
        return 5


def _hold_items(db: Session):
    """Saved lines: 'division' (uses the default days) or 'division|days'. Returns [(value, days)]."""
    default = _hold_default_days(db)
    out = []
    for line in _setting(db, HOLD_KEYS["divs"], "").split("\n"):
        if not line.strip():
            continue
        v, _, d = line.rpartition("|") if "|" in line else (line, "", "")
        try:
            days = max(1, min(30, int(d))) if d.strip() else default
        except ValueError:
            v, days = line, default
        if v.strip():
            out.append((v.strip(), days))
    return out


def _hold_cfg(db: Session):
    """(map of normalised division -> hold days, default days). Both '0120' and '120' style keys are matched."""
    m = {}
    for v, d in _hold_items(db):
        m[_norm_text(v)] = d
        m[_norm_facility(_norm_text(v))] = d
    return m, _hold_default_days(db)


def _hold_release_date(created, days: int):
    """The first business day on which an order created on `created` may be assigned: business day number days+1 after it."""
    d = created.date() if isinstance(created, datetime) else created
    if not d:
        return None
    hol = _holiday_dates()
    n = 0
    while n < days + 1:
        d += timedelta(days=1)
        if d.weekday() < 5 and d not in hol:
            n += 1
    return d


def _order_hold_until(order, cfg, today=None):
    """Release date if this EDM order is still on hold today, else None."""
    divs, _default = cfg
    if not divs or order.created is None or ((order.process.name if order.process else "") or "").strip().upper() != "EDM":
        return None
    days = None
    for v in (order.division, order.division_name):
        n = _norm_text(v)
        if n and (n in divs or _norm_facility(n) in divs):
            days = divs.get(n, divs.get(_norm_facility(n)))
            break
    if days is None:
        return None
    rel = _hold_release_date(order.created, days)
    return rel if rel and (today or _work_today()) < rel else None


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
            models.User.last_login_date == _work_today(),
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
        # Oldest Deposit Date first (blank ones last), then the order they came in.
        .order_by(models.WorkOrder.deposit_date.is_(None), models.WorkOrder.deposit_date.asc(), models.WorkOrder.id.asc())
        .all()
    )
    if not candidates:
        return

    hold = _hold_cfg(db)
    if hold[0]:
        today_w = _work_today()
        candidates = [c for c in candidates if _order_hold_until(c, hold, today_w) is None]
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
        next_order.assigned_date = _work_today()
        next_order.employee_id = colleague.employee_id or colleague.username
        next_order.employee_name = colleague.full_name
        next_order.posting_status = "In-Process"
        _start_timer(next_order)
        db.add(next_order)

    db.commit()


# ---------------------------------------------------------------------------
# Read endpoints
# ---------------------------------------------------------------------------

def _attach_team_files(db: Session, orders) -> None:
    """Tags each order with `team_files` (id, file name, who sent it) from its Onshore / Recon / Calling / Team Lead hand-off, so rows can show the links."""
    ids = [o.id for o in orders]
    by_order = {}
    if ids:
        rows = (
            db.query(models.OnshoreAttachment, models.OnshoreMessage)
            .join(models.OnshoreMessage, models.OnshoreMessage.id == models.OnshoreAttachment.message_id)
            .filter(models.OnshoreAttachment.order_id.in_(ids))
            .order_by(models.OnshoreAttachment.id.asc())
            .all()
        )
        for a, m in rows:
            by_order.setdefault(a.order_id, []).append(
                {"id": a.id, "file_name": a.file_name, "from_role": m.author_role, "from_name": m.author_name}
            )
    for o in orders:
        o.team_files = by_order.get(o.id, [])


@app.get("/orders/day-summary")
def orders_day_summary(
    process_id: int,
    current_user: models.User = Depends(auth.get_current_user),
    db: Session = Depends(get_db),
):
    """Orders submitted to Production today (IST) - count and Trans Count - for the Active Queue summary strip."""
    _deny_quality(current_user)
    _require_view_access(current_user, process_id)
    start_ist = datetime.now(IST).replace(hour=0, minute=0, second=0, microsecond=0)
    start_utc = start_ist.astimezone(timezone.utc).replace(tzinfo=None)
    q = db.query(models.WorkOrder).filter(
        models.WorkOrder.process_id == process_id,
        models.WorkOrder.submitted == True,  # noqa: E712
        models.WorkOrder.submitted_at >= start_utc,
    )
    if current_user.role == "colleague":
        q = q.filter(models.WorkOrder.assigned_to_id == current_user.id)
    elif current_user.role == "team_lead":
        q = q.filter(models.WorkOrder.team_lead_id == current_user.id)
    rows = q.all()
    return {"count": len(rows), "trans_count": sum(int(r.trans_count or 0) for r in rows)}


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
    if current_user.role in ("admin", "manager", "super_admin"):
        _attach_team_leads(db, orders, prefer="owner")     # so the screen can group by Team Lead
    _attach_team_files(db, orders)
    if current_user.role != "colleague":
        hold = _hold_cfg(db)
        if hold[0]:
            today_w = _work_today()
            for o in orders:
                o.held_until = _order_hold_until(o, hold, today_w) if o.assigned_to_id is None else None
    return orders


@app.get("/orders/edm-hold")
def edm_hold_get(current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")), db: Session = Depends(get_db)):
    days = _hold_default_days(db)
    p = db.query(models.Process).filter(func.upper(models.Process.name) == "EDM").first()
    known = []
    if p:
        rows = (db.query(models.WorkOrder.division, models.WorkOrder.division_name, func.count(models.WorkOrder.id))
                .filter(models.WorkOrder.process_id == p.id).group_by(models.WorkOrder.division, models.WorkOrder.division_name).all())
        known = sorted(({"division": d or "", "division_name": n or "", "orders": c} for d, n, c in rows if (d or n)), key=lambda x: (x["division_name"] or x["division"]).lower())
    items = [{"value": v, "days": d} for v, d in _hold_items(db)]
    return {"days": days, "selected": [i["value"] for i in items], "items": items, "known": known}


def _hold_save_items(db: Session, items: list, default_days: int):
    clean = {}
    for v, d in items:
        v = str(v).replace("|", " ").strip()
        if v:
            clean[v] = d
    _set_setting(db, HOLD_KEYS["divs"], "\n".join(f"{v}|{d}" for v, d in clean.items()))
    _set_setting(db, HOLD_KEYS["days"], str(default_days))
    return clean


def _hold_days_value(x, what="Days"):
    try:
        d = int(x)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail=f"{what} must be a number")
    if not 1 <= d <= 30:
        raise HTTPException(status_code=400, detail=f"{what} must be between 1 and 30")
    return d


@app.put("/orders/edm-hold")
def edm_hold_put(payload: dict, current_user: models.User = Depends(auth.require_role("admin", "manager", "super_admin")), db: Session = Depends(get_db)):
    default = _hold_days_value(payload.get("days", 5))
    if payload.get("items") is not None:
        items = [(str(i.get("value", "")), _hold_days_value(i.get("days", default), "Days")) for i in payload["items"] if isinstance(i, dict)]
    else:
        items = [(str(x), default) for x in (payload.get("divisions") or [])]
    clean = _hold_save_items(db, items, default)
    _audit(db, current_user, "Orders", "EDM division hold updated", None, None, f"{len(clean)} division(s): " + ", ".join(f"{v}={d}d" for v, d in clean.items())[:400])
    db.commit()
    return {"ok": True}


@app.put("/orders/edm-hold/division")
def edm_hold_division(payload: dict, current_user: models.User = Depends(auth.require_role("admin", "manager", "super_admin")), db: Session = Depends(get_db)):
    """Set (days = 1..30) or remove (days = null / 0) the hold for one division, e.g. from the Clients table."""
    value = str(payload.get("value", "")).replace("|", " ").strip()
    if not value:
        raise HTTPException(status_code=400, detail="Division is required")
    items = [(v, d) for v, d in _hold_items(db) if _norm_facility(_norm_text(v)) != _norm_facility(_norm_text(value))]
    raw = payload.get("days")
    if raw not in (None, "", 0, "0"):
        items.append((value, _hold_days_value(raw)))
    _hold_save_items(db, items, _hold_default_days(db))
    _audit(db, current_user, "Orders", "EDM division hold updated", None, None, f"{value}: " + (f"{raw} business days" if raw not in (None, "", 0, "0") else "hold removed"))
    db.commit()
    return {"ok": True}


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
    if current_user.role in ("admin", "manager", "quality", "super_admin"):
        _attach_team_leads(db, orders, prefer="colleague")
    return orders


@app.get("/orders/escalations", response_model=List[schemas.WorkOrderOut])
def list_escalations(
    process_id: int,
    resolved: bool = False,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
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
        had_queries = db.query(models.EscalationEvent.order_id).filter(
            models.EscalationEvent.process_id == process_id, models.EscalationEvent.closed_date.isnot(None))
        query = query.filter(or_(
            and_(
                models.WorkOrder.escalation_category.in_(("Clarification", *ESCALATION_CATEGORY_FIELDS.keys())),
                models.WorkOrder.escalated == False,  # noqa: E712
                models.WorkOrder.issue_closed_date.isnot(None),
            ),
            models.WorkOrder.id.in_(had_queries),
        ))
    else:
        query = query.filter(
            models.WorkOrder.escalated == True,  # noqa: E712
            models.WorkOrder.submitted == False,  # noqa: E712
        )
    if current_user.role == "team_lead":
        query = query.filter(models.WorkOrder.team_lead_id == current_user.id)
    order_col = models.WorkOrder.issue_raised_date
    orders = query.order_by(order_col.desc() if resolved else order_col.asc()).all()
    if current_user.role in ("admin", "manager", "super_admin"):
        _attach_team_leads(db, orders, prefer="owner")
    _attach_team_files(db, orders)
    if resolved:
        return _resolved_query_rows(db, orders)
    # one row per open reason: an order with two reasons shows twice, each with its own team and status
    dets = _details_by_order(db, [o.id for o in orders])
    items = []
    for o in orders:
        base = schemas.WorkOrderOut.model_validate(o)
        det = dets.get(o.id)
        dd = (det.data or {}) if det else {}
        if not dd.get("_pdone"):
            items.append(base)
        for ex in dd.get("_queue") or []:
            items.append(_extra_view(base, o, ex, det))
    return items


def _resolved_query_rows(db: Session, orders) -> list:
    """The resolved report lists every query on an order as its own row (own dates, category, comments and details)."""
    events = {}
    for e in db.query(models.EscalationEvent).filter(
        models.EscalationEvent.order_id.in_([o.id for o in orders] or [0]), models.EscalationEvent.closed_date.isnot(None),
    ).order_by(models.EscalationEvent.closed_date.desc(), models.EscalationEvent.id.desc()).all():
        events.setdefault(e.order_id, []).append(e)
    clar_fields = set(schemas.ClarificationDetailOut.model_fields) - {"order_id", "updated_at"}
    rows = []
    for o in orders:
        base = schemas.WorkOrderOut.model_validate(o)
        evs = events.get(o.id)
        if not evs:
            rows.append(base)
            continue
        for e in evs:
            item = base.model_copy(deep=True)
            item.issue_raised_date, item.issue_closed_date = e.raised_date, e.closed_date
            item.escalation_category = e.category
            item.ventra_comment, item.poster_comment = e.ventra_comment, e.poster_comment
            try:
                snap = json.loads(e.detail_json or "{}")
            except ValueError:
                snap = {}
            if snap:
                if e.category == "Clarification":
                    item.clarification_detail = schemas.ClarificationDetailOut(order_id=o.id, **{k: v for k, v in snap.items() if k in clar_fields})
                else:
                    item.escalation_detail = schemas.EscalationDetailOut(order_id=o.id, category=e.category or "", data=snap)
            rows.append(item)
    rows.sort(key=lambda r: (r.issue_closed_date or date.min, r.id), reverse=True)
    return rows


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
    before = _order_snapshot(order)
    for field, value in payload.dict(exclude_unset=True).items():
        setattr(order, field, value)
    order.last_edited_by = current_user.username
    if reasons:
        _log_order_change(db, current_user, order, "edit", reasons, before)
    _d = _order_diff_text(before, order)
    if _d:
        _audit_order(db, current_user, "Edited order", order, _d)
    db.commit()
    db.refresh(order)
    return order


EDM_ALREADY_POSTED_NOTE = "Already posted under BAR BATCH"


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

    # Time Taken has to be running: Start must be clicked before anything is saved
    # (and Restart after a pause / resolved clarification).
    if order.timer_status == "not_started":
        raise HTTPException(status_code=400, detail="Click Start on this order before saving")
    if order.timer_status == "paused":
        raise HTTPException(status_code=400, detail="The timer is paused \u2014 click Restart on this order before saving")

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
    payload_data.pop("issue_raised_date", None)     # auto-stamped when Clarification is first saved

    # Final posting_status after this update is applied (may be unchanged).
    final_status = payload_data.get("posting_status", order.posting_status)

    # Posted $, BAR Batch, and Trans Count are the core figures — nothing
    # else can be saved until all three are filled in (existing value or
    # part of this update).
    final_posted_amount = payload_data.get("posted_amount", order.posted_amount)
    ecom_order = _is_ecom_process(order.process)
    final_bar_batch = payload_data.get("bar_batch", order.bar_batch)
    final_trans_count = payload_data.get("trans_count", order.trans_count)
    missing_core = []
    if final_posted_amount is None:
        missing_core.append("Posted $")
    if not final_bar_batch and not ecom_order:
        missing_core.append("BAR Batch")
    if final_trans_count is None:
        missing_core.append("# of Transaction" if ecom_order else "Trans Count")
    if ecom_order and not (payload_data.get("posting_status") or order.posting_status):
        missing_core.append("Posting Status")
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
    bar_text = str(final_bar_batch or "").strip()
    problems = []
    # EDM: Amount > 0, Manual Posted $ = 0, a 7-digit BAR Batch, Trans Count > 0 and Completed means the money was already
    # posted under that BAR Batch. It saves, with Poster Comment defaulting to EDM_ALREADY_POSTED_NOTE.
    edm_already = bool(
        ((order.process.name if order.process else "") or "").strip().upper() == "EDM"
        and order.amount is not None and float(order.amount) > 0 and abs(float(final_posted_amount)) < 0.005
        and bar_text.isdigit() and len(bar_text) == 7 and int(bar_text) > 0
        and final_trans_count is not None and final_status == "Completed")      # Trans Count may be 0 here
    if edm_already and not str(payload_data.get("poster_comment", order.poster_comment) or "").strip():
        payload_data["poster_comment"] = EDM_ALREADY_POSTED_NOTE
    if not ecom_order:
        if abs(float(final_posted_amount)) < 0.005:
            # CB Unmatched Posting: with Posted $ 0 the BAR Batch may also be a real 7-digit batch number
            unmatched_ok = "UNMATCHED" in str(getattr(order.process, "name", "") or "").upper() and bar_text.isdigit() and len(bar_text) == 7
            if bar_text not in ("0", "00", "0000000") and not unmatched_ok and not edm_already:
                problems.append("BAR Batch must be 0 when Posted $ is 0")
        else:
            if not (bar_text.isdigit() and len(bar_text) == 7 and int(bar_text) > 0):
                problems.append("BAR Batch must be a 7-digit number when Posted $ is not 0")
        bar_positive = bar_text.isdigit() and int(bar_text) > 0
        if bar_positive and not (final_trans_count and final_trans_count > 0) and not edm_already and "UNMATCHED" not in str(getattr(order.process, "name", "") or "").upper():   # CB Unmatched Posting / EDM already posted: Trans Count 0 is fine
            problems.append("Trans Count must be greater than 0 when BAR Batch is greater than 0")
    final_835 = payload_data.get("posted_835_amount", order.posted_835_amount)
    if final_835 is not None and float(final_835) < 0:
        problems.append("835 Posted $ can't be negative")
    if final_835 and float(final_835) > 0.005:
        lines_total = _p835_lines_total(db, order.id)
        if lines_total <= 0:
            problems.append("835 Posted $ is above 0 \u2014 fill the 835 details form first (click Details next to 835 Posted $)")
        elif abs(lines_total - float(final_835)) >= 0.005:
            problems.append(f"The 835 details lines add up to {lines_total:.2f} but 835 Posted $ is {float(final_835):.2f} \u2014 open Details and fix the line amounts")
    pending_base = _pending_base(order)
    if pending_base is not None and final_status in ("Completed", "Clarification"):
        pending_now = pending_base - float(final_posted_amount) - float(final_835 or 0)
        final_comment = payload_data.get("poster_comment", order.poster_comment)
        comment_optional = "UNMATCHED" in str(getattr(order.process, "name", "") or "").upper()   # CB Unmatched Posting: comment not mandatory
        if abs(pending_now) >= 0.005 and not (final_comment or "").strip() and not comment_optional and not edm_already:
            problems.append("Poster Comment is required when Pending $ is not 0")
        if abs(pending_now) >= 0.005 and final_status == "Completed" and not edm_already and ((order.process.name if order.process else "") or "").strip().upper() == "EDM":
            problems.append("Posting Status can't be Completed while Pending $ is not 0")
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
        order.issue_raised_date = _work_today()

    # Auto-set Posted Date the moment a colleague marks Completed — no manual
    # entry needed, and it guarantees TAT can always be calculated below.
    if order.posting_status == "Completed" and not order.posted_date:
        order.posted_date = _work_today()

    # Completing an order freezes its "Time Taken" — stop the clock and
    # roll in whatever time was still running.
    if order.posting_status == "Completed" and order.timer_status != "stopped":
        _finalize_timer(order, "stopped")      # time spent paused is never added

    # Resolving a Clarification (moving to any other status) auto-stamps
    # Issue Closed Date, mirroring how Issue Raised Date auto-stamps on the
    # way in. Escalation Category and Issue Raised Date are deliberately
    # NOT cleared anymore — they're kept as history so the TAT pause-window
    # calculation at Completion stays accurate.
    if previous_status == "Clarification" and order.posting_status != "Clarification" and not order.issue_closed_date:
        order.issue_closed_date = _work_today()

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
        if not order.escalated:
            _open_escalation_event(db, order, current_user, {
                k: getattr(detail, k) for k in (
                    "deposit_type", "exchange", "era_check", "edm_batch_number", "bar_batch_number", "batch_description",
                    "escalation_type", "clarification_details", "team", "poster_login", "amount_posted")})
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
        q_now = _refresh_queue((detail.data or {}).get("_queue"), order, current_user)
        if q_now:
            merged["_queue"] = q_now
        detail.data = merged
        detail.posted_by_name = current_user.full_name
        detail.updated_at = datetime.now(IST)
        if not order.escalated:
            _open_escalation_event(db, order, current_user, dict(merged))
        order.escalated = True
        if order.timer_status == "running":
            _finalize_timer(order, "paused")

    # Pending $ = Amount - Posted $ (ECOM: (NotPosted$ + Offset $) - Posted $), recalculated any time either changes.
    _recalc_pending(order)
    if not (order.posted_835_amount and float(order.posted_835_amount) > 0.005):
        db.query(models.Posting835Line).filter_by(order_id=order.id).delete()     # no 835 posting any more -> no details lines

    # Completing an order requires the core figures to already be filled in
    # (redundant with the check above, kept as a final guard), plus a Posted
    # Date — otherwise TAT can never be calculated and the row locks with it
    # permanently missing.
    if order.posting_status == "Completed":
        missing = []
        if order.posted_amount is None:
            missing.append("Posted $")
        if not order.bar_batch and not _is_ecom_process(order.process):
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

    # TAT = business days (Mon-Fri) between the Created date and Posted Date,
    # minus business days spent waiting on escalations.
    if order.posted_date and _tat_start(order):
        total_bdays = _count_business_days_ex_holidays(_tat_start(order), order.posted_date)
        pause_bdays = _pause_business_days(db, order)
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
    current_user: models.User = Depends(auth.require_role("admin", "manager", "super_admin")),
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
    if user.role not in ("admin", "manager", "quality"):
        _require_process_access(user, process_id)


def _deny_quality(user: models.User):
    """Quality can only view / export Production — nothing else."""
    if user.role == "quality":
        raise HTTPException(status_code=403, detail="The Quality profile can only view Production data")
    if user.role in TEAM_ROLES:
        raise HTTPException(status_code=403, detail="This profile can only work its own escalation queue")


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
    all_access = user.role in ("admin", "manager", "super_admin")
    if process_id is not None and not all_access:
        _require_process_access(user, process_id)
    cond = _team_orders_condition(db, user, include_unowned=True)   # None for admin / super_admin
    tl_cond = _tl_condition(db, team_lead_id) if (team_lead_id is not None and user.role in ("admin", "manager", "super_admin")) else None
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
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
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
    current_user: models.User = Depends(auth.require_role("admin", "manager", "super_admin")),
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
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
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
    hold_map, _hd = _hold_cfg(db)

    def _hd_for(c):
        return hold_map.get(_norm_text(c.facility_no), hold_map.get(_norm_facility(_norm_text(c.facility_no)), hold_map.get(_norm_text(c.client_name))))
    return [
        {
            "id": c.id, "facility_no": c.facility_no, "client_name": c.client_name,
            "status": c.status, "inactive_date": c.inactive_date, "edm_hold_days": _hd_for(c),
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
    current_user: models.User = Depends(auth.require_role("colleague", "team_lead", "admin", "manager", "super_admin")),
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
    _audit(db, current_user, "Client", "Added client", None, None, f"{client.facility_no} - {client.client_name} ({client.status})")
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
    _tl_label = db.query(models.User.full_name).filter(models.User.id == payload.team_lead_id).scalar() if payload.team_lead_id else "no Team Lead"
    _audit(db, current_user, "Client", "Bulk changed client Team Lead", None, None, f"{len(clients)} client(s), {len(target_ids)} process(es) -> {_tl_label}; e.g. " + ", ".join(c.facility_no for c in clients[:6]))
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
    _was = {"facility_no": client.facility_no, "client_name": client.client_name, "status": client.status, "inactive_date": client.inactive_date}
    for k, v in _clean_client_fields(db, payload, client_id).items():
        setattr(client, k, v)
    _ch = [f"{k}: {_was[k]} -> {getattr(client, k)}" for k in _was if _was[k] != getattr(client, k)]
    if _ch:
        _audit(db, current_user, "Client", "Edited client", None, None, f"{_was['facility_no']} - {_was['client_name']}: " + "; ".join(_ch))
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
    _tlnames = {u.id: u.full_name for u in db.query(models.User).filter(models.User.role == "team_lead").all()}
    _pnames = {p.id: p.name for p in db.query(models.Process).all()}
    _old = {r.process_id: r.team_lead_id for r in db.query(models.ClientProcessTeamLead).filter_by(client_id=client_id).all()}
    _chg = [f"{_pnames.get(pid, pid)}: {_tlnames.get(_old.get(pid), 'none')} -> {_tlnames.get(tl_id, 'none')}" for pid, tl_id in payload.assignments.items() if _old.get(pid) != tl_id]
    if _chg:
        _audit(db, current_user, "Client", "Changed client Team Lead", None, None, f"{client.facility_no} - {client.client_name}: " + "; ".join(_chg))
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
        "hours_auto": (lambda v: round(sum(v), 2) if v else None)([r["hours_auto"] for r in group if r.get("hours_auto") is not None]),
        "production_pct": prod,
        "accounts_audited": sum(aud) if aud else None,
        "errors": sum(err) if err else None,
        "quality_pct": qual,
    }


def _batch_base_rows(db: Session, current_user: models.User, start_date, end_date, process_id, user_id) -> list:
    """One row per colleague / process / date with completed work (before any Overall rows)."""
    if process_id is not None and current_user.role not in ("admin", "manager"):
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
        g = groups.setdefault(key, {"batches": 0, "trans": 0, "secs": 0})
        g["batches"] += 1
        g["trans"] += o.trans_count or 0
        g["secs"] += o.time_taken_seconds or 0
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
        # Total Hours Worked is what the colleague entered (as before). hours_auto is a separate,
        # automatic figure: the day's total Time Taken on the completed orders (shown to Team Lead / Admin / Super Admin).
        hours_auto = round(g["secs"] / 3600.0, 2) if g["secs"] else None
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
            "hours_auto": hours_auto if current_user.role != "colleague" else None,
            "pending_hours": st.pending_hours if st else None,
            "hours_status": st.hours_status if st else None,
            "comment": st.comment if st else None,
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
    current_user: models.User = Depends(auth.require_role("colleague", "team_lead", "admin", "manager", "super_admin")),
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
    current_user: models.User = Depends(auth.require_role("admin", "manager", "super_admin")),
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
    current_user: models.User = Depends(auth.require_role("colleague", "team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    Saves the manual inputs for one colleague / process / date.
    Colleague -> Total Hours Worked on their own rows only.
    Team Lead -> # of Accounts Audited and # of Errors for their team.
    Once saved, a row is locked: only Admin / Super Admin (any field, any
    colleague) can change it.
    """
    data = payload.dict(exclude_unset=True)
    colleague_fields = {"hours_worked", "comment"}
    lead_fields = {"accounts_audited", "errors"}
    sent = set(data.keys()) & (colleague_fields | lead_fields)
    if not sent:
        raise HTTPException(status_code=400, detail="Nothing to save")
    is_override = current_user.role in ("admin", "manager", "super_admin")   # may edit saved rows and every field

    if current_user.role == "colleague":
        if sent & lead_fields:
            raise HTTPException(status_code=403, detail="Accounts Audited and Errors are entered by your Team Lead")
        target_user_id = current_user.id
    else:
        if current_user.role == "team_lead" and sent & colleague_fields:
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
    elif not is_override:
        # Saved rows are locked; only Admin / Super Admin can change them.
        if sent & colleague_fields and (
            (st.hours_worked is not None or st.hours_status == "Pending") and st.hours_status != "Rejected"
        ):
            raise HTTPException(status_code=403, detail="These hours are already saved and locked. Ask an Admin to change them.")
        if sent & lead_fields and (st.accounts_audited is not None or st.errors is not None):
            raise HTTPException(status_code=403, detail="Audit figures are already saved and locked. Ask an Admin to change them.")
    for f in sent:
        if f == "hours_worked":
            continue            # handled below: may need approval
        if f == "comment":
            st.comment = (data[f] or "").strip() or None
            continue
        setattr(st, f, data[f])

    if "hours_worked" in sent:
        new_hours = data["hours_worked"]
        # The day's total across ALL processes (this one replaced by the new figure).
        others = db.query(func.coalesce(func.sum(models.DailyBatchStat.hours_worked), 0.0)).filter(
            models.DailyBatchStat.user_id == target_user_id,
            models.DailyBatchStat.work_date == payload.work_date,
            models.DailyBatchStat.process_id != payload.process_id,
        ).scalar() or 0.0
        if new_hours is not None and new_hours + others > HOURS_APPROVAL_THRESHOLD + 1e-9 and not is_override:
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
            if new_hours is not None and new_hours + others < STANDARD_SHIFT_HOURS - 1e-9 and not (st.comment or "").strip():
                db.rollback()
                raise HTTPException(
                    status_code=400,
                    detail=f"Comment is required when the total hours worked for the day are less than {STANDARD_SHIFT_HOURS:g}",
                )

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
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
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


@app.patch("/orders/{order_id}/timer/start", response_model=schemas.WorkOrderOut)
def start_timer(
    order_id: int,
    current_user: models.User = Depends(auth.require_role("colleague")),
    db: Session = Depends(get_db),
):
    """The colleague's Start button: begins Time Taken on an order that hasn't been started."""
    order = db.query(models.WorkOrder).filter(models.WorkOrder.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    if order.assigned_to_id != current_user.id:
        raise HTTPException(status_code=403, detail="This order is not assigned to you")
    if order.escalated or order.posting_status == "Completed":
        raise HTTPException(status_code=400, detail="This order is locked and its timer can't be changed")
    if order.timer_status != "not_started":
        raise HTTPException(status_code=400, detail="Timer has already been started")
    order.timer_status = "running"
    order.timer_started_at = datetime.now(IST).replace(tzinfo=None)
    db.commit()
    db.refresh(order)
    return order


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
    before = _order_snapshot(order)

    for field, value in payload_data.items():
        setattr(order, field, value)
    order.last_edited_by = current_user.username

    # Keep derived figures consistent with whatever the Team Lead just fixed.
    _recalc_pending(order)
    if order.posted_date and _tat_start(order):
        total_bdays = _count_business_days_ex_holidays(_tat_start(order), order.posted_date)
        pause_bdays = _pause_business_days(db, order)
        order.tat_days = total_bdays - pause_bdays

    if reasons:
        _log_order_change(db, current_user, order, "correction", reasons, before)
    _d = _order_diff_text(before, order)
    if _d:
        _audit_order(db, current_user, "Corrected order", order, _d)
    db.commit()
    db.refresh(order)
    return order


@app.patch("/orders/{order_id}/production-edit", response_model=schemas.WorkOrderOut)
def production_edit(
    order_id: int,
    payload: schemas.ProductionEdit,
    current_user: models.User = Depends(auth.require_role("team_lead", "admin", "manager", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    Edit a row that is already in Production. Team Leads: their own team's
    rows only; Admin / Super Admin: any row. Every change (who, when, old and
    new value) is recorded in the change log.
    """
    q = db.query(models.WorkOrder).filter(models.WorkOrder.id == order_id)
    if current_user.role == "team_lead":
        cond = _team_orders_condition(db, current_user)
        if cond is not None:
            q = q.filter(cond)
    order = q.first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    if current_user.role not in ("admin", "manager"):
        _require_process_access(current_user, order.process_id)
    if not order.submitted:
        raise HTTPException(status_code=400, detail="Only rows already submitted to Production can be edited here")

    data = payload.dict(exclude_unset=True)
    if "posting_status" in data and data["posting_status"] not in ("Completed", "In-Process", "Clarification"):
        raise HTTPException(status_code=400, detail="posting_status must be 'Completed', 'In-Process', or 'Clarification'")

    before = _order_snapshot(order)
    reasons = ["Production edit"] + _order_lock_reasons(order)
    for field, value in data.items():
        setattr(order, field, value)
    order.last_edited_by = current_user.username
    _recalc_pending(order)
    if order.posted_date and _tat_start(order):
        order.tat_days = _count_business_days_ex_holidays(_tat_start(order), order.posted_date) - _pause_business_days(db, order)

    _log_order_change(db, current_user, order, "production_edit", reasons, before)
    _d = _order_diff_text(before, order)
    if _d:
        _audit_order(db, current_user, "Edited order in Production", order, _d)
    db.commit()
    db.refresh(order)
    return order


@app.patch("/orders/{order_id}/resolve-escalation", response_model=schemas.WorkOrderOut)
def resolve_escalation(
    order_id: int,
    ventra_comment: str = Form(""),
    files: List[UploadFile] = File(default=[]),
    track: str = "",
    current_user: models.User = Depends(auth.require_role("team_lead", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    A Team Lead's VENTRA Comment resolves the escalation: it auto-stamps
    Issue Closed Date and unlocks the row back to the colleague — Posting
    Status stays 'Clarification' so they can finish posting it themselves.
    Optional attachments (multipart) are kept with the order and shown to the colleague.
    """
    order = db.query(models.WorkOrder).filter(models.WorkOrder.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    _require_process_access(current_user, order.process_id)
    if not order.escalated:
        raise HTTPException(status_code=400, detail="This order isn't currently escalated")
    tr = _track_for(db, order, track)
    blobs = _read_onshore_files(files)

    if tr.get("onshore_status") in ("with_onshore", "yellow", "blue", "orange"):
        raise HTTPException(status_code=400, detail="This escalation is with another team \u2014 it can be resolved once they respond (red)")
    if tr.get("onshore_status") == "red":
        # The team's answer becomes the VENTRA Comment, and their circle turns green.
        comment = (tr.get("onshore_comment") or "").strip()
        tr.set("onshore_status", "green")
        res_msg = _add_onshore_message(db, current_user, order, "resolved", comment, tr.msg_track)
    else:
        comment = (ventra_comment or "").strip()
        res_msg = None
    if not comment:
        raise HTTPException(status_code=400, detail="VENTRA Comment can't be empty")
    if blobs:
        if res_msg is None:
            res_msg = _add_onshore_message(db, current_user, order, "resolved", comment, tr.msg_track)
        _save_onshore_files(db, current_user, order, res_msg, blobs)

    # An escalation with several reasons runs one hand-off per reason, in parallel. Each is resolved on its own;
    # the escalation closes (and the order unlocks) only after the last one is resolved.
    det = tr.det
    done = []
    if det and order.escalation_category in ESCALATION_CATEGORY_FIELDS:
        dd = dict(det.data or {})
        done = list(dd.get("_done") or [])
        entry = {"category": tr.category, "comment": comment, "by": current_user.full_name, "at": _work_today().isoformat()}
        if not tr.primary:
            entry.update(tr.item)                       # keep the reason's details and its team's history for the Completed tab
            entry["onshore_status"] = "green" if entry.get("onshore_team") else None
        done.append(entry)
        if tr.primary:
            dd["_pdone"] = True
        else:
            dd["_queue"] = [x for x in (dd.get("_queue") or []) if x.get("category") != tr.category]
        dd["_done"] = done
        det.data = dd
        flag_modified(det, "data")
        if dd.get("_queue") or (not dd.get("_pdone")):
            order.last_edited_by = current_user.username
            db.commit()
            db.refresh(order)
            return order
        if len(done) > 1:
            comment = " | ".join(f"{d['category']}: {d['comment']}" for d in done)
    order.ventra_comment = comment
    order.issue_closed_date = _work_today()
    _close_escalation_event(db, order, current_user, comment)
    order.escalated = False
    order.last_edited_by = current_user.username
    # The clock stays paused after a Team Lead resolves it: the colleague
    # clicks Restart when they actually pick the order back up, so the waiting
    # time is never counted.
    db.commit()
    db.refresh(order)
    return order


# ---------------------------------------------------------------------------
# Onshore team: escalations a Team Lead couldn't resolve (Patient not found,
# Invoice Creation, Clarification only)
#   Team Lead  --send-->  Onshore queue (with_onshore)
#   Onshore    --red-->   back to the Team Lead for review (their comment)
#   Onshore    --yellow-> needs more information; the Team Lead answers and it
#                         goes back to Onshore (with_onshore)
#   Team Lead  --Resolve--> Onshore comment becomes the VENTRA Comment, green
# ---------------------------------------------------------------------------

ONSHORE_CATEGORIES = tuple(TEAM_FOR_CATEGORY.keys())


def _add_onshore_message(db: Session, user: models.User, order: models.WorkOrder, kind: str, text: Optional[str], track: Optional[str] = None):
    msg = models.OnshoreMessage(
        order_id=order.id, created_at=datetime.now(IST).replace(tzinfo=None),
        author_id=user.id, author_name=user.full_name, author_role=user.role, kind=kind, text=text, track=track or None,
    )
    db.add(msg)
    db.flush()
    return msg


def _read_onshore_files(files) -> list:
    """Validates optional uploads (max 5 files, 5 MB each) and returns [(UploadFile, bytes)]."""
    files = [f for f in (files or []) if f and f.filename]
    if len(files) > MAX_ATTACHMENTS_PER_UPDATE:
        raise HTTPException(status_code=400, detail=f"Max {MAX_ATTACHMENTS_PER_UPDATE} attachments")
    blobs = []
    for f in files:
        content = f.file.read()
        if len(content) > MAX_ATTACHMENT_BYTES:
            raise HTTPException(status_code=400, detail=f"{f.filename} is over the 5 MB limit")
        blobs.append((f, content))
    return blobs


def _save_onshore_files(db: Session, user: models.User, order: models.WorkOrder, msg, blobs: list):
    now = datetime.now(IST).replace(tzinfo=None)
    for f, content in blobs:
        db.add(models.OnshoreAttachment(
            message_id=msg.id, order_id=order.id, file_name=f.filename, content_type=f.content_type,
            file_data=base64.b64encode(content).decode("ascii"), uploaded_by_name=user.full_name, created_at=now,
        ))


_TRACK_STATE = ("onshore_status", "onshore_team", "onshore_comment", "onshore_tl_reply", "onshore_sent_at")


class _Track:
    """
    One reason of an escalation. The main reason keeps its hand-off state on the order itself; every extra reason
    (EOB not found AND Invoice Creation, say) keeps its own state inside EscalationDetail.data["_queue"], so each can
    sit with a different team at the same time.
    """
    def __init__(self, order: models.WorkOrder, det, category: str = ""):
        self.order, self.det, self.category = order, det, (category or "")
        self.primary = (not self.category) or self.category == order.escalation_category
        self.item = None
        if self.primary:
            self.category = order.escalation_category or ""
            if det and (det.data or {}).get("_pdone"):
                raise HTTPException(status_code=400, detail=f"{self.category} has already been resolved on this order")
        else:
            q = (det.data or {}).get("_queue") if det else None
            self.item = next((x for x in (q or []) if x.get("category") == self.category), None)
            if self.item is None:
                raise HTTPException(status_code=404, detail="That escalation reason isn't open on this order")

    def get(self, name):
        if self.primary:
            return getattr(self.order, name)
        v = self.item.get(name)
        if name == "onshore_sent_at" and isinstance(v, str):
            try:
                return datetime.fromisoformat(v)
            except ValueError:
                return None
        return v

    def set(self, name, value):
        if self.primary:
            setattr(self.order, name, value)
        else:
            self.item[name] = value.isoformat() if isinstance(value, datetime) else value
            flag_modified(self.det, "data")

    @property
    def msg_track(self):
        return None if self.primary else self.category


def _track_for(db: Session, order: models.WorkOrder, track: str = "") -> _Track:
    det = db.query(models.EscalationDetail).filter(models.EscalationDetail.order_id == order.id).first()
    return _Track(order, det, track)


def _extra_view(item, order: models.WorkOrder, ex: dict, det):
    """A copy of an order's row showing one extra reason: its own category, details and hand-off state."""
    v = item.model_copy(deep=True)
    v.track = ex.get("category")
    v.escalation_category = ex.get("category")
    v.escalation_detail = schemas.EscalationDetailOut(order_id=order.id, category=ex.get("category") or "", data=dict(ex.get("data") or {}), updated_at=det.updated_at)
    v.onshore_status, v.onshore_team = ex.get("onshore_status"), ex.get("onshore_team")
    v.onshore_comment, v.onshore_tl_reply = ex.get("onshore_comment"), ex.get("onshore_tl_reply")
    sent = ex.get("onshore_sent_at")
    try:
        v.onshore_sent_at = datetime.fromisoformat(sent) if isinstance(sent, str) else sent
    except ValueError:
        v.onshore_sent_at = None
    return v


def _details_by_order(db: Session, ids) -> dict:
    out = {}
    ids = list(ids)
    for i in range(0, len(ids), 800):
        for d in db.query(models.EscalationDetail).filter(models.EscalationDetail.order_id.in_(ids[i:i + 800])).all():
            out[d.order_id] = d
    return out


def _q_match(tab: str, my_team, st, team) -> bool:
    """Same rules as the team queue's SQL filters, for the extra reasons that live inside the details JSON."""
    if not st:
        return False
    if tab == "pending":
        if my_team == "calling":
            return team == "calling" and st in ("with_onshore", "blue", "orange")
        if my_team:
            return team == my_team and st == "with_onshore"
        return st in ("with_onshore", "blue", "orange")
    if st not in (("red", "yellow", "blue", "orange", "with_onshore") if tab == "awaiting" else ("green",)):
        return False
    if my_team == "recon":
        if team not in ("recon", "calling"):
            return False
    elif my_team and team != my_team:
        return False
    if tab == "awaiting" and my_team:
        return (st in ("red", "yellow") or team == "calling") if my_team == "recon" else st in ("red", "yellow")
    if tab == "awaiting":
        return st in ("red", "yellow", "blue", "orange")
    return True


def _lead_escalation_order(db: Session, user: models.User, order_id: int) -> models.WorkOrder:
    """The escalated order a Team Lead (or Super Admin) is acting on — their own team's only."""
    order = db.query(models.WorkOrder).filter(models.WorkOrder.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    _require_process_access(user, order.process_id)
    if user.role == "team_lead" and order.team_lead_id != user.id:
        raise HTTPException(status_code=403, detail="This order belongs to another Team Lead")
    if not order.escalated:
        raise HTTPException(status_code=400, detail="This order isn't currently escalated")
    return order


@app.post("/orders/{order_id}/send-to-onshore", response_model=schemas.WorkOrderOut)
def send_to_onshore(
    order_id: int,
    note: str = Form(""),
    files: List[UploadFile] = File(default=[]),
    track: str = "",
    current_user: models.User = Depends(auth.require_role("team_lead", "super_admin")),
    db: Session = Depends(get_db),
):
    """
    Team Lead couldn't resolve it: move the escalation to the next team's queue —
    Recon for EOB not found; Onshore for Patient not found, Invoice Creation and Clarification.
    """
    order = _lead_escalation_order(db, current_user, order_id)
    tr = _track_for(db, order, track)
    team = TEAM_FOR_CATEGORY.get(tr.category)
    if not team:
        raise HTTPException(status_code=400, detail="Only EOB not found (Recon) and Patient not found, Invoice Creation, Clarification (Onshore) can be sent to another team")
    if tr.get("onshore_status") is not None:
        raise HTTPException(status_code=400, detail="This escalation has already been sent to another team")
    blobs = _read_onshore_files(files)
    tr.set("onshore_status", "with_onshore")
    tr.set("onshore_team", team)
    tr.set("onshore_sent_at", datetime.now(IST).replace(tzinfo=None))
    tr.set("onshore_comment", None)
    tr.set("onshore_tl_reply", None)
    msg = _add_onshore_message(db, current_user, order, "sent", (note or "").strip() or None, tr.msg_track)
    _save_onshore_files(db, current_user, order, msg, blobs)
    db.commit()
    db.refresh(order)
    return order


@app.post("/orders/{order_id}/onshore-info", response_model=schemas.WorkOrderOut)
def onshore_info(
    order_id: int,
    text: str = Form(""),
    files: List[UploadFile] = File(default=[]),
    track: str = "",
    current_user: models.User = Depends(auth.require_role("team_lead", "super_admin")),
    db: Session = Depends(get_db),
):
    """The team asked for more information (yellow): the Team Lead answers and it returns to that team."""
    order = _lead_escalation_order(db, current_user, order_id)
    tr = _track_for(db, order, track)
    if tr.get("onshore_status") != "yellow":
        raise HTTPException(status_code=400, detail="The team hasn't asked for information on this order")
    text_ = (text or "").strip()
    if not text_:
        raise HTTPException(status_code=400, detail="Enter the information that was asked for")
    blobs = _read_onshore_files(files)
    tr.set("onshore_tl_reply", text_)
    tr.set("onshore_status", "with_onshore")
    msg = _add_onshore_message(db, current_user, order, "tl_info", text_, tr.msg_track)
    _save_onshore_files(db, current_user, order, msg, blobs)
    db.commit()
    db.refresh(order)
    return order


@app.post("/orders/{order_id}/onshore-respond", response_model=schemas.WorkOrderOut)
def onshore_respond(
    order_id: int,
    comment: str = Form(""),
    status: str = Form(...),
    files: List[UploadFile] = File(default=[]),
    track: str = "",
    current_user: models.User = Depends(auth.require_role("onshore", "recon", "calling")),
    db: Session = Depends(get_db),
):
    """
    A team's answer, with optional attachments:
      red    = done on our side — back to the Team Lead for review
      yellow = we need more information from the Team Lead
      blue   = (Recon only) unable to resolve — move it to the Calling team's queue
      orange = (Calling only) an update for the Team Lead; it stays in Calling's queue so they can
               add an updated comment later and finish with red
    """
    order = db.query(models.WorkOrder).filter(models.WorkOrder.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    tr = _track_for(db, order, track)
    st_, team_ = tr.get("onshore_status"), tr.get("onshore_team")
    waiting = st_ == "with_onshore" or (st_ in ("blue", "orange") and team_ == "calling")
    if not waiting or team_ != current_user.role:
        raise HTTPException(status_code=400, detail="This order isn't waiting for your team's response")
    allowed = {"recon": ("red", "yellow", "blue"), "calling": ("red", "yellow", "orange")}.get(current_user.role, ("red", "yellow"))
    if status not in allowed:
        raise HTTPException(status_code=400, detail="status must be " + " or ".join(f"'{a}'" for a in allowed))
    text_ = (comment or "").strip()
    if not text_:
        raise HTTPException(status_code=400, detail="Enter a comment")
    blobs = _read_onshore_files(files)

    tr.set("onshore_comment", text_)
    if status == "blue":
        tr.set("onshore_team", "calling")
        tr.set("onshore_status", "blue")
    else:
        tr.set("onshore_status", status)
    msg = _add_onshore_message(db, current_user, order, "team_" + status, text_, tr.msg_track)
    _save_onshore_files(db, current_user, order, msg, blobs)
    db.commit()
    db.refresh(order)
    return order


@app.get("/orders/{order_id}/onshore-history", response_model=List[schemas.OnshoreMessageOut])
def onshore_history(
    order_id: int,
    track: str = "",
    current_user: models.User = Depends(auth.require_role("colleague", "team_lead", "admin", "manager", "super_admin", "onshore", "recon", "calling")),
    db: Session = Depends(get_db),
):
    """The hand-off history (messages + attachments) of one escalation, for the Team Lead, Admin / Super Admin and the teams."""
    order = db.query(models.WorkOrder).filter(models.WorkOrder.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    if current_user.role == "team_lead" and order.team_lead_id != current_user.id:
        raise HTTPException(status_code=403, detail="This order belongs to another Team Lead")
    if current_user.role == "colleague" and order.assigned_to_id != current_user.id:
        raise HTTPException(status_code=403, detail="This order is assigned to another colleague")
    mq = db.query(models.OnshoreMessage).filter(models.OnshoreMessage.order_id == order_id)
    if track and track != order.escalation_category:
        mq = mq.filter(models.OnshoreMessage.track == track)
    else:
        mq = mq.filter(models.OnshoreMessage.track.is_(None))
    msgs = mq.order_by(models.OnshoreMessage.id.asc()).all()
    atts = {}
    for a in db.query(models.OnshoreAttachment).filter(models.OnshoreAttachment.order_id == order_id).all():
        atts.setdefault(a.message_id, []).append(a)
    out = []
    for m in msgs:
        mo = schemas.OnshoreMessageOut.model_validate(m)
        mo.attachments = [schemas.OnshoreAttachmentOut.model_validate(a) for a in atts.get(m.id, [])]
        out.append(mo)
    return out


@app.get("/onshore/attachments/{attachment_id}")
def get_onshore_attachment(
    attachment_id: int,
    current_user: models.User = Depends(auth.require_role("colleague", "team_lead", "admin", "manager", "super_admin", "onshore", "recon", "calling")),
    db: Session = Depends(get_db),
):
    """Serves an attachment from the hand-off history to the Team Lead, the teams and Admin / Super Admin."""
    att = db.query(models.OnshoreAttachment).filter(models.OnshoreAttachment.id == attachment_id).first()
    if not att:
        raise HTTPException(status_code=404, detail="Attachment not found")
    if current_user.role == "team_lead":
        order = db.query(models.WorkOrder).filter(models.WorkOrder.id == att.order_id).first()
        if not order or order.team_lead_id != current_user.id:
            raise HTTPException(status_code=403, detail="This attachment belongs to another Team Lead's order")
    if current_user.role == "colleague":
        order = db.query(models.WorkOrder).filter(models.WorkOrder.id == att.order_id).first()
        if not order or order.assigned_to_id != current_user.id:
            raise HTTPException(status_code=403, detail="This attachment belongs to another colleague's order")
    return Response(
        content=base64.b64decode(att.file_data),
        media_type=att.content_type or "application/octet-stream",
        headers={"Content-Disposition": f'inline; filename="{att.file_name}"'},
    )


@app.get("/onshore/queue", response_model=List[schemas.OnshoreItemOut])
def onshore_queue(
    tab: str = "pending",
    team: Optional[str] = None,
    process_id: Optional[int] = None,
    current_user: models.User = Depends(auth.require_role("onshore", "recon", "calling", "super_admin", "admin", "manager")),
    db: Session = Depends(get_db),
):
    """
    tab=pending   : waiting for this team (Calling also gets what Recon passed on: blue)
    tab=awaiting  : answered / needs info / passed on — with the Team Lead (or the next team)
    tab=completed : resolved by the Team Lead (green)
    A team sees only its own work (Recon also follows what it passed to Calling). Admin / Super
    Admin see every team and can narrow with ?team=onshore|recon|calling.
    """
    if tab not in ("pending", "awaiting", "completed"):
        raise HTTPException(status_code=400, detail="tab must be pending, awaiting or completed")
    q = db.query(models.WorkOrder).options(
        selectinload(models.WorkOrder.clarification_detail), selectinload(models.WorkOrder.escalation_detail)
    ).filter(models.WorkOrder.onshore_status.isnot(None))
    if process_id is not None:
        q = q.filter(models.WorkOrder.process_id == process_id)

    my_team = current_user.role if current_user.role in TEAM_ROLES else team
    if tab == "pending":
        if my_team == "calling":
            q = q.filter(models.WorkOrder.onshore_team == "calling", models.WorkOrder.onshore_status.in_(("with_onshore", "blue", "orange")))
        elif my_team:
            q = q.filter(models.WorkOrder.onshore_team == my_team, models.WorkOrder.onshore_status == "with_onshore")
        else:
            q = q.filter(models.WorkOrder.onshore_status.in_(("with_onshore", "blue", "orange")))
    else:
        statuses = ("red", "yellow", "blue", "orange", "with_onshore") if tab == "awaiting" else ("green",)
        q = q.filter(models.WorkOrder.onshore_status.in_(statuses))
        if my_team == "recon":
            q = q.filter(models.WorkOrder.onshore_team.in_(("recon", "calling")))
        elif my_team:
            q = q.filter(models.WorkOrder.onshore_team == my_team)
        if tab == "awaiting" and my_team:
            # "with_onshore" belongs on the pending tab for the team that holds it;
            # here it only shows work that has moved on to someone else (Recon -> Calling).
            if my_team == "recon":
                q = q.filter(or_(models.WorkOrder.onshore_status.in_(("red", "yellow")), models.WorkOrder.onshore_team == "calling"))
            else:
                q = q.filter(models.WorkOrder.onshore_status.in_(("red", "yellow")))
        elif tab == "awaiting":
            q = q.filter(models.WorkOrder.onshore_status.in_(("red", "yellow", "blue", "orange")))
    orders = q.order_by(models.WorkOrder.onshore_sent_at.asc(), models.WorkOrder.id.asc()).all()
    primary_ids = {o.id for o in orders}
    # Extra reasons of a multi-reason escalation keep their hand-off state inside the details JSON.
    xq = db.query(models.WorkOrder).options(
        selectinload(models.WorkOrder.clarification_detail), selectinload(models.WorkOrder.escalation_detail)
    ).filter(models.WorkOrder.id.in_(db.query(models.EscalationDetail.order_id).filter(
        cast(models.EscalationDetail.data, String).like('%"onshore_status"%'))))
    if process_id is not None:
        xq = xq.filter(models.WorkOrder.process_id == process_id)
    for xo in xq.all():
        if xo.id not in primary_ids:
            orders.append(xo)
    dets = _details_by_order(db, [o.id for o in orders])
    _attach_team_leads(db, orders, prefer="owner")
    names = {p.id: p.name for p in db.query(models.Process).all()}
    ids = [o.id for o in orders]
    msgs, atts = {}, {}
    if ids:
        for a in db.query(models.OnshoreAttachment).filter(models.OnshoreAttachment.order_id.in_(ids)).all():
            atts.setdefault(a.message_id, []).append(a)
        for m in db.query(models.OnshoreMessage).filter(models.OnshoreMessage.order_id.in_(ids)).order_by(models.OnshoreMessage.id.asc()).all():
            msgs.setdefault(m.order_id, []).append(m)

    def fill(item, o, track):
        item.team_lead_name = getattr(o, "team_lead_name", None)
        item.process_name = names.get(o.process_id)
        item.messages = []
        for m in msgs.get(o.id, []):
            if (m.track or None) != (track or None):
                continue
            mo = schemas.OnshoreMessageOut.model_validate(m)
            mo.attachments = [schemas.OnshoreAttachmentOut.model_validate(a) for a in atts.get(m.id, [])]
            item.messages.append(mo)
        return item

    out = []
    for o in orders:
        base = schemas.OnshoreItemOut.model_validate(o)
        if o.id in primary_ids:
            out.append(fill(base.model_copy(deep=True), o, None))
        det = dets.get(o.id)
        dd = (det.data or {}) if det else {}
        for ex in list(dd.get("_queue") or []) + [x for x in (dd.get("_done") or []) if x.get("data")]:
            if _q_match(tab, my_team, ex.get("onshore_status"), ex.get("onshore_team")):
                out.append(fill(_extra_view(base, o, ex, det), o, ex.get("category")))
    out.sort(key=lambda it: (str(it.onshore_sent_at or ""), it.id))
    return out


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


def _build_esc_fields(category: str, data: dict, order: models.WorkOrder, user: models.User, extra: bool = False) -> dict:
    """Auto fields recomputed from the order; only the category's manual fields come from `data`.
    For an extra reason of a multi-reason escalation the Poster Comment is its own, typed for that reason."""
    fields = ESCALATION_CATEGORY_FIELDS[category]
    merged = _compute_escalation_auto_fields(category, order, user)
    if extra:
        for key, _label, kind in fields:
            if kind == "poster_comment":
                merged[key] = str((data or {}).get(key) or "").strip() or None
    manual_kinds = ("manual", "manual_date", "manual_currency", "manual_select")
    manual_keys = {key for key, _label, kind in fields if kind in manual_kinds}
    for key in manual_keys:
        if key in (data or {}):
            merged[key] = data[key]
    if "amount" in manual_keys and merged.get("amount") in (None, "") and order.amount is not None:
        merged["amount"] = str(order.amount)      # Amount defaults to the order's own Amount
    if "utility_category" in manual_keys and merged.get("utility_category"):
        if merged["utility_category"] not in UTILITY_CATEGORY_OPTIONS:
            raise HTTPException(status_code=400, detail=f"utility_category must be one of {UTILITY_CATEGORY_OPTIONS}")
    return merged


def _refresh_queue(queue, order: models.WorkOrder, user: models.User) -> list:
    """Waiting reasons keep what the colleague typed; their auto fields are re-derived from the order as it is now."""
    out = []
    for q in (queue or []):
        c = q.get("category")
        if c in ESCALATION_CATEGORY_FIELDS:
            item = {k: v for k, v in q.items() if k not in ("category", "data")}      # keep the reason's own hand-off state
            item.update({"category": c, "data": _build_esc_fields(c, q.get("data") or {}, order, user, extra=True)})
            out.append(item)
    return out


def _event_cats(e) -> list:
    """Every reason an escalation carried (an escalation can have several, e.g. EOB not found + Invoice Creation)."""
    try:
        allc = json.loads(e.detail_json or "{}").get("_all")
    except (ValueError, AttributeError):
        allc = None
    return list(allc) if allc else [e.category or "(none)"]


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

    merged = _build_esc_fields(category, payload.data, order, current_user)
    # One escalation can carry more than one reason (e.g. EOB not found AND Invoice Creation). The first is worked now;
    # the others wait in _queue and come up one by one as each is resolved. The escalation closes only after the last.
    queue, seen = [], {category}
    for ex in (payload.extras or []):
        c2 = str((ex or {}).get("category") or "")
        if not c2 or c2 in seen:
            continue
        if c2 not in ESCALATION_CATEGORY_FIELDS:
            raise HTTPException(status_code=400, detail=f"'{c2}' can't be added as an extra reason")
        seen.add(c2)
        queue.append({"category": c2, "data": _build_esc_fields(c2, (ex or {}).get("data") or {}, order, current_user, extra=True)})
    if queue:
        merged["_queue"] = queue

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


def _open_escalation_event(db: Session, order: models.WorkOrder, user: models.User, snapshot: dict) -> None:
    """A colleague raises a (new) query: log it with its own Issue Raised Date and reset the team hand-off."""
    today = _work_today()
    qc = [q.get("category") for q in (snapshot.get("_queue") or []) if q.get("category")]
    if qc:
        snapshot = dict(snapshot); snapshot["_all"] = [order.escalation_category] + qc
    db.add(models.EscalationEvent(
        order_id=order.id, process_id=order.process_id, team_lead_id=order.team_lead_id,
        category=order.escalation_category, raised_date=today, poster_comment=order.poster_comment,
        detail_json=json.dumps(snapshot, default=str), raised_by_name=user.full_name,
        created_at=datetime.now(IST).replace(tzinfo=None),
    ))
    order.issue_raised_date = today
    order.issue_closed_date = None
    order.onshore_status = None
    order.onshore_team = None
    order.onshore_comment = None
    order.onshore_tl_reply = None
    order.onshore_sent_at = None


def _close_escalation_event(db: Session, order: models.WorkOrder, user: models.User, comment: str) -> None:
    ev = (
        db.query(models.EscalationEvent)
        .filter(models.EscalationEvent.order_id == order.id, models.EscalationEvent.closed_date.is_(None))
        .order_by(models.EscalationEvent.id.desc()).first()
    )
    if ev:
        ev.closed_date = _work_today()
        ev.ventra_comment = comment
        ev.resolved_by_name = user.full_name


def _pause_business_days(db: Session, order: models.WorkOrder) -> int:
    """Business days spent waiting on Team Lead answers: every closed query's raised -> closed window added up."""
    events = db.query(models.EscalationEvent).filter(
        models.EscalationEvent.order_id == order.id, models.EscalationEvent.closed_date.isnot(None),
        models.EscalationEvent.raised_date.isnot(None),
    ).all()
    if events:
        return sum(_count_business_days_ex_holidays(e.raised_date, e.closed_date) for e in events)
    if order.issue_raised_date and order.issue_closed_date:      # orders from before queries were logged
        return _count_business_days_ex_holidays(order.issue_raised_date, order.issue_closed_date)
    return 0


def _tat_start(order) -> Optional[date]:
    """TAT starts on the Created date of the inventory line (falls back to Received Date if there isn't one)."""
    if order.created:
        return order.created.date() if isinstance(order.created, datetime) else order.created
    return order.received_date


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
