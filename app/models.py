from sqlalchemy import Column, Integer, String, Float, Date, DateTime, ForeignKey, Boolean, Table, JSON, UniqueConstraint, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .database import Base

# Fixed list of processes the org runs. Seeded into the processes table at
# startup (see main.py) — adding a new one later just means adding it here
# and restarting, no manual SQL needed.
PROCESS_NAMES = [
    "EDM", "ECOM", "IBIS", "CB Unmatched Posting", "VCC",
    "CB Invoice Issue", "835 Push", "Correspondence (Zero Payment Posting)",
    "Email Task",
]

# Many-to-many: which processes each user can work in.
user_process_association = Table(
    "user_process_association",
    Base.metadata,
    Column("user_id", Integer, ForeignKey("users.id"), primary_key=True),
    Column("process_id", Integer, ForeignKey("processes.id"), primary_key=True),
)


class Process(Base):
    __tablename__ = "processes"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, nullable=False)
    # Optional daily production target for this process, set by a Super
    # Admin. Purely informational at this stage — nullable so existing
    # processes (and newly-created ones left blank) don't require a value.
    daily_target = Column(Integer, nullable=True)

    users = relationship("User", secondary=user_process_association, back_populates="processes")


class CelebrationComment(Base):
    """
    A comment left on a colleague's birthday/work-anniversary entry on the
    org-wide 'Today's Celebrations' section. Open to any logged-in user,
    regardless of process — this is a social feature, not process data.
    occasion_date scopes the comment to the specific calendar day it was
    posted on: without it, a comment from last year's birthday would keep
    resurfacing every later date that person shows up for (their work
    anniversary, next year's birthday, etc.), since it's otherwise only
    tied to the person, not to which celebration it was actually for.
    """
    __tablename__ = "celebration_comments"

    id = Column(Integer, primary_key=True, index=True)
    target_user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    occasion_date = Column(Date, nullable=True)
    message = Column(String, nullable=False)
    posted_by_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    posted_by_name = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now())


class CelebrationReaction(Base):
    """
    A like/heart left on a specific comment under a colleague's
    birthday/anniversary entry (not on the entry itself). One row per
    (comment, reactor, reaction type) — clicking the same reaction again
    removes it (toggle), so a person can't stack up the same reaction
    multiple times on one comment.
    """
    __tablename__ = "celebration_reactions"

    id = Column(Integer, primary_key=True, index=True)
    comment_id = Column(Integer, ForeignKey("celebration_comments.id"), nullable=False)
    reaction = Column(String, nullable=False)  # "like" | "heart"
    posted_by_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    posted_by_name = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now())


class ProcessUpdate(Base):
    """
    A structured update posted to one or more processes' Home screens —
    e.g. a Team Lead logging how a payer communication came in and what
    was done about it. Posting "to all processes" creates one row per
    process rather than a single shared row, so each process's list stays
    independently filterable/queryable. Newest shows first and most
    prominently on the Home screen.
    """
    __tablename__ = "process_updates"

    id = Column(Integer, primary_key=True, index=True)
    process_id = Column(Integer, ForeignKey("processes.id"), nullable=False)
    received_date = Column(Date, nullable=True)
    mode = Column(String, nullable=True)  # Team message / Email / Smartsheet / Call
    received_from = Column(String, nullable=True)
    category = Column(String, nullable=True)  # Payer / Adjustment / Generic
    status = Column(String, nullable=False, default="Active")  # Active / Inactive
    message = Column(String, nullable=False)  # the update comment itself
    verified_by = Column(String, nullable=True)  # Verified/Approved by
    posted_by_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    posted_by_name = Column(String, nullable=False)
    posted_by_role = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, nullable=True)
    updated_by_name = Column(String, nullable=True)
    updated_by_role = Column(String, nullable=True)

    attachments = relationship(
        "ProcessUpdateAttachment",
        order_by="ProcessUpdateAttachment.created_at",
        cascade="all, delete-orphan",
    )


class ProcessUpdateAttachment(Base):
    """
    An image or document attached to a Process Update. Stored inline as
    base64 in Postgres — there's no separate object storage (S3, Supabase
    Storage, etc.) configured for this app, so this keeps things working
    with zero extra setup. Capped at 5 MB/file and 5 files/update (enforced
    in the upload endpoint) to keep that reasonable; if attachments end up
    being used heavily, moving this to real object storage would be worth
    revisiting.
    """
    __tablename__ = "process_update_attachments"

    id = Column(Integer, primary_key=True, index=True)
    process_update_id = Column(Integer, ForeignKey("process_updates.id"), nullable=False)
    file_name = Column(String, nullable=False)
    content_type = Column(String, nullable=True)
    file_data = Column(String, nullable=False)  # base64-encoded file bytes
    uploaded_by_name = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now())


class ClarificationDetail(Base):
    """
    The extra structured detail captured when a colleague sets an order's
    Escalation Category to 'Clarification' — one row per order (upsert,
    not append-only), filled via a popup rather than grid columns since
    most of the app's users never touch it. Several fields are
    auto-filled and not user-editable (deposit_type from the process
    name, exchange/era_check/team are fixed constants, edm/bar batch
    number, batch description and amount posted are copied from the
    order itself) — only escalation_type and clarification_details are
    actually typed in by the colleague.
    """
    __tablename__ = "clarification_details"

    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(Integer, ForeignKey("work_orders.id"), nullable=False, unique=True)
    deposit_type = Column(String, nullable=True)  # process name, e.g. "EDM"
    exchange = Column(String, nullable=True, default="-")
    era_check = Column(String, nullable=True, default="-")
    edm_batch_number = Column(String, nullable=True)
    bar_batch_number = Column(String, nullable=True)
    batch_description = Column(String, nullable=True)
    escalation_type = Column(String, nullable=True)  # e.g. "Duplicate", "Images", ...
    clarification_details = Column(String, nullable=True)  # free text, manual input
    team = Column(String, nullable=True, default="CBE")
    poster_login = Column(String, nullable=True)  # full name of whoever filled this in
    amount_posted = Column(String, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, nullable=True)


class EscalationDetail(Base):
    """
    Structured detail for an escalation category OTHER than Clarification
    (which has its own dedicated ClarificationDetail table/workflow,
    including the lock-to-Team-Lead handoff). EOB not found, Invoice
    Creation, Patient not found, and Need to Delete each have a different
    field set (see ESCALATION_CATEGORY_FIELDS in main.py), so rather than
    a wide sparse table with dozens of mostly-null columns, the
    category-specific values live in a single JSON column. This is pure
    data capture — saving one of these does NOT lock the row or hand it
    to the Team Lead the way Clarification does.
    """
    __tablename__ = "escalation_details"

    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(Integer, ForeignKey("work_orders.id"), nullable=False, unique=True)
    category = Column(String, nullable=False)
    data = Column(JSON, nullable=False, default=dict)
    posted_by_name = Column(String, nullable=False)
    updated_at = Column(DateTime, nullable=True)


class AppSetting(Base):
    """Simple key-value store for admin-configurable settings, e.g. the
    inactivity session timeout. Not tied to any one user."""
    __tablename__ = "app_settings"

    key = Column(String, primary_key=True)
    value = Column(String, nullable=False)


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True, nullable=False)
    full_name = Column(String, nullable=False)
    password_hash = Column(String, nullable=False)
    # role: "colleague", "team_lead", or "super_admin"
    role = Column(String, nullable=False, default="colleague")
    # Optional staff/employee ID, copied onto WorkOrder.employee_id automatically
    # at assignment time. Falls back to username if not set.
    employee_id = Column(String, nullable=True)

    # Forces a password-change screen on next login — set True whenever a
    # Super Admin creates the account or resets the password.
    must_change_password = Column(Boolean, nullable=False, default=True)

    # Profile fields, settable only by a Super Admin at creation/edit time.
    email = Column(String, nullable=True)
    dob = Column(Date, nullable=True)
    doj = Column(Date, nullable=True)
    anniversary_date = Column(Date, nullable=True)
    designation = Column(String, nullable=True)
    reporting_manager = Column(String, nullable=True)
    employment_status = Column(String, nullable=False, default="Active")  # "Active" or "Inactive"

    # IST date of the colleague's most recent login. Auto-assignment only offers
    # orders to colleagues who have logged in today.
    last_login_date = Column(Date, nullable=True)

    # Forgot-password flow: a short-lived token emailed to the user, cleared
    # once used or once a new one is issued.
    reset_token = Column(String, nullable=True)
    reset_token_expires = Column(DateTime, nullable=True)

    orders = relationship("WorkOrder", back_populates="assignee", foreign_keys="WorkOrder.assigned_to_id")
    processes = relationship("Process", secondary=user_process_association, back_populates="users")


class WorkOrder(Base):
    __tablename__ = "work_orders"

    id = Column(Integer, primary_key=True, index=True)

    # Which process this order belongs to — every order lives in exactly one
    # process, and every query is scoped to the process the user selected at
    # login.
    process_id = Column(Integer, ForeignKey("processes.id"), nullable=True)
    process = relationship("Process")

    # Which Team Lead's import this order came from — auto-assignment only
    # offers it to colleagues who report to that Team Lead. Null means it
    # was imported by a Super Admin and is open to any colleague in the
    # process.
    team_lead_id = Column(Integer, ForeignKey("users.id"), nullable=True)

    # --- A-D: Team Lead fields ---
    received_date = Column(Date)
    assigned_date = Column(Date)
    employee_id = Column(String)          # C: Employee
    employee_name = Column(String)        # D: Employee Name

    # --- E-O: Inventory data (bulk-imported / auto-populated) ---
    edm = Column(String)                  # E
    status = Column(String)               # F
    created = Column(DateTime)            # G: Created
    image_count = Column(Integer)         # H: Image
    doc_count = Column(Integer)           # I: Doc
    def_doc_type = Column(String)         # J: Def Doc Type
    amount = Column(Float)                # K: Amount
    last_edited_by = Column(String)       # L: Last Edited By (auto-set on save)
    description = Column(String)          # M
    division = Column(String)             # N
    deposit_date = Column(Date)           # O

    # --- P-Z: Colleague-filled fields ---
    posted_amount = Column(Float)         # P: Posted $
    pending_amount = Column(Float)        # Q: Pending $
    bar_batch = Column(String)            # R: BAR Batch
    trans_count = Column(Integer)         # S: Trans Count
    posting_status = Column(String)       # T: Posting Status
    poster_comment = Column(String)       # U
    ventra_comment = Column(String)       # V
    escalation_category = Column(String)  # W
    issue_raised_date = Column(Date)      # X
    issue_closed_date = Column(Date)      # Y
    posted_date = Column(Date)            # Z

    # --- AA: auto-calculated ---
    tat_days = Column(Integer)            # TAT = posted_date - received_date, excluding issue pause window

    # Set true by the colleague's end-of-day Submit action. Submitted orders
    # are treated as moved to Production — hidden from both the colleague and
    # Team Lead active views, and locked from further edits by anyone.
    submitted = Column(Boolean, default=False, nullable=False)
    # When the end-of-day Submit action moved this row to Production.
    submitted_at = Column(DateTime, nullable=True)

    assigned_to_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    assignee = relationship("User", back_populates="orders", foreign_keys=[assigned_to_id])

    # True while a Clarification escalation is sitting with the Team Lead
    # awaiting their VENTRA Comment — locks the row from the colleague (it
    # stays visible in their queue, read-only) and frees their one-open-
    # order slot for a new assignment. Cleared back to False the moment the
    # Team Lead resolves it, handing the row back to the colleague.
    escalated = Column(Boolean, default=False, nullable=False)
    clarification_detail = relationship("ClarificationDetail", uselist=False, cascade="all, delete-orphan")
    escalation_detail = relationship("EscalationDetail", uselist=False, cascade="all, delete-orphan")

    # Onshore hand-off (escalation categories Patient not found / Invoice
    # Creation / Clarification only). onshore_status: NULL = never sent,
    # "with_onshore" = waiting for the Onshore team, "red" = Onshore answered
    # (Team Lead review), "yellow" = Onshore needs more information from the
    # Team Lead, "green" = Team Lead resolved it.
    onshore_status = Column(String, nullable=True, index=True)
    onshore_comment = Column(Text, nullable=True)       # Onshore's latest comment / question
    onshore_tl_reply = Column(Text, nullable=True)      # Team Lead's latest answer to a yellow request
    onshore_sent_at = Column(DateTime, nullable=True)
    onshore_team = Column(String, nullable=True)        # onshore | recon | calling: which team holds it now

    # "Time Taken" tracking — active working time only, not wall-clock
    # time since assignment. Starts "not_started" when an order is
    # (auto- or re-)assigned and only runs once the colleague clicks Start; time_taken_seconds accumulates each time the
    # colleague pauses, the row completes, or it gets locked by an
    # escalation. timer_started_at is the wall-clock moment the current
    # running session began (None while paused/stopped).
    timer_status = Column(String, nullable=False, default="not_started")  # "not_started" | "running" | "paused" | "stopped"
    timer_started_at = Column(DateTime, nullable=True)
    time_taken_seconds = Column(Integer, nullable=False, default=0)

    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class DailyBatchStat(Base):
    """
    Manual inputs for the Batch Count Dashboard, one row per colleague per
    process per work date. Everything else on that dashboard (processes
    worked, # of batches, total transaction count, Production %, Quality %)
    is calculated live from work_orders and never stored, so it can't drift.

    hours_worked is typed by the colleague; accounts_audited and errors are
    typed by their Team Lead.
    """
    __tablename__ = "daily_batch_stats"
    __table_args__ = (
        UniqueConstraint("user_id", "process_id", "work_date", name="uq_daily_batch_stat"),
    )

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    process_id = Column(Integer, ForeignKey("processes.id"), nullable=False)
    work_date = Column(Date, nullable=False, index=True)

    hours_worked = Column(Float, nullable=True)        # colleague
    accounts_audited = Column(Integer, nullable=True)  # team lead
    errors = Column(Integer, nullable=True)            # team lead

    # Daily total over 8 hours needs Team Lead approval. The requested figure
    # waits in pending_hours; hours_worked only ever holds approved / normal hours.
    pending_hours = Column(Float, nullable=True)
    hours_status = Column(String, nullable=True)       # Pending | Approved | Rejected | NULL
    hours_decided_by = Column(String, nullable=True)
    hours_decided_at = Column(DateTime, nullable=True)

    updated_by = Column(String, nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class OrderChangeLog(Base):
    """
    Audit trail: every change a Team Lead makes to an order that is locked or
    completed (Completed, escalated-and-locked, or submitted to Production) —
    corrections, assignment edits, reassigns and deletions. Shown to Admin and
    Super Admin. order_id is deliberately NOT a foreign key so the entry
    survives when the order itself is deleted; edm / employee_name are copied
    at the time for the same reason.
    """
    __tablename__ = "order_change_log"

    id = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime, nullable=False, index=True)   # IST, set in code
    process_id = Column(Integer, nullable=True, index=True)
    order_id = Column(Integer, nullable=False, index=True)
    edm = Column(String, nullable=True)
    employee_name = Column(String, nullable=True)               # colleague on the order at the time
    actor_id = Column(Integer, nullable=True, index=True)
    actor_username = Column(String, nullable=True)
    actor_name = Column(String, nullable=True)
    actor_role = Column(String, nullable=True)
    action = Column(String, nullable=False)                     # correction | edit | reassign | delete
    order_state = Column(String, nullable=True)                 # e.g. "Completed", "Escalated (locked)"
    changes = Column(JSON, nullable=False, default=list)        # [{"field", "old", "new"}, ...]


class Client(Base):
    """
    Client master list (Facility No + name), managed by a Super Admin under
    Admin -> Clients. A client is Active, or Inactive since `inactive_date`.
    Which Team Lead looks after it in each process is in
    ClientProcessTeamLead. Master data only — nothing in order routing reads
    it (yet).
    """
    __tablename__ = "clients"

    id = Column(Integer, primary_key=True, index=True)
    facility_no = Column(String, unique=True, nullable=False, index=True)
    client_name = Column(String, nullable=False)
    status = Column(String, nullable=False, default="Active")      # Active | Inactive
    inactive_date = Column(Date, nullable=True)                    # set only while Inactive
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class ClientProcessTeamLead(Base):
    """The Team Lead assigned to one client in one process (one row per client + process)."""
    __tablename__ = "client_process_team_leads"
    __table_args__ = (
        UniqueConstraint("client_id", "process_id", name="uq_client_process_team_lead"),
    )

    id = Column(Integer, primary_key=True, index=True)
    client_id = Column(Integer, ForeignKey("clients.id", ondelete="CASCADE"), nullable=False, index=True)
    process_id = Column(Integer, ForeignKey("processes.id"), nullable=False)
    team_lead_id = Column(Integer, ForeignKey("users.id"), nullable=False)


class ImportException(Base):
    """
    Rows an inventory import deliberately did NOT bring in and that someone
    should know about (shown under "Import Exceptions"):
      unassigned_client   the row's client isn't in the client list, or has no
                          Team Lead for this process  -> Admin / Super Admin
      duplicate_completed the EDM was already imported in the last month and
                          is Completed                 -> Team Lead / Admin / Super Admin
    One entry per document: the same EDM isn't logged again within 30 days.
    """
    __tablename__ = "import_exceptions"

    id = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime, nullable=False, index=True)    # IST, set in code
    process_id = Column(Integer, nullable=True, index=True)
    kind = Column(String, nullable=False, index=True)            # unassigned_client | duplicate_completed
    edm = Column(String, nullable=True, index=True)
    division_raw = Column(String, nullable=True)                 # what the file's Division column said
    facility_no = Column(String, nullable=True)                  # matched client, if any
    client_name = Column(String, nullable=True)
    def_doc_type = Column(String, nullable=True)
    amount = Column(Float, nullable=True)
    reason = Column(String, nullable=True)
    imported_by_id = Column(Integer, nullable=True)
    imported_by_name = Column(String, nullable=True)
    imported_by_role = Column(String, nullable=True)
    team_lead_id = Column(Integer, nullable=True, index=True)    # the Team Lead this row was meant for (if known)
    existing_order_id = Column(Integer, nullable=True)           # duplicate_completed: the order already done
    existing_posted_date = Column(Date, nullable=True)
    existing_employee_name = Column(String, nullable=True)
    row_json = Column(Text, nullable=True)                       # the whole inventory row, so an unassigned one can be moved into a queue later


class EscalationEvent(Base):
    """
    One query (escalation) raised on an order. An order can be escalated more than
    once — on the same day or on different days — and each one keeps its own Issue
    Raised / Issue Closed Date, category, comments and a snapshot of its details.
    TAT's paused time is the sum of the closed ones. order_id is deliberately not an FK.
    """
    __tablename__ = "escalation_events"

    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(Integer, nullable=False, index=True)
    process_id = Column(Integer, nullable=True, index=True)
    team_lead_id = Column(Integer, nullable=True, index=True)
    category = Column(String, nullable=True)
    raised_date = Column(Date, nullable=True)
    closed_date = Column(Date, nullable=True)
    poster_comment = Column(Text, nullable=True)
    ventra_comment = Column(Text, nullable=True)
    detail_json = Column(Text, nullable=True)        # the popup's values when it was raised
    raised_by_name = Column(String, nullable=True)
    resolved_by_name = Column(String, nullable=True)
    created_at = Column(DateTime, nullable=True)


class OnshoreMessage(Base):
    """History of an order's Onshore hand-off: sent, Onshore replies (red / yellow), Team Lead info, resolved."""
    __tablename__ = "onshore_messages"

    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(Integer, nullable=False, index=True)     # not a foreign key: survives order deletion
    created_at = Column(DateTime, nullable=False)              # IST
    author_id = Column(Integer, nullable=True)
    author_name = Column(String, nullable=True)
    author_role = Column(String, nullable=True)
    kind = Column(String, nullable=False)                      # sent | onshore_red | onshore_yellow | tl_info | resolved
    text = Column(Text, nullable=True)


class OnshoreAttachment(Base):
    """Optional file a Recon / Calling / Onshore user attaches to a reply. Stored inline as base64 (5 MB max each)."""
    __tablename__ = "onshore_attachments"

    id = Column(Integer, primary_key=True, index=True)
    message_id = Column(Integer, nullable=False, index=True)
    order_id = Column(Integer, nullable=False, index=True)    # not a foreign key: survives order deletion
    file_name = Column(String, nullable=False)
    content_type = Column(String, nullable=True)
    file_data = Column(Text, nullable=False)                  # base64
    uploaded_by_name = Column(String, nullable=True)
    created_at = Column(DateTime, nullable=False)
