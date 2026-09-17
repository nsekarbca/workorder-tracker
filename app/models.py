from sqlalchemy import Column, Integer, String, Float, Date, DateTime, ForeignKey, Boolean, Table
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
    """
    __tablename__ = "celebration_comments"

    id = Column(Integer, primary_key=True, index=True)
    target_user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
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

    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
