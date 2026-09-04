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

    users = relationship("User", secondary=user_process_association, back_populates="processes")


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

    orders = relationship("WorkOrder", back_populates="assignee")
    processes = relationship("Process", secondary=user_process_association, back_populates="users")


class WorkOrder(Base):
    __tablename__ = "work_orders"

    id = Column(Integer, primary_key=True, index=True)

    # Which process this order belongs to — every order lives in exactly one
    # process, and every query is scoped to the process the user selected at
    # login.
    process_id = Column(Integer, ForeignKey("processes.id"), nullable=True)
    process = relationship("Process")

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
    assignee = relationship("User", back_populates="orders")

    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
