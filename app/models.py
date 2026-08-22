from sqlalchemy import Column, Integer, String, Float, Date, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .database import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True, nullable=False)
    full_name = Column(String, nullable=False)
    password_hash = Column(String, nullable=False)
    # role: "team_lead" or "colleague"
    role = Column(String, nullable=False, default="colleague")
    # Optional staff/employee ID, copied onto WorkOrder.employee_id automatically
    # at assignment time. Falls back to username if not set.
    employee_id = Column(String, nullable=True)

    orders = relationship("WorkOrder", back_populates="assignee")


class WorkOrder(Base):
    __tablename__ = "work_orders"

    id = Column(Integer, primary_key=True, index=True)

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

    assigned_to_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    assignee = relationship("User", back_populates="orders")

    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
