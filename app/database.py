import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# Reads DATABASE_URL from environment. Falls back to local SQLite file
# for easy testing on your own machine. On Render, set DATABASE_URL to
# your Supabase/Neon Postgres connection string (starts with postgresql://).
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./workorders.db")

connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}

if DATABASE_URL.startswith("sqlite"):
    connect_args["timeout"] = 30          # wait up to 30 s for a lock instead of failing at once

engine = create_engine(DATABASE_URL, connect_args=connect_args, pool_pre_ping=not DATABASE_URL.startswith("sqlite"))

if DATABASE_URL.startswith("sqlite"):
    from sqlalchemy import event

    @event.listens_for(engine, "connect")
    def _sqlite_pragmas(dbapi_conn, _):
        """WAL lets readers and one writer work together (far fewer 'database is locked' errors);
        busy_timeout makes a writer wait for the lock. Set SQLITE_WAL=0 if the file is on a network share."""
        cur = dbapi_conn.cursor()
        try:
            if os.getenv("SQLITE_WAL", "1") != "0":
                cur.execute("PRAGMA journal_mode=WAL")
                cur.execute("PRAGMA synchronous=NORMAL")
            cur.execute("PRAGMA busy_timeout=30000")
        except Exception:
            pass
        finally:
            cur.close()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
