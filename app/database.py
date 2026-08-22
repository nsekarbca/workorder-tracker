import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# Reads DATABASE_URL from environment. Falls back to local SQLite file
# for easy testing on your own machine. On Render, set DATABASE_URL to
# your Supabase/Neon Postgres connection string (starts with postgresql://).
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./workorders.db")

connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}

engine = create_engine(DATABASE_URL, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
