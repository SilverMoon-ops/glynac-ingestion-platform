from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, declarative_base

from app.config import settings

def make_engine(url: str, **overrides):
    """
    SQLAlchemy engine. For SQLite the connection pool is sized for many concurrent jobs: every
    running job (10 Salesforce + 8 HubSpot + Slack channel workers) holds a connection while it
    waits on the network, plus the web requests. SQLAlchemy's default of 15 is exhausted by one
    click on "Sync all" ("QueuePool limit ... reached"), after which every request fails.
    """
    if not url.startswith("sqlite"):
        return create_engine(url, **overrides)
    options = {"pool_size": 50, "max_overflow": 50, "pool_timeout": 60}
    options.update(overrides)
    eng = create_engine(url, connect_args={"check_same_thread": False}, **options)

    @event.listens_for(eng, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA busy_timeout=30000")  # wait for a competing writer instead of failing
        cursor.close()

    return eng


engine = make_engine(settings.database_url)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    """FastAPI dependency: yields a DB session, always closes it."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    # Import models here so they're registered on Base before create_all runs.
    from app import models  # noqa: F401

    Base.metadata.create_all(bind=engine)
    _ensure_columns()


def _ensure_columns():
    """create_all never alters existing tables; add columns introduced later."""
    from sqlalchemy import inspect, text

    wanted = {
        "jobs": {"control": "VARCHAR", "owner": "VARCHAR"},
        "channel_checkpoint": {"completed": "BOOLEAN NOT NULL DEFAULT 0"},
    }
    inspector = inspect(engine)
    with engine.begin() as conn:
        for table, columns in wanted.items():
            existing = {c["name"] for c in inspector.get_columns(table)}
            for name, ddl in columns.items():
                if name not in existing:
                    conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}"))
