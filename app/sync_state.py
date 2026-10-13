"""Incremental-sync high-water marks (the custom state store behind HubSpot incremental loads)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List, Optional

from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import SyncState


def parse_ts(value: str) -> datetime:
    """ISO-8601 (with Z or an offset) or epoch milliseconds -> aware UTC datetime."""
    value = str(value).strip()
    if value.isdigit():
        return datetime.fromtimestamp(int(value) / 1000, timezone.utc)
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def newest(a: Optional[str], b: Optional[str]) -> Optional[str]:
    """The later of two timestamps (either may be None)."""
    if not a:
        return b
    if not b:
        return a
    return a if parse_ts(a) >= parse_ts(b) else b


def get_high_water(db: Session, service: str, org_id: str, object_name: str) -> Optional[str]:
    row = db.query(SyncState).filter_by(service=service, org_id=org_id, object_name=object_name).first()
    return row.high_water_mark if row else None


def advance_high_water(db: Session, service: str, org_id: str, object_name: str, value: Optional[str]) -> None:
    """
    Move the mark forward, never backwards, in one atomic SQL statement (two runs finishing at
    the same moment must not let the older value win). The stored values are normalised UTC
    ISO-8601 strings, which sort chronologically as text.
    """
    if not value:
        return
    new = iso(parse_ts(value))
    try:
        db.add(SyncState(service=service, org_id=org_id, object_name=object_name, high_water_mark=new))
        db.commit()
        return
    except IntegrityError:
        db.rollback()  # the mark already exists
    db.execute(
        update(SyncState)
        .where(SyncState.service == service, SyncState.org_id == org_id,
               SyncState.object_name == object_name, SyncState.high_water_mark < new)
        .values(high_water_mark=new)
    )
    db.commit()


def reset_high_water(db: Session, service: str, org_id: str, object_name: str) -> bool:
    row = db.query(SyncState).filter_by(service=service, org_id=org_id, object_name=object_name).first()
    if row is None:
        return False
    db.delete(row)
    db.commit()
    return True


def list_states(db: Session, service: Optional[str] = None, org_id: Optional[str] = None) -> List[dict]:
    q = db.query(SyncState)
    if service:
        q = q.filter_by(service=service)
    if org_id:
        q = q.filter_by(org_id=org_id)
    return [
        {"service": r.service, "org_id": r.org_id, "object_name": r.object_name,
         "high_water_mark": r.high_water_mark, "updated_at": r.updated_at.isoformat() if r.updated_at else None}
        for r in q.order_by(SyncState.service, SyncState.org_id, SyncState.object_name).all()
    ]


def with_overlap(high_water: str, seconds: int) -> str:
    """Start slightly before the mark: late-committed records can carry slightly older timestamps,
    and re-reading a few records is harmless because loads are idempotent."""
    return iso(parse_ts(high_water) - timedelta(seconds=seconds))
