import json
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.models import AuditLog, DeadLetter


def log_audit(db: Session, job_id: str, event: str, detail: Optional[str] = None) -> None:
    """
    Persisted audit trail. Replaces console.log-only checkpoint messages —
    this survives a restart and is queryable, e.g.:
        SELECT * FROM audit_log WHERE job_id = ? ORDER BY created_at;
    """
    entry = AuditLog(job_id=job_id, event=event, detail=detail)
    db.add(entry)
    db.commit()


def send_to_dead_letter(db: Session, job_id: str, record: Any, reason: str) -> None:
    """Call this after a record exhausts retries, instead of dropping it."""
    dl = DeadLetter(job_id=job_id, record=json.dumps(record, default=str), reason=reason)
    db.add(dl)
    db.commit()
    log_audit(db, job_id, "DEAD_LETTER", detail=reason)
