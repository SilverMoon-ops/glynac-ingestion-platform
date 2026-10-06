from __future__ import annotations

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict, computed_field


class JobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    service: str
    object_name: Optional[str] = None
    org_id: Optional[str] = None
    status: str
    cursor: Optional[str] = None
    row_count: int
    retry_count: int
    error: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    @computed_field
    def duration_seconds(self) -> float:
        if self.updated_at and self.created_at:
            delta = (self.updated_at - self.created_at).total_seconds()
            return max(0.0, round(delta, 2))
        return 0.0


class AuditLogOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    event: str
    detail: Optional[str] = None
    created_at: datetime
