from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class JobOut(BaseModel):
    id: str
    service: str
    object_name: Optional[str] = None
    status: str
    cursor: Optional[str] = None
    row_count: int
    retry_count: int
    error: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class AuditLogOut(BaseModel):
    event: str
    detail: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True
