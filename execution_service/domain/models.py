from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

from .enums import ExecutionStatus


@dataclass
class ExecutionRecord:
    execution_id: str
    saga_id: str
    os_id: int
    budget_id: str
    services: list[dict[str, int]]
    parts: list[dict[str, int]]
    status: ExecutionStatus
    started_at: datetime
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    version: int = 1
