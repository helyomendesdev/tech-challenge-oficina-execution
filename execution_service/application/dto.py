from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class ExecutionEnvelope:
    message_id: str
    type: str
    version: int
    saga_id: str
    os_id: int
    causation_id: str | None
    request_id: str
    occurred_at: datetime
    payload: dict[str, Any] = field(default_factory=dict)
    traceparent: str | None = None
    tracestate: str | None = None


@dataclass(frozen=True)
class ExecutionResult:
    status: str
    message_type: str
    execution_id: str
    saga_id: str
    os_id: int
    traceparent: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    message_id: str | None = None


@dataclass(frozen=True)
class OutboxMessage:
    message: dict[str, Any]
    message_attributes: dict[str, dict[str, str]]
    published: bool = False
    input_message_id: str | None = None
