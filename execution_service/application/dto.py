from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from execution_service.domain.exceptions import MessageValidationError
from execution_service.domain.service_execution import ServiceStatus

_UUID = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-"
    r"[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)


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
    execution_id: str | None
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


@dataclass(frozen=True)
class ServiceExecutionCommandContext:
    saga_id: str
    execution_id: str
    os_id: int
    message_id: str
    request_id: str
    traceparent: str | None = None
    tracestate: str | None = None
    causation_id: str | None = None

    def __post_init__(self) -> None:
        for field_name in ("saga_id", "execution_id", "message_id"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not _UUID.fullmatch(value):
                raise MessageValidationError(f"{field_name} must be a lowercase RFC 4122 UUID")
        if self.causation_id is not None and (
            not isinstance(self.causation_id, str) or not _UUID.fullmatch(self.causation_id)
        ):
            raise MessageValidationError("causation_id must be a lowercase RFC 4122 UUID")
        if isinstance(self.os_id, bool) or not isinstance(self.os_id, int) or self.os_id < 1:
            raise MessageValidationError("os_id must be a positive integer")
        if not isinstance(self.request_id, str) or not 1 <= len(self.request_id) <= 128:
            raise MessageValidationError("request_id must contain between 1 and 128 characters")


@dataclass(frozen=True)
class ServiceCommandReceipt:
    fingerprint: str
    status: ServiceStatus
    outbox_message: OutboxMessage | None


@dataclass(frozen=True)
class ServiceExecutionResult:
    status: ServiceStatus
    duplicate: bool
    outbox_message: OutboxMessage | None
