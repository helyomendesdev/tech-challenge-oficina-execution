from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from execution_service.application.dto import (
    OutboxMessage,
    PersistedServiceStartDecision,
    ServiceCommandReceipt,
)
from execution_service.domain.models import ExecutionRecord
from execution_service.domain.service_execution import ExecutionLifecycle, ServiceStatus


class ExecutionRepositoryPort(ABC):
    @abstractmethod
    def get_by_saga_id(self, saga_id: str) -> ExecutionRecord | None:
        raise NotImplementedError

    @abstractmethod
    def get_input_fingerprint(self, message_id: str) -> str | None:
        raise NotImplementedError

    @abstractmethod
    def register_command(
        self,
        record: ExecutionRecord,
        input_message_id: str,
        input_fingerprint: str,
        response: OutboxMessage,
        expected_version: int | None,
    ) -> bool:
        raise NotImplementedError

    @abstractmethod
    def get_outbox(self, input_message_id: str) -> OutboxMessage | None:
        raise NotImplementedError

    @abstractmethod
    def get_pending_outbox(self, limit: int = 10) -> list[OutboxMessage]:
        raise NotImplementedError

    @abstractmethod
    def mark_outbox_published(self, input_message_id: str) -> None:
        raise NotImplementedError


class ServiceExecutionRepositoryPort(ABC):
    """Atomic persistence boundary for service state, idempotency, and optional outbox."""

    @abstractmethod
    def load_lifecycle(self, saga_id: str) -> tuple[ExecutionLifecycle, int] | None:
        """Return a detached aggregate copy and its optimistic concurrency version."""
        raise NotImplementedError

    @abstractmethod
    def get_service_command_receipt(self, message_id: str) -> ServiceCommandReceipt | None:
        """Load the durable result before reapplying a redelivered command."""
        raise NotImplementedError

    @abstractmethod
    def commit_service_command(
        self,
        lifecycle: ExecutionLifecycle,
        message_id: str,
        fingerprint: str,
        status: ServiceStatus,
        outbox_message: OutboxMessage | None,
        expected_version: int,
    ) -> bool:
        """Atomically persist lifecycle, command receipt, and outbox when present."""
        raise NotImplementedError


class PersistedServiceStartDecisionPort(ABC):
    @abstractmethod
    def get_persisted_decision(
        self,
        decision_message_id: str,
    ) -> PersistedServiceStartDecision | None:
        """Return only a durably accepted OS/Saga decision, or None if it is unavailable."""
        raise NotImplementedError


class MessagePublisherPort(ABC):
    @abstractmethod
    def publish(
        self,
        message: dict[str, Any],
        *,
        message_group_id: str,
        message_deduplication_id: str,
        message_attributes: dict[str, dict[str, str]],
    ) -> None:
        raise NotImplementedError
