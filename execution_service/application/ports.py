from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from execution_service.application.dto import OutboxMessage
from execution_service.domain.models import ExecutionRecord


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
