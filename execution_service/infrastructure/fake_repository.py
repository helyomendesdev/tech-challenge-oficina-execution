from __future__ import annotations

from dataclasses import replace
from threading import RLock

from execution_service.application.dto import OutboxMessage
from execution_service.application.ports import ExecutionRepositoryPort
from execution_service.domain.exceptions import IdempotencyKeyReuseError
from execution_service.domain.models import ExecutionRecord


class InMemoryExecutionRepository(ExecutionRepositoryPort):
    """Atomic in-process repository for tests and explicitly local development."""

    def __init__(self):
        self.records: dict[str, ExecutionRecord] = {}
        self.fingerprints: dict[str, str] = {}
        self.outbox: dict[str, OutboxMessage] = {}
        self._lock = RLock()

    def get_by_saga_id(self, saga_id: str) -> ExecutionRecord | None:
        with self._lock:
            record = self.records.get(saga_id)
            return replace(record) if record is not None else None

    def get_input_fingerprint(self, message_id: str) -> str | None:
        with self._lock:
            return self.fingerprints.get(message_id)

    def register_command(
        self,
        record: ExecutionRecord,
        input_message_id: str,
        input_fingerprint: str,
        response: OutboxMessage,
        expected_version: int | None,
    ) -> bool:
        with self._lock:
            old_fingerprint = self.fingerprints.get(input_message_id)
            if old_fingerprint is not None:
                if old_fingerprint != input_fingerprint:
                    raise IdempotencyKeyReuseError(
                        f"messageId={input_message_id} was reused with a different body"
                    )
                return False

            old_record = self.records.get(record.saga_id)
            if expected_version is None:
                if old_record is not None:
                    raise RuntimeError("Concurrent execution state change; retry the command")
            elif old_record is None or old_record.version != expected_version:
                raise RuntimeError("Concurrent execution state change; retry the command")

            self.fingerprints[input_message_id] = input_fingerprint
            self.records[record.saga_id] = replace(record)
            self.outbox[input_message_id] = response
            return True

    def get_outbox(self, input_message_id: str) -> OutboxMessage | None:
        with self._lock:
            message = self.outbox.get(input_message_id)
            return (
                replace(message, input_message_id=input_message_id) if message is not None else None
            )

    def get_pending_outbox(self, limit: int = 10) -> list[OutboxMessage]:
        with self._lock:
            return [
                replace(message, input_message_id=input_message_id)
                for input_message_id, message in self.outbox.items()
                if not message.published
            ][:limit]

    def mark_outbox_published(self, input_message_id: str) -> None:
        with self._lock:
            message = self.outbox.get(input_message_id)
            if message is None:
                raise RuntimeError(f"Outbox record missing for messageId={input_message_id}")
            self.outbox[input_message_id] = replace(message, published=True)
