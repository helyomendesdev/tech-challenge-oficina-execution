from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
from threading import RLock

from execution_service.application.dto import OutboxMessage, ServiceCommandReceipt
from execution_service.application.ports import ExecutionRepositoryPort
from execution_service.domain.enums import ExecutionStatus
from execution_service.domain.exceptions import (
    ConcurrentExecutionUpdateError,
    IdempotencyKeyReuseError,
    TransitionNotAllowedError,
)
from execution_service.domain.models import ExecutionRecord
from execution_service.domain.service_execution import ExecutionLifecycle, ServiceStatus


class InMemoryExecutionRepository(ExecutionRepositoryPort):
    """Atomic in-process repository for tests and explicitly local development."""

    def __init__(self):
        self.records: dict[str, ExecutionRecord] = {}
        self.fingerprints: dict[str, str] = {}
        self.outbox: dict[str, OutboxMessage] = {}
        self._lifecycles: dict[str, tuple[ExecutionLifecycle, int]] = {}
        self._service_receipts: dict[str, ServiceCommandReceipt] = {}
        self._lock = RLock()

    def get_by_saga_id(self, saga_id: str) -> ExecutionRecord | None:
        with self._lock:
            record = self.records.get(saga_id)
            return deepcopy(record) if record is not None else None

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
        lifecycle: ExecutionLifecycle | None = None,
        expected_lifecycle_version: int | None = None,
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
                raise ConcurrentExecutionUpdateError(
                    "Concurrent execution state change; retry the command"
                )

            old_lifecycle = self._lifecycles.get(record.saga_id)
            if lifecycle is not None:
                if expected_lifecycle_version is None:
                    if old_lifecycle is not None:
                        raise ConcurrentExecutionUpdateError(
                            "Execution lifecycle already exists; retry the command"
                        )
                elif old_lifecycle is None or old_lifecycle[1] != expected_lifecycle_version:
                    raise ConcurrentExecutionUpdateError(
                        "Concurrent service execution state change; retry the command"
                    )
            elif expected_lifecycle_version is not None:
                raise ValueError("expected_lifecycle_version requires a lifecycle")

            self.fingerprints[input_message_id] = input_fingerprint
            self.records[record.saga_id] = deepcopy(record)
            self.outbox[input_message_id] = replace(
                deepcopy(response),
                input_message_id=input_message_id,
            )
            if lifecycle is not None:
                version = (
                    0 if expected_lifecycle_version is None else expected_lifecycle_version + 1
                )
                self._lifecycles[record.saga_id] = (lifecycle.clone(), version)
            return True

    def register_failure_response(
        self,
        input_message_id: str,
        input_fingerprint: str,
        response: OutboxMessage,
    ) -> bool:
        with self._lock:
            previous = self.fingerprints.get(input_message_id)
            if previous is not None:
                if previous != input_fingerprint:
                    raise IdempotencyKeyReuseError(
                        f"messageId={input_message_id} was reused with a different body"
                    )
                return False

            saga_id = response.message["sagaId"]
            if saga_id in self.records:
                raise ConcurrentExecutionUpdateError(
                    "Execution has already been accepted for this saga"
                )

            self.fingerprints[input_message_id] = input_fingerprint
            self.outbox[input_message_id] = replace(
                deepcopy(response),
                input_message_id=input_message_id,
            )
            return True

    def get_outbox(self, input_message_id: str) -> OutboxMessage | None:
        with self._lock:
            message = self.outbox.get(input_message_id)
            return deepcopy(message) if message is not None else None

    def get_pending_outbox(self, limit: int = 10) -> list[OutboxMessage]:
        with self._lock:
            return [deepcopy(message) for message in self.outbox.values() if not message.published][
                :limit
            ]

    def mark_outbox_published(self, input_message_id: str) -> None:
        with self._lock:
            message = self.outbox.get(input_message_id)
            if message is None:
                raise RuntimeError(f"Outbox record missing for messageId={input_message_id}")
            self.outbox[input_message_id] = replace(message, published=True)

    def load_lifecycle(self, saga_id: str) -> tuple[ExecutionLifecycle, int] | None:
        with self._lock:
            stored = self._lifecycles.get(saga_id)
            if stored is None:
                return None
            lifecycle, version = stored
            return lifecycle.clone(), version

    def get_service_command_receipt(self, message_id: str) -> ServiceCommandReceipt | None:
        with self._lock:
            receipt = self._service_receipts.get(message_id)
            return deepcopy(receipt) if receipt is not None else None

    def commit_service_command(
        self,
        lifecycle: ExecutionLifecycle,
        message_id: str,
        fingerprint: str,
        status: ServiceStatus,
        outbox_message: OutboxMessage | None,
        expected_version: int,
        execution_status: ExecutionStatus | None = None,
    ) -> bool:
        with self._lock:
            previous = self._service_receipts.get(message_id)
            if previous is not None:
                if previous.fingerprint != fingerprint:
                    raise IdempotencyKeyReuseError(
                        f"messageId={message_id} was reused with a different body"
                    )
                return False

            stored = self._lifecycles.get(lifecycle.saga_id)
            record = self.records.get(lifecycle.saga_id)
            if stored is None or stored[1] != expected_version:
                raise ConcurrentExecutionUpdateError(
                    "Concurrent service execution state change; retry the command"
                )
            if record is None or record.status is not ExecutionStatus.EM_EXECUCAO:
                raise TransitionNotAllowedError("Execution is no longer accepting service changes")

            persisted_outbox = (
                None
                if outbox_message is None
                else replace(
                    deepcopy(outbox_message),
                    input_message_id=message_id,
                )
            )
            self._lifecycles[lifecycle.saga_id] = (lifecycle.clone(), expected_version + 1)
            self._service_receipts[message_id] = ServiceCommandReceipt(
                fingerprint=fingerprint,
                status=status,
                outbox_message=persisted_outbox,
            )
            if persisted_outbox is not None:
                record.status = execution_status or ExecutionStatus.CONCLUIDA
                record.updated_at = datetime.now(timezone.utc)
                record.version += 1
                self.outbox[message_id] = persisted_outbox
            return True
