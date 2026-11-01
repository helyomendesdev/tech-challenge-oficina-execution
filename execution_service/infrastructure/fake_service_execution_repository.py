from __future__ import annotations

from copy import deepcopy
from threading import RLock

from execution_service.application.dto import (
    OutboxMessage,
    ServiceCommandReceipt,
)
from execution_service.application.ports import ServiceExecutionRepositoryPort
from execution_service.domain.exceptions import IdempotencyKeyReuseError
from execution_service.domain.service_execution import ExecutionLifecycle, ServiceStatus


class InMemoryServiceExecutionRepository(ServiceExecutionRepositoryPort):
    """Atomic in-process adapter for application tests and local development only."""

    def __init__(self) -> None:
        self._lifecycles: dict[str, tuple[ExecutionLifecycle, int]] = {}
        self._receipts: dict[str, ServiceCommandReceipt] = {}
        self._lock = RLock()

    def add_lifecycle(self, lifecycle: ExecutionLifecycle) -> None:
        with self._lock:
            if lifecycle.saga_id in self._lifecycles:
                raise RuntimeError(f"Lifecycle already exists for sagaId={lifecycle.saga_id}")
            self._lifecycles[lifecycle.saga_id] = (lifecycle.clone(), 0)

    def load_lifecycle(self, saga_id: str) -> tuple[ExecutionLifecycle, int] | None:
        with self._lock:
            stored = self._lifecycles.get(saga_id)
            if stored is None:
                return None
            lifecycle, version = stored
            return lifecycle.clone(), version

    def get_service_command_receipt(self, message_id: str) -> ServiceCommandReceipt | None:
        with self._lock:
            receipt = self._receipts.get(message_id)
            return deepcopy(receipt) if receipt is not None else None

    def commit_service_command(
        self,
        lifecycle: ExecutionLifecycle,
        message_id: str,
        fingerprint: str,
        status: ServiceStatus,
        outbox_message: OutboxMessage | None,
        expected_version: int,
    ) -> bool:
        with self._lock:
            previous = self._receipts.get(message_id)
            if previous is not None:
                if previous.fingerprint != fingerprint:
                    raise IdempotencyKeyReuseError(
                        f"messageId={message_id} was reused with a different body"
                    )
                return False

            stored = self._lifecycles.get(lifecycle.saga_id)
            if stored is None or stored[1] != expected_version:
                raise RuntimeError("Concurrent service execution state change; retry the command")

            persisted_outbox = (
                deepcopy(outbox_message)
                if outbox_message is None
                else OutboxMessage(
                    message=deepcopy(outbox_message.message),
                    message_attributes=deepcopy(outbox_message.message_attributes),
                    published=outbox_message.published,
                    input_message_id=message_id,
                )
            )
            receipt = ServiceCommandReceipt(
                fingerprint=fingerprint,
                status=status,
                outbox_message=persisted_outbox,
            )

            self._lifecycles[lifecycle.saga_id] = (lifecycle.clone(), expected_version + 1)
            self._receipts[message_id] = receipt
            return True
