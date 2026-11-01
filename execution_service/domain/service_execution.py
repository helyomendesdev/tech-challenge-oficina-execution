from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from types import MappingProxyType
from typing import Mapping

from .exceptions import IdempotencyKeyReuseError, MessageValidationError, TransitionNotAllowedError

_UUID = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-"
    r"[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)


class ServiceStatus(str, Enum):
    PENDENTE = "PENDENTE"
    INICIO_PENDENTE = "INICIO_PENDENTE"
    EM_EXECUCAO = "EM_EXECUCAO"
    CONCLUIDO = "CONCLUIDO"


class DomainEventType(str, Enum):
    EXECUTION_COMPLETED = "EXECUTION_COMPLETED"


@dataclass(frozen=True)
class PartConsumption:
    item_peca_os_id: int
    peca_id: int
    quantity: int

    def __post_init__(self) -> None:
        _positive_integer(self.item_peca_os_id, "item_peca_os_id")
        _positive_integer(self.peca_id, "peca_id")
        _positive_integer(self.quantity, "quantity")


@dataclass(frozen=True)
class ExecutionCompleted:
    os_id: int
    saga_id: str
    execution_id: str
    completed_at: datetime
    type: DomainEventType = DomainEventType.EXECUTION_COMPLETED

    def __post_init__(self) -> None:
        _positive_integer(self.os_id, "os_id")
        _uuid(self.saga_id, "saga_id")
        _uuid(self.execution_id, "execution_id")
        _aware_datetime(self.completed_at, "completed_at")
        if self.type is not DomainEventType.EXECUTION_COMPLETED:
            raise MessageValidationError("type must be EXECUTION_COMPLETED")


class ServiceExecution:
    """Service lifecycle; external decisions must be persisted before resolution."""

    __slots__ = (
        "_service_id",
        "_status",
        "_started_at",
        "_completed_at",
        "_parts_used",
        "_pending_started_at",
        "_pending_parts",
        "_start_message_id",
        "_start_fingerprint",
        "_start_resolution_message_id",
        "_start_resolution",
        "_finish_message_id",
        "_finish_at",
    )

    def __init__(self, service_id: int) -> None:
        _positive_integer(service_id, "service_id")
        self._service_id = service_id
        self._status = ServiceStatus.PENDENTE
        self._started_at: datetime | None = None
        self._completed_at: datetime | None = None
        self._parts_used: tuple[PartConsumption, ...] = ()
        self._pending_started_at: datetime | None = None
        self._pending_parts: tuple[PartConsumption, ...] = ()
        self._start_message_id: str | None = None
        self._start_fingerprint: tuple[datetime, tuple[PartConsumption, ...]] | None = None
        self._start_resolution_message_id: str | None = None
        self._start_resolution: str | None = None
        self._finish_message_id: str | None = None
        self._finish_at: datetime | None = None

    @property
    def service_id(self) -> int:
        return self._service_id

    @property
    def status(self) -> ServiceStatus:
        return self._status

    @property
    def started_at(self) -> datetime | None:
        return self._started_at

    @property
    def completed_at(self) -> datetime | None:
        return self._completed_at

    @property
    def parts_used(self) -> tuple[PartConsumption, ...]:
        return self._parts_used

    @property
    def pending_started_at(self) -> datetime | None:
        return self._pending_started_at

    @property
    def pending_parts(self) -> tuple[PartConsumption, ...]:
        return self._pending_parts

    @property
    def start_message_id(self) -> str | None:
        return self._start_message_id

    @property
    def start_resolution_message_id(self) -> str | None:
        return self._start_resolution_message_id

    @property
    def start_resolution(self) -> str | None:
        return self._start_resolution

    @property
    def finish_message_id(self) -> str | None:
        return self._finish_message_id

    @property
    def finish_at(self) -> datetime | None:
        return self._finish_at

    def request_service_start(
        self,
        message_id: str,
        started_at: datetime,
        parts: Sequence[PartConsumption],
    ) -> bool:
        _uuid(message_id, "message_id")
        _aware_datetime(started_at, "started_at")
        if isinstance(parts, (str, bytes)) or not isinstance(parts, Sequence):
            raise MessageValidationError("parts must be a sequence of PartConsumption values")
        requested_parts = tuple(parts)
        if any(not isinstance(part, PartConsumption) for part in requested_parts):
            raise MessageValidationError("parts must contain PartConsumption values")

        fingerprint = (started_at, requested_parts)
        if message_id == self._start_message_id:
            if fingerprint != self._start_fingerprint:
                raise IdempotencyKeyReuseError(
                    f"messageId={message_id} was reused with a different start request"
                )
            return False

        if self._status is not ServiceStatus.PENDENTE:
            raise TransitionNotAllowedError(
                f"Cannot request service start while status is {self._status.value}"
            )

        self._status = ServiceStatus.INICIO_PENDENTE
        self._start_message_id = message_id
        self._start_fingerprint = fingerprint
        self._pending_started_at = started_at
        self._pending_parts = requested_parts
        self._start_resolution_message_id = None
        self._start_resolution = None
        return True

    def confirm_start_and_consumption(
        self,
        message_id: str,
        decision_message_id: str,
    ) -> bool:
        """Apply a persisted OS/Saga acceptance of both service start and part consumption.

        This method does not contact or verify the external owner. Its caller must
        invoke it only after the correlated decision has been durably accepted.
        """
        return self._resolve_start(message_id, decision_message_id, "ACCEPTED")

    def reject_start_after_os_confirmation(
        self,
        message_id: str,
        decision_message_id: str,
    ) -> bool:
        """Return to PENDENTE only after an explicit, persisted OS/Saga rejection.

        Timeouts and unknown outcomes must not call this method. Durable replay
        detection for prior attempts belongs to the repository.
        """
        return self._resolve_start(message_id, decision_message_id, "REJECTED")

    def _resolve_start(
        self,
        message_id: str,
        decision_message_id: str,
        outcome: str,
    ) -> bool:
        _uuid(message_id, "message_id")
        _uuid(decision_message_id, "decision_message_id")

        if message_id != self._start_message_id:
            raise TransitionNotAllowedError("Start resolution does not match the pending request")

        if self._start_resolution is not None:
            if (
                self._start_resolution == outcome
                and self._start_resolution_message_id == decision_message_id
            ):
                return False
            raise TransitionNotAllowedError("The pending start already has a different resolution")

        if self._status is not ServiceStatus.INICIO_PENDENTE:
            raise TransitionNotAllowedError(
                f"Cannot resolve service start while status is {self._status.value}"
            )

        self._start_resolution_message_id = decision_message_id
        self._start_resolution = outcome
        if outcome == "ACCEPTED":
            self._status = ServiceStatus.EM_EXECUCAO
            self._started_at = self._pending_started_at
            self._parts_used = self._pending_parts
            self._pending_started_at = None
            self._pending_parts = ()
        else:
            self._status = ServiceStatus.PENDENTE
            self._pending_started_at = None
            self._pending_parts = ()
        return True

    def finish_service(self, message_id: str, completed_at: datetime) -> bool:
        _uuid(message_id, "message_id")
        _aware_datetime(completed_at, "completed_at")

        if message_id == self._finish_message_id:
            if completed_at != self._finish_at:
                raise IdempotencyKeyReuseError(
                    f"messageId={message_id} was reused with a different finish request"
                )
            return False

        if self._status is not ServiceStatus.EM_EXECUCAO:
            raise TransitionNotAllowedError(
                f"Cannot finish service while status is {self._status.value}"
            )
        if self._started_at is None:
            raise TransitionNotAllowedError("Cannot finish a service without a start date")
        if completed_at < self._started_at:
            raise MessageValidationError("completed_at cannot be earlier than started_at")

        self._status = ServiceStatus.CONCLUIDO
        self._completed_at = completed_at
        self._finish_message_id = message_id
        self._finish_at = completed_at
        return True

    def clone(self) -> ServiceExecution:
        clone = ServiceExecution(self._service_id)
        for attribute in self.__slots__:
            setattr(clone, attribute, getattr(self, attribute))
        return clone


class ExecutionLifecycle:
    __slots__ = ("_os_id", "_saga_id", "_execution_id", "_services", "_completion_event")

    def __init__(
        self,
        os_id: int,
        saga_id: str,
        execution_id: str,
        service_ids: Sequence[int],
    ) -> None:
        _positive_integer(os_id, "os_id")
        _uuid(saga_id, "saga_id")
        _uuid(execution_id, "execution_id")
        if isinstance(service_ids, (str, bytes)) or not isinstance(service_ids, Sequence):
            raise MessageValidationError("service_ids must be a sequence of positive integers")
        service_ids = tuple(service_ids)
        if not service_ids:
            raise MessageValidationError("service_ids must contain at least one service")
        for service_id in service_ids:
            _positive_integer(service_id, "service_id")
        if len(set(service_ids)) != len(service_ids):
            raise MessageValidationError("service_ids must be unique within an OS")

        self._os_id = os_id
        self._saga_id = saga_id
        self._execution_id = execution_id
        services = {service_id: ServiceExecution(service_id) for service_id in service_ids}
        self._services: Mapping[int, ServiceExecution] = MappingProxyType(services)
        self._completion_event: ExecutionCompleted | None = None

    @property
    def os_id(self) -> int:
        return self._os_id

    @property
    def saga_id(self) -> str:
        return self._saga_id

    @property
    def execution_id(self) -> str:
        return self._execution_id

    @property
    def services(self) -> Mapping[int, ServiceExecution]:
        return self._services

    @property
    def completion_event(self) -> ExecutionCompleted | None:
        """In-memory domain event; durable uniqueness requires repository + Outbox."""
        return self._completion_event

    def service(self, service_id: int) -> ServiceExecution:
        _positive_integer(service_id, "service_id")
        try:
            return self._services[service_id]
        except KeyError as exc:
            raise MessageValidationError(
                f"service_id={service_id} does not belong to os_id={self._os_id}"
            ) from exc

    def clone(self) -> ExecutionLifecycle:
        clone = ExecutionLifecycle(
            os_id=self._os_id,
            saga_id=self._saga_id,
            execution_id=self._execution_id,
            service_ids=tuple(self._services),
        )
        clone._services = MappingProxyType(
            {service_id: service.clone() for service_id, service in self._services.items()}
        )
        clone._completion_event = self._completion_event
        return clone

    def finish_service(
        self,
        service_id: int,
        message_id: str,
        completed_at: datetime,
    ) -> ExecutionCompleted | None:
        service = self.service(service_id)
        if not service.finish_service(message_id, completed_at):
            return None

        if self._completion_event is None and all(
            item.status is ServiceStatus.CONCLUIDO for item in self._services.values()
        ):
            self._completion_event = ExecutionCompleted(
                os_id=self._os_id,
                saga_id=self._saga_id,
                execution_id=self._execution_id,
                completed_at=completed_at,
            )
            return self._completion_event
        return None


def _positive_integer(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise MessageValidationError(f"{name} must be a positive integer")


def _uuid(value: str, name: str) -> None:
    if not isinstance(value, str) or not _UUID.fullmatch(value):
        raise MessageValidationError(f"{name} must be a lowercase RFC 4122 UUID")


def _aware_datetime(value: datetime, name: str) -> None:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise MessageValidationError(f"{name} must be a timezone-aware datetime")
