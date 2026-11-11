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
_ERROR_CODE = re.compile(r"^[A-Z][A-Z0-9_]*$")


class ServiceStatus(str, Enum):
    PENDENTE = "PENDENTE"
    EM_EXECUCAO = "EM_EXECUCAO"
    CONCLUIDO = "CONCLUIDO"
    FALHOU = "FALHOU"
    CANCELADO = "CANCELADO"


class DomainEventType(str, Enum):
    EXECUTION_COMPLETED = "EXECUTION_COMPLETED"


@dataclass(frozen=True)
class PartConsumption:
    item_peca_os_id: int | None
    peca_id: int
    quantity: int

    def __post_init__(self) -> None:
        if self.item_peca_os_id is not None:
            _positive_integer(self.item_peca_os_id, "item_peca_os_id")
        _positive_integer(self.peca_id, "peca_id")
        _positive_integer(self.quantity, "quantity")


@dataclass(frozen=True)
class ExecutionCompleted:
    os_id: int
    saga_id: str
    execution_id: str
    completed_at: datetime
    parts_consumed: tuple[PartConsumption, ...] | None = None
    type: DomainEventType = DomainEventType.EXECUTION_COMPLETED

    def __post_init__(self) -> None:
        _positive_integer(self.os_id, "os_id")
        _uuid(self.saga_id, "saga_id")
        _uuid(self.execution_id, "execution_id")
        _aware_datetime(self.completed_at, "completed_at")
        if self.type is not DomainEventType.EXECUTION_COMPLETED:
            raise MessageValidationError("type must be EXECUTION_COMPLETED")


@dataclass(frozen=True)
class ExecutionFailed:
    os_id: int
    saga_id: str
    budget_id: str
    execution_id: str | None
    service_id: int
    error_code: str
    error_message: str

    def __post_init__(self) -> None:
        _positive_integer(self.os_id, "os_id")
        _uuid(self.saga_id, "saga_id")
        _uuid(self.budget_id, "budget_id")
        if self.execution_id is not None:
            _uuid(self.execution_id, "execution_id")
        _positive_integer(self.service_id, "service_id")
        validate_failure_details(self.error_code, self.error_message)


class ServiceExecution:
    """Internal service lifecycle and consumption record."""

    __slots__ = (
        "_service_id",
        "_status",
        "_started_at",
        "_completed_at",
        "_parts_used",
        "_start_message_id",
        "_start_fingerprint",
        "_consumption_commands",
        "_finish_message_id",
        "_finish_at",
        "_failure_message_id",
        "_failure_code",
        "_failure_message",
        "_cancel_message_id",
    )

    def __init__(self, service_id: int) -> None:
        _positive_integer(service_id, "service_id")
        self._service_id = service_id
        self._status = ServiceStatus.PENDENTE
        self._started_at: datetime | None = None
        self._completed_at: datetime | None = None
        self._parts_used: tuple[PartConsumption, ...] = ()
        self._start_message_id: str | None = None
        self._start_fingerprint: datetime | None = None
        self._consumption_commands: dict[str, tuple[PartConsumption, ...]] = {}
        self._finish_message_id: str | None = None
        self._finish_at: datetime | None = None
        self._failure_message_id: str | None = None
        self._failure_code: str | None = None
        self._failure_message: str | None = None
        self._cancel_message_id: str | None = None

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
    def start_message_id(self) -> str | None:
        return self._start_message_id

    @property
    def finish_message_id(self) -> str | None:
        return self._finish_message_id

    @property
    def finish_at(self) -> datetime | None:
        return self._finish_at

    @property
    def failure_code(self) -> str | None:
        return self._failure_code

    @property
    def failure_message(self) -> str | None:
        return self._failure_message

    def has_consumption_command(self, message_id: str) -> bool:
        _uuid(message_id, "message_id")
        return message_id in self._consumption_commands

    def request_service_start(
        self,
        message_id: str,
        started_at: datetime,
    ) -> bool:
        _uuid(message_id, "message_id")
        _aware_datetime(started_at, "started_at")

        fingerprint = started_at
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

        self._status = ServiceStatus.EM_EXECUCAO
        self._start_message_id = message_id
        self._start_fingerprint = fingerprint
        self._started_at = started_at
        return True

    def record_consumption(
        self,
        message_id: str,
        parts: Sequence[PartConsumption],
    ) -> bool:
        _uuid(message_id, "message_id")
        if isinstance(parts, (str, bytes)) or not isinstance(parts, Sequence):
            raise MessageValidationError("parts must be a sequence of PartConsumption values")
        requested_parts = tuple(parts)
        if any(not isinstance(part, PartConsumption) for part in requested_parts):
            raise MessageValidationError("parts must contain PartConsumption values")
        previous = self._consumption_commands.get(message_id)
        if previous is not None:
            if previous != requested_parts:
                raise IdempotencyKeyReuseError(
                    f"messageId={message_id} was reused with different consumption"
                )
            return False
        if self._status is not ServiceStatus.EM_EXECUCAO:
            raise TransitionNotAllowedError(
                f"Cannot record consumption while status is {self._status.value}"
            )
        self._consumption_commands[message_id] = requested_parts
        self._parts_used += requested_parts
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

    def fail_service(self, message_id: str, error_code: str, error_message: str) -> bool:
        _uuid(message_id, "message_id")
        validate_failure_details(error_code, error_message)

        if message_id == self._failure_message_id:
            if (error_code, error_message) != (self._failure_code, self._failure_message):
                raise IdempotencyKeyReuseError(
                    f"messageId={message_id} was reused with a different failure request"
                )
            return False
        if self._status not in {ServiceStatus.PENDENTE, ServiceStatus.EM_EXECUCAO}:
            raise TransitionNotAllowedError(
                f"Cannot fail service while status is {self._status.value}"
            )

        self._status = ServiceStatus.FALHOU
        self._failure_message_id = message_id
        self._failure_code = error_code
        self._failure_message = error_message
        return True

    def cancel(self, message_id: str) -> bool:
        _uuid(message_id, "message_id")
        if message_id == self._cancel_message_id:
            return False
        if self._status in {ServiceStatus.CONCLUIDO, ServiceStatus.FALHOU}:
            return False
        if self._status is ServiceStatus.CANCELADO:
            return False
        self._status = ServiceStatus.CANCELADO
        self._cancel_message_id = message_id
        return True

    def to_state(self) -> dict[str, object]:
        return {
            "serviceId": self._service_id,
            "status": self._status.value,
            "startedAt": self._started_at.isoformat() if self._started_at else None,
            "completedAt": self._completed_at.isoformat() if self._completed_at else None,
            "partsUsed": [
                {
                    "itemPecaOSId": part.item_peca_os_id,
                    "pecaId": part.peca_id,
                    "quantidade": part.quantity,
                }
                for part in self._parts_used
            ],
            "startMessageId": self._start_message_id,
            "consumptionCommands": [
                {
                    "messageId": message_id,
                    "parts": [
                        {
                            "itemPecaOSId": part.item_peca_os_id,
                            "pecaId": part.peca_id,
                            "quantidade": part.quantity,
                        }
                        for part in parts
                    ],
                }
                for message_id, parts in self._consumption_commands.items()
            ],
            "finishMessageId": self._finish_message_id,
            "cancelMessageId": self._cancel_message_id,
            "failureMessageId": self._failure_message_id,
            "failureCode": self._failure_code,
            "failureMessage": self._failure_message,
        }

    @classmethod
    def from_state(cls, state: dict[str, object]) -> ServiceExecution:
        service = cls(_state_integer(state["serviceId"], "serviceId"))
        service._status = ServiceStatus(str(state["status"]))
        service._started_at = _state_datetime(state.get("startedAt"))
        service._completed_at = _state_datetime(state.get("completedAt"))
        service._parts_used = tuple(
            PartConsumption(
                item_peca_os_id=(
                    _state_integer(part["itemPecaOSId"], "itemPecaOSId")
                    if part.get("itemPecaOSId") is not None
                    else None
                ),
                peca_id=_state_integer(part["pecaId"], "pecaId"),
                quantity=_state_integer(part["quantidade"], "quantidade"),
            )
            for part in state.get("partsUsed", [])
        )
        service._start_message_id = _state_optional_string(state.get("startMessageId"))
        service._consumption_commands = {
            str(command["messageId"]): tuple(
                PartConsumption(
                    item_peca_os_id=(
                        _state_integer(part["itemPecaOSId"], "itemPecaOSId")
                        if part.get("itemPecaOSId") is not None
                        else None
                    ),
                    peca_id=_state_integer(part["pecaId"], "pecaId"),
                    quantity=_state_integer(part["quantidade"], "quantidade"),
                )
                for part in command.get("parts", [])
            )
            for command in state.get("consumptionCommands", [])
        }
        service._finish_message_id = _state_optional_string(state.get("finishMessageId"))
        service._cancel_message_id = _state_optional_string(state.get("cancelMessageId"))
        service._failure_message_id = _state_optional_string(state.get("failureMessageId"))
        service._failure_code = _state_optional_string(state.get("failureCode"))
        service._failure_message = _state_optional_string(state.get("failureMessage"))
        if service._started_at is not None:
            service._start_fingerprint = service._started_at
        if service._completed_at is not None:
            service._finish_at = service._completed_at
        return service

    def clone(self) -> ServiceExecution:
        clone = ServiceExecution(self._service_id)
        for attribute in self.__slots__:
            setattr(clone, attribute, getattr(self, attribute))
        return clone


class ExecutionLifecycle:
    __slots__ = (
        "_os_id",
        "_saga_id",
        "_execution_id",
        "_services",
        "_reserved_parts",
        "_budget_id",
        "_start_message_id",
        "_request_id",
        "_completion_event",
        "_failure_event",
        "_traceparent",
        "_tracestate",
        "_cancellation_reason",
    )

    def __init__(
        self,
        os_id: int,
        saga_id: str,
        execution_id: str,
        service_ids: Sequence[int],
        reserved_parts: Sequence[PartConsumption] = (),
        traceparent: str | None = None,
        tracestate: str | None = None,
        budget_id: str | None = None,
        start_message_id: str | None = None,
        request_id: str | None = None,
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
        if isinstance(reserved_parts, (str, bytes)) or not isinstance(reserved_parts, Sequence):
            raise MessageValidationError("reserved_parts must be a sequence")
        reserved_parts = tuple(reserved_parts)
        if any(not isinstance(part, PartConsumption) for part in reserved_parts):
            raise MessageValidationError("reserved_parts must contain PartConsumption values")
        if tracestate is not None and traceparent is None:
            raise MessageValidationError("tracestate requires traceparent")
        if budget_id is not None:
            _uuid(budget_id, "budget_id")
        if start_message_id is not None:
            _uuid(start_message_id, "start_message_id")
        if request_id is not None and (
            not isinstance(request_id, str) or not 1 <= len(request_id) <= 128
        ):
            raise MessageValidationError("request_id must contain between 1 and 128 characters")

        self._os_id = os_id
        self._saga_id = saga_id
        self._execution_id = execution_id
        services = {service_id: ServiceExecution(service_id) for service_id in service_ids}
        self._services: Mapping[int, ServiceExecution] = MappingProxyType(services)
        self._reserved_parts = reserved_parts
        self._budget_id = budget_id
        self._start_message_id = start_message_id
        self._request_id = request_id
        self._completion_event: ExecutionCompleted | None = None
        self._failure_event: ExecutionFailed | None = None
        self._traceparent = traceparent
        self._tracestate = tracestate
        self._cancellation_reason: str | None = None

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

    @property
    def failure_event(self) -> ExecutionFailed | None:
        return self._failure_event

    @property
    def reserved_parts(self) -> tuple[PartConsumption, ...]:
        return self._reserved_parts

    @property
    def budget_id(self) -> str | None:
        return self._budget_id

    @property
    def start_message_id(self) -> str | None:
        return self._start_message_id

    @property
    def request_id(self) -> str | None:
        return self._request_id

    @property
    def traceparent(self) -> str | None:
        return self._traceparent

    @property
    def tracestate(self) -> str | None:
        return self._tracestate

    @property
    def cancellation_reason(self) -> str | None:
        return self._cancellation_reason

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
            reserved_parts=self._reserved_parts,
            traceparent=self._traceparent,
            tracestate=self._tracestate,
            budget_id=self._budget_id,
            start_message_id=self._start_message_id,
            request_id=self._request_id,
        )
        clone._services = MappingProxyType(
            {service_id: service.clone() for service_id, service in self._services.items()}
        )
        clone._completion_event = self._completion_event
        clone._failure_event = self._failure_event
        clone._cancellation_reason = self._cancellation_reason
        return clone

    def start_service(
        self,
        service_id: int,
        message_id: str,
        started_at: datetime,
    ) -> bool:
        self._ensure_not_failed()
        return self.service(service_id).request_service_start(
            message_id,
            started_at,
        )

    def record_service_consumption(
        self,
        service_id: int,
        message_id: str,
        parts: Sequence[PartConsumption],
    ) -> bool:
        self._ensure_not_failed()
        service = self.service(service_id)
        if isinstance(parts, (str, bytes)) or not isinstance(parts, Sequence):
            raise MessageValidationError("parts must be a sequence of PartConsumption values")
        requested_parts = tuple(parts)
        if any(not isinstance(part, PartConsumption) for part in requested_parts):
            raise MessageValidationError("parts must contain PartConsumption values")
        if not service.has_consumption_command(message_id):
            self._validate_reserved_consumption(requested_parts)
        return service.record_consumption(message_id, requested_parts)

    def finish_service(
        self,
        service_id: int,
        message_id: str,
        completed_at: datetime,
    ) -> ExecutionCompleted | None:
        self._ensure_not_failed()
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
                parts_consumed=self._consumed_parts_for_contract(),
            )
            return self._completion_event
        return None

    def fail_service(
        self,
        service_id: int,
        message_id: str,
        error_code: str,
        error_message: str,
    ) -> ExecutionFailed | None:
        service = self.service(service_id)
        if self._failure_event is not None and service_id != self._failure_event.service_id:
            raise TransitionNotAllowedError("Execution has already failed")
        if self._failure_event is None and (
            self._budget_id is None or self._start_message_id is None or self._request_id is None
        ):
            raise MessageValidationError(
                "Execution lifecycle is missing failure event correlation data"
            )
        if not service.fail_service(message_id, error_code, error_message):
            return None

        if self._failure_event is None:
            self._failure_event = ExecutionFailed(
                os_id=self._os_id,
                saga_id=self._saga_id,
                budget_id=self._budget_id,
                execution_id=self._execution_id,
                service_id=service_id,
                error_code=error_code,
                error_message=error_message,
            )
        return self._failure_event

    def cancel(self, message_id: str, reason: str) -> None:
        _uuid(message_id, "message_id")
        for service in self._services.values():
            service.cancel(message_id)
        self._cancellation_reason = reason

    def to_state(self) -> dict[str, object]:
        return {
            "osId": self._os_id,
            "sagaId": self._saga_id,
            "executionId": self._execution_id,
            "services": [service.to_state() for service in self._services.values()],
            "reservedParts": [
                {
                    "itemPecaOSId": part.item_peca_os_id,
                    "pecaId": part.peca_id,
                    "quantidade": part.quantity,
                }
                for part in self._reserved_parts
            ],
            "budgetId": self._budget_id,
            "startMessageId": self._start_message_id,
            "requestId": self._request_id,
            "completedAt": (
                self._completion_event.completed_at.isoformat() if self._completion_event else None
            ),
            "failureEvent": (
                {
                    "serviceId": self._failure_event.service_id,
                    "errorCode": self._failure_event.error_code,
                    "errorMessage": self._failure_event.error_message,
                }
                if self._failure_event
                else None
            ),
            "traceparent": self._traceparent,
            "tracestate": self._tracestate,
            "cancellationReason": self._cancellation_reason,
        }

    @classmethod
    def from_state(cls, state: dict[str, object]) -> ExecutionLifecycle:
        services = state["services"]
        if not isinstance(services, list):
            raise MessageValidationError("Persisted lifecycle services must be a list")
        reserved = state.get("reservedParts", [])
        if not isinstance(reserved, list):
            raise MessageValidationError("Persisted reserved parts must be a list")
        lifecycle = cls(
            os_id=_state_integer(state["osId"], "osId"),
            saga_id=str(state["sagaId"]),
            execution_id=str(state["executionId"]),
            service_ids=[_state_integer(item["serviceId"], "serviceId") for item in services],
            reserved_parts=[
                PartConsumption(
                    item_peca_os_id=(
                        _state_integer(item["itemPecaOSId"], "itemPecaOSId")
                        if item.get("itemPecaOSId") is not None
                        else None
                    ),
                    peca_id=_state_integer(item["pecaId"], "pecaId"),
                    quantity=_state_integer(item["quantidade"], "quantidade"),
                )
                for item in reserved
            ],
            budget_id=_state_optional_string(state.get("budgetId")),
            start_message_id=_state_optional_string(state.get("startMessageId")),
            request_id=_state_optional_string(state.get("requestId")),
            traceparent=_state_optional_string(state.get("traceparent")),
            tracestate=_state_optional_string(state.get("tracestate")),
        )
        service_states = {_state_integer(item["serviceId"], "serviceId"): item for item in services}
        lifecycle._services = MappingProxyType(
            {
                service_id: ServiceExecution.from_state(service_states[service_id])
                for service_id in service_states
            }
        )
        completed_at = _state_datetime(state.get("completedAt"))
        if completed_at is not None:
            lifecycle._completion_event = ExecutionCompleted(
                os_id=lifecycle._os_id,
                saga_id=lifecycle._saga_id,
                execution_id=lifecycle._execution_id,
                completed_at=completed_at,
                parts_consumed=lifecycle._consumed_parts_for_contract(),
            )
        failure_event = state.get("failureEvent")
        if failure_event is not None:
            if not isinstance(failure_event, dict):
                raise MessageValidationError("Persisted failure event must be an object")
            budget_id = lifecycle._budget_id
            if budget_id is None:
                raise MessageValidationError(
                    "Persisted failure event requires the lifecycle budget_id"
                )
            error_message = _state_optional_string(failure_event.get("errorMessage"))
            error_code = _state_optional_string(failure_event.get("errorCode"))
            if error_message is None or error_code is None:
                raise MessageValidationError(
                    "Persisted failure event requires errorCode and errorMessage strings"
                )
            lifecycle._failure_event = ExecutionFailed(
                os_id=lifecycle._os_id,
                saga_id=lifecycle._saga_id,
                budget_id=budget_id,
                execution_id=lifecycle._execution_id,
                service_id=_state_integer(failure_event["serviceId"], "serviceId"),
                error_code=error_code,
                error_message=error_message,
            )
        lifecycle._cancellation_reason = _state_optional_string(state.get("cancellationReason"))
        return lifecycle

    def _ensure_not_failed(self) -> None:
        if self._failure_event is not None:
            raise TransitionNotAllowedError("Execution has already failed")

    def _validate_reserved_consumption(
        self,
        requested_parts: tuple[PartConsumption, ...],
    ) -> None:
        current: dict[tuple[int | None, int], int] = {}
        for service in self._services.values():
            for part in service.parts_used:
                key = (part.item_peca_os_id, part.peca_id)
                current[key] = current.get(key, 0) + part.quantity
        reserved: dict[tuple[int | None, int], int] = {}
        for part in self._reserved_parts:
            key = (part.item_peca_os_id, part.peca_id)
            reserved[key] = reserved.get(key, 0) + part.quantity
        requested: dict[tuple[int | None, int], int] = {}
        for part in requested_parts:
            key = (part.item_peca_os_id, part.peca_id)
            requested[key] = requested.get(key, 0) + part.quantity
        for key, quantity in requested.items():
            if key not in reserved:
                raise MessageValidationError(
                    f"Consumed part {key!r} was not reserved for this execution"
                )
            if current.get(key, 0) + quantity > reserved[key]:
                raise MessageValidationError(
                    f"Consumed quantity for reserved part {key!r} exceeds its quantity"
                )

    def _consumed_parts_for_contract(self) -> tuple[PartConsumption, ...] | None:
        if any(part.item_peca_os_id is None for part in self._reserved_parts):
            return None
        consumed: dict[tuple[int, int], int] = {}
        for service in self._services.values():
            for part in service.parts_used:
                if part.item_peca_os_id is None:
                    return None
                key = (part.item_peca_os_id, part.peca_id)
                consumed[key] = consumed.get(key, 0) + part.quantity
        return tuple(
            PartConsumption(item_id, peca_id, quantity)
            for (item_id, peca_id), quantity in sorted(consumed.items())
        )


def _state_integer(value: object, name: str) -> int:
    if isinstance(value, bool):
        raise MessageValidationError(f"Persisted {name} must be an integer")
    try:
        integer = int(value)  # DynamoDB returns numeric attributes as Decimal.
    except (TypeError, ValueError, OverflowError) as exc:
        raise MessageValidationError(f"Persisted {name} must be an integer") from exc
    if integer != value:
        raise MessageValidationError(f"Persisted {name} must be an integer")
    return integer


def _state_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise MessageValidationError("Persisted timestamps must be strings")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise MessageValidationError("Persisted timestamp is invalid") from exc
    _aware_datetime(parsed, "persisted timestamp")
    return parsed


def _state_optional_string(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise MessageValidationError("Persisted optional value must be a string")
    return value


def _positive_integer(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise MessageValidationError(f"{name} must be a positive integer")


def validate_failure_details(error_code: str, error_message: str) -> None:
    if not isinstance(error_code, str) or not _ERROR_CODE.fullmatch(error_code):
        raise MessageValidationError(
            "error_code must start with an uppercase letter and contain only A-Z, 0-9 or _"
        )
    if not isinstance(error_message, str) or not error_message.strip() or len(error_message) > 500:
        raise MessageValidationError("error_message must contain between 1 and 500 characters")


def _uuid(value: str, name: str) -> None:
    if not isinstance(value, str) or not _UUID.fullmatch(value):
        raise MessageValidationError(f"{name} must be a lowercase RFC 4122 UUID")


def _aware_datetime(value: datetime, name: str) -> None:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise MessageValidationError(f"{name} must be a timezone-aware datetime")
