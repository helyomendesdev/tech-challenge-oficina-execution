from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any, Callable
from uuid import uuid4

from execution_service.application.dto import (
    OutboxMessage,
    ServiceCommandReceipt,
    ServiceExecutionCommandContext,
    ServiceExecutionResult,
)
from execution_service.application.ports import ServiceExecutionRepositoryPort
from execution_service.application.use_cases import _child_traceparent
from execution_service.domain.enums import ExecutionStatus, SagaMessageType
from execution_service.domain.exceptions import (
    ConcurrentExecutionUpdateError,
    IdempotencyKeyReuseError,
    MessageValidationError,
)
from execution_service.domain.service_execution import (
    ExecutionCompleted,
    ExecutionFailed,
    ExecutionLifecycle,
    PartConsumption,
)

_COMMIT_ATTEMPTS = 5


class _ServiceExecutionUseCase:
    def __init__(self, repository: ServiceExecutionRepositoryPort) -> None:
        self._repository = repository

    def _execute(
        self,
        context: ServiceExecutionCommandContext,
        operation: str,
        inputs: dict[str, Any],
        change: Callable[[ExecutionLifecycle], ExecutionCompleted | ExecutionFailed | None],
    ) -> ServiceExecutionResult:
        fingerprint = self._command_fingerprint(context, operation, inputs)
        for _ in range(_COMMIT_ATTEMPTS):
            duplicate = self._existing_command_result(context, fingerprint)
            if duplicate is not None:
                return duplicate
            result = self._commit_attempt(context, fingerprint, inputs, change)
            if result is not None:
                return result

        raise ConcurrentExecutionUpdateError(
            f"Could not commit service command after {_COMMIT_ATTEMPTS} concurrent updates"
        )

    @staticmethod
    def _command_fingerprint(
        context: ServiceExecutionCommandContext,
        operation: str,
        inputs: dict[str, Any],
    ) -> str:
        return _fingerprint(
            {
                "operation": operation,
                "sagaId": context.saga_id,
                "executionId": context.execution_id,
                "osId": context.os_id,
                "requestId": context.request_id,
                "inputs": inputs,
            }
        )

    def _existing_command_result(
        self,
        context: ServiceExecutionCommandContext,
        fingerprint: str,
    ) -> ServiceExecutionResult | None:
        previous = self._repository.get_service_command_receipt(context.message_id)
        if previous is None:
            return None
        return self._duplicate_result(context.message_id, fingerprint, previous)

    def _commit_attempt(
        self,
        context: ServiceExecutionCommandContext,
        fingerprint: str,
        inputs: dict[str, Any],
        change: Callable[[ExecutionLifecycle], ExecutionCompleted | ExecutionFailed | None],
    ) -> ServiceExecutionResult | None:
        loaded = self._repository.load_lifecycle(context.saga_id)
        if loaded is None:
            raise MessageValidationError(
                f"Execution lifecycle not found for sagaId={context.saga_id}"
            )
        lifecycle, version = loaded
        if lifecycle.os_id != context.os_id or lifecycle.execution_id != context.execution_id:
            raise MessageValidationError(
                "Command correlation does not match the execution lifecycle"
            )

        event = change(lifecycle)
        status = lifecycle.service(inputs["serviceId"]).status
        outbox = _service_event_outbox(context, event) if event is not None else None
        execution_status = _execution_status(event)
        try:
            committed = self._repository.commit_service_command(
                lifecycle=lifecycle,
                message_id=context.message_id,
                fingerprint=fingerprint,
                status=status,
                outbox_message=outbox,
                expected_version=version,
                execution_status=execution_status,
            )
        except ConcurrentExecutionUpdateError:
            return None

        receipt = self._repository.get_service_command_receipt(context.message_id)
        if receipt is None:
            raise RuntimeError(
                f"Service command receipt missing after commit for messageId={context.message_id}"
            )
        return ServiceExecutionResult(
            status=receipt.status,
            duplicate=not committed,
            outbox_message=receipt.outbox_message,
        )

    @staticmethod
    def _duplicate_result(
        message_id: str,
        fingerprint: str,
        receipt: ServiceCommandReceipt,
    ) -> ServiceExecutionResult:
        if receipt.fingerprint != fingerprint:
            raise IdempotencyKeyReuseError(
                f"messageId={message_id} was reused with a different body"
            )
        return ServiceExecutionResult(
            status=receipt.status,
            duplicate=True,
            outbox_message=receipt.outbox_message,
        )


class StartServiceUseCase(_ServiceExecutionUseCase):
    def execute(
        self,
        context: ServiceExecutionCommandContext,
        service_id: int,
        started_at: datetime,
    ) -> ServiceExecutionResult:
        def change(lifecycle: ExecutionLifecycle) -> None:
            lifecycle.start_service(
                service_id,
                context.message_id,
                started_at,
            )
            return None

        return self._execute(
            context,
            "START_SERVICE",
            {
                "serviceId": service_id,
                "startedAt": _datetime_input(started_at),
            },
            change,
        )


class RecordServiceConsumptionUseCase(_ServiceExecutionUseCase):
    def execute(
        self,
        context: ServiceExecutionCommandContext,
        service_id: int,
        parts: Sequence[PartConsumption],
    ) -> ServiceExecutionResult:
        if isinstance(parts, (str, bytes)) or not isinstance(parts, Sequence):
            raise MessageValidationError("parts must be a sequence of PartConsumption values")
        requested_parts = tuple(parts)
        if any(not isinstance(part, PartConsumption) for part in requested_parts):
            raise MessageValidationError("parts must contain PartConsumption values")

        def change(lifecycle: ExecutionLifecycle) -> None:
            lifecycle.record_service_consumption(
                service_id,
                context.message_id,
                requested_parts,
            )
            return None

        return self._execute(
            context,
            "RECORD_SERVICE_CONSUMPTION",
            {
                "serviceId": service_id,
                "parts": [
                    {
                        "itemPecaOSId": part.item_peca_os_id,
                        "pecaId": part.peca_id,
                        "quantidade": part.quantity,
                    }
                    for part in requested_parts
                ],
            },
            change,
        )


class FinishServiceUseCase(_ServiceExecutionUseCase):
    def execute(
        self,
        context: ServiceExecutionCommandContext,
        service_id: int,
        completed_at: datetime,
    ) -> ServiceExecutionResult:
        def change(lifecycle: ExecutionLifecycle) -> ExecutionCompleted | None:
            return lifecycle.finish_service(
                service_id,
                context.message_id,
                completed_at,
            )

        return self._execute(
            context,
            "FINISH_SERVICE",
            {
                "serviceId": service_id,
                "completedAt": _datetime_input(completed_at),
            },
            change,
        )


class FailServiceUseCase(_ServiceExecutionUseCase):
    def execute(
        self,
        context: ServiceExecutionCommandContext,
        service_id: int,
        error_code: str,
        error_message: str,
    ) -> ServiceExecutionResult:
        def change(lifecycle: ExecutionLifecycle) -> ExecutionFailed | None:
            return lifecycle.fail_service(
                service_id,
                context.message_id,
                error_code,
                error_message,
            )

        return self._execute(
            context,
            "FAIL_SERVICE",
            {
                "serviceId": service_id,
                "errorCode": error_code,
                "errorMessage": error_message,
            },
            change,
        )


class ExecutionWorkUseCase:
    """Internal application entry point used by execution workers and integration tests."""

    def __init__(self, repository: ServiceExecutionRepositoryPort) -> None:
        self._repository = repository
        self._start = StartServiceUseCase(repository)
        self._record_consumption = RecordServiceConsumptionUseCase(repository)
        self._finish = FinishServiceUseCase(repository)
        self._fail = FailServiceUseCase(repository)

    def start_service(
        self,
        saga_id: str,
        service_id: int,
        *,
        started_at: datetime | None = None,
        request_id: str = "execution-internal",
        message_id: str | None = None,
    ) -> ServiceExecutionResult:
        context = self._context(saga_id, request_id, message_id)
        return self._start.execute(
            context,
            service_id,
            started_at or datetime.now(timezone.utc),
        )

    def record_consumption(
        self,
        saga_id: str,
        service_id: int,
        parts: Sequence[PartConsumption],
        *,
        request_id: str = "execution-internal",
        message_id: str | None = None,
    ) -> ServiceExecutionResult:
        context = self._context(saga_id, request_id, message_id)
        return self._record_consumption.execute(
            context,
            service_id,
            parts,
        )

    def finish_service(
        self,
        saga_id: str,
        service_id: int,
        *,
        completed_at: datetime | None = None,
        request_id: str = "execution-internal",
        message_id: str | None = None,
    ) -> ServiceExecutionResult:
        context = self._context(saga_id, request_id, message_id)
        return self._finish.execute(
            context,
            service_id,
            completed_at or datetime.now(timezone.utc),
        )

    def fail_service(
        self,
        saga_id: str,
        service_id: int,
        error_code: str,
        error_message: str,
        *,
        request_id: str = "execution-internal",
        message_id: str | None = None,
    ) -> ServiceExecutionResult:
        context = self._context(saga_id, request_id, message_id)
        return self._fail.execute(context, service_id, error_code, error_message)

    def _context(
        self,
        saga_id: str,
        request_id: str,
        message_id: str | None,
    ) -> ServiceExecutionCommandContext:
        loaded = self._repository.load_lifecycle(saga_id)
        if loaded is None:
            raise MessageValidationError(f"Execution lifecycle not found for sagaId={saga_id}")
        lifecycle, _ = loaded
        return ServiceExecutionCommandContext(
            saga_id=saga_id,
            execution_id=lifecycle.execution_id,
            os_id=lifecycle.os_id,
            message_id=message_id or str(uuid4()),
            request_id=lifecycle.request_id or request_id,
            causation_id=lifecycle.start_message_id,
            traceparent=lifecycle.traceparent,
            tracestate=lifecycle.tracestate,
        )


def _fingerprint(command: dict[str, Any]) -> str:
    encoded = json.dumps(command, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _execution_status(event: ExecutionCompleted | ExecutionFailed | None) -> ExecutionStatus | None:
    if isinstance(event, ExecutionFailed):
        return ExecutionStatus.FALHOU
    if event is not None:
        return ExecutionStatus.CONCLUIDA
    return None


def _datetime_input(value: datetime) -> str:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise MessageValidationError("service timestamps must be timezone-aware datetimes")
    return value.isoformat()


def _service_event_outbox(
    context: ServiceExecutionCommandContext,
    event: ExecutionCompleted | ExecutionFailed,
) -> OutboxMessage:
    traceparent = _child_traceparent(context.traceparent)
    attributes = {
        "traceparent": {
            "DataType": "String",
            "StringValue": traceparent,
        }
    }
    if context.tracestate:
        attributes["tracestate"] = {
            "DataType": "String",
            "StringValue": context.tracestate,
        }
    if context.causation_id is None:
        raise MessageValidationError(
            "Execution lifecycle has no causation_id for its Saga response"
        )

    if isinstance(event, ExecutionFailed):
        message_type = SagaMessageType.EXECUCAO_FALHOU.value
        payload: dict[str, Any] = {
            "orcamentoId": event.budget_id,
            "execucaoId": event.execution_id,
            "erro": {
                "codigo": event.error_code,
                "mensagem": event.error_message,
            },
        }
    else:
        message_type = SagaMessageType.EXECUCAO_FINALIZADA.value
        payload = {
            "execucaoId": event.execution_id,
            "finalizadaEm": event.completed_at.isoformat(),
        }
        if event.parts_consumed is not None:
            payload["pecasConsumidas"] = [
                {
                    "itemPecaOSId": part.item_peca_os_id,
                    "pecaId": part.peca_id,
                    "quantidade": part.quantity,
                }
                for part in event.parts_consumed
            ]
    return OutboxMessage(
        message={
            "messageId": str(uuid4()),
            "type": message_type,
            "version": 1,
            "sagaId": event.saga_id,
            "osId": event.os_id,
            "causationId": context.causation_id,
            "requestId": context.request_id,
            "occurredAt": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
        },
        message_attributes=attributes,
    )
