from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any, Callable
from uuid import uuid4

from execution_service.application.dto import (
    OutboxMessage,
    PersistedServiceStartDecision,
    ServiceCommandReceipt,
    ServiceExecutionCommandContext,
    ServiceExecutionResult,
)
from execution_service.application.ports import (
    PersistedServiceStartDecisionPort,
    ServiceExecutionRepositoryPort,
)
from execution_service.domain.enums import SagaMessageType
from execution_service.domain.exceptions import (
    IdempotencyKeyReuseError,
    MessageValidationError,
    TransitionNotAllowedError,
)
from execution_service.domain.service_execution import (
    ExecutionCompleted,
    ExecutionLifecycle,
    PartConsumption,
    ServiceStatus,
)


class _ServiceExecutionUseCase:
    def __init__(self, repository: ServiceExecutionRepositoryPort) -> None:
        self._repository = repository

    def _execute(
        self,
        context: ServiceExecutionCommandContext,
        operation: str,
        inputs: dict[str, Any],
        change: Callable[[ExecutionLifecycle], ExecutionCompleted | None],
    ) -> ServiceExecutionResult:
        fingerprint = _fingerprint(
            {
                "operation": operation,
                "sagaId": context.saga_id,
                "executionId": context.execution_id,
                "osId": context.os_id,
                "requestId": context.request_id,
                "inputs": inputs,
            }
        )
        previous = self._repository.get_service_command_receipt(context.message_id)
        if previous is not None:
            return self._duplicate_result(context.message_id, fingerprint, previous)

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
        status = _resulting_status(lifecycle, inputs)
        outbox = _completion_outbox(context, event) if event is not None else None
        committed = self._repository.commit_service_command(
            lifecycle=lifecycle,
            message_id=context.message_id,
            fingerprint=fingerprint,
            status=status,
            outbox_message=outbox,
            expected_version=version,
        )
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
        parts: Sequence[PartConsumption],
    ) -> ServiceExecutionResult:
        if isinstance(parts, (str, bytes)) or not isinstance(parts, Sequence):
            raise MessageValidationError("parts must be a sequence of PartConsumption values")
        requested_parts = tuple(parts)
        if any(not isinstance(part, PartConsumption) for part in requested_parts):
            raise MessageValidationError("parts must contain PartConsumption values")

        def change(lifecycle: ExecutionLifecycle) -> None:
            lifecycle.service(service_id).request_service_start(
                context.message_id,
                started_at,
                requested_parts,
            )
            return None

        return self._execute(
            context,
            "START_SERVICE",
            {
                "serviceId": service_id,
                "startedAt": _datetime_input(started_at),
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


class _ResolveServiceStartUseCase(_ServiceExecutionUseCase):
    _outcome: str

    def __init__(
        self,
        repository: ServiceExecutionRepositoryPort,
        decision_port: PersistedServiceStartDecisionPort,
    ) -> None:
        super().__init__(repository)
        self._decision_port = decision_port

    def execute(
        self,
        context: ServiceExecutionCommandContext,
        service_id: int,
        start_message_id: str,
    ) -> ServiceExecutionResult:
        operation = f"{self._outcome}_SERVICE_START"
        inputs = {
            "serviceId": service_id,
            "startMessageId": start_message_id,
            "decisionMessageId": context.message_id,
        }
        fingerprint = _fingerprint(
            {
                "operation": operation,
                "sagaId": context.saga_id,
                "executionId": context.execution_id,
                "osId": context.os_id,
                "requestId": context.request_id,
                "inputs": inputs,
            }
        )
        previous = self._repository.get_service_command_receipt(context.message_id)
        if previous is not None:
            return self._duplicate_result(context.message_id, fingerprint, previous)

        decision = self._decision_port.get_persisted_decision(context.message_id)
        self._validate_decision(
            decision,
            context=context,
            service_id=service_id,
            start_message_id=start_message_id,
        )

        def change(lifecycle: ExecutionLifecycle) -> None:
            service = lifecycle.service(service_id)
            if self._outcome == "ACCEPTED":
                service.confirm_start_and_consumption(
                    start_message_id,
                    decision.decision_message_id,
                )
            else:
                service.reject_start_after_os_confirmation(
                    start_message_id,
                    decision.decision_message_id,
                )
            return None

        return self._execute(
            context,
            operation,
            inputs,
            change,
        )

    def _validate_decision(
        self,
        decision: PersistedServiceStartDecision | None,
        *,
        context: ServiceExecutionCommandContext,
        service_id: int,
        start_message_id: str,
    ) -> None:
        if decision is None:
            raise TransitionNotAllowedError(
                "Service start cannot be resolved without a persisted OS/Saga decision"
            )
        if (
            decision.saga_id != context.saga_id
            or decision.os_id != context.os_id
            or decision.service_id != service_id
            or decision.start_message_id != start_message_id
            or decision.decision_message_id != context.message_id
            or decision.outcome != self._outcome
        ):
            raise MessageValidationError(
                "Persisted OS/Saga decision does not match the service start command"
            )


class ConfirmServiceStartUseCase(_ResolveServiceStartUseCase):
    _outcome = "ACCEPTED"


class RejectServiceStartUseCase(_ResolveServiceStartUseCase):
    _outcome = "REJECTED"


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


def _fingerprint(command: dict[str, Any]) -> str:
    encoded = json.dumps(command, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _datetime_input(value: datetime) -> str:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise MessageValidationError("service timestamps must be timezone-aware datetimes")
    return value.isoformat()


def _resulting_status(lifecycle: ExecutionLifecycle, inputs: dict[str, Any]) -> ServiceStatus:
    service_id = inputs.get("serviceId")
    return lifecycle.service(service_id).status


def _completion_outbox(
    context: ServiceExecutionCommandContext,
    event: ExecutionCompleted,
) -> OutboxMessage:
    return OutboxMessage(
        message={
            "messageId": str(uuid4()),
            "type": SagaMessageType.EXECUCAO_FINALIZADA.value,
            "version": 1,
            "sagaId": event.saga_id,
            "osId": event.os_id,
            "causationId": context.message_id,
            "requestId": context.request_id,
            "occurredAt": datetime.now(timezone.utc).isoformat(),
            "payload": {
                "execucaoId": event.execution_id,
                "finalizadaEm": event.completed_at.isoformat(),
            },
        },
        message_attributes={},
    )
