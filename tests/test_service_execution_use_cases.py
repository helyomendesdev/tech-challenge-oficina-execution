from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from execution_service.application.dto import (
    PersistedServiceStartDecision,
    ServiceExecutionCommandContext,
)
from execution_service.application.service_execution_use_cases import (
    ConfirmServiceStartUseCase,
    FinishServiceUseCase,
    RejectServiceStartUseCase,
    StartServiceUseCase,
)
from execution_service.domain.exceptions import (
    IdempotencyKeyReuseError,
    MessageValidationError,
    TransitionNotAllowedError,
)
from execution_service.domain.service_execution import (
    ExecutionLifecycle,
    PartConsumption,
    ServiceStatus,
)
from execution_service.infrastructure.fake_service_execution_repository import (
    InMemoryServiceExecutionRepository,
)

OS_ID = 42
SERVICE_IDS = (101, 102)
STARTED_AT = datetime(2026, 1, 1, 10, tzinfo=timezone.utc)


class PersistedDecisionFake:
    def __init__(self) -> None:
        self.decisions: dict[str, PersistedServiceStartDecision] = {}
        self.calls = 0
        self.unavailable = False

    def get_persisted_decision(self, decision_message_id: str):
        self.calls += 1
        if self.unavailable:
            raise RuntimeError("decision store unavailable")
        return self.decisions.get(decision_message_id)

    def add(
        self,
        context: ServiceExecutionCommandContext,
        service_id: int,
        start_message_id: str,
        outcome: str,
    ) -> None:
        self.decisions[context.message_id] = PersistedServiceStartDecision(
            saga_id=context.saga_id,
            os_id=context.os_id,
            service_id=service_id,
            start_message_id=start_message_id,
            decision_message_id=context.message_id,
            outcome=outcome,
        )


@pytest.fixture
def lifecycle():
    return ExecutionLifecycle(
        os_id=OS_ID,
        saga_id=str(uuid4()),
        execution_id=str(uuid4()),
        service_ids=SERVICE_IDS,
    )


@pytest.fixture
def repository(lifecycle):
    repo = InMemoryServiceExecutionRepository()
    repo.add_lifecycle(lifecycle)
    return repo


def context(lifecycle, message_id: str | None = None):
    return ServiceExecutionCommandContext(
        saga_id=lifecycle.saga_id,
        execution_id=lifecycle.execution_id,
        os_id=lifecycle.os_id,
        message_id=message_id or str(uuid4()),
        request_id="request-1",
    )


def start_service(repository, lifecycle, service_id, message_id=None):
    command = context(lifecycle, message_id)
    result = StartServiceUseCase(repository).execute(
        context=command,
        service_id=service_id,
        started_at=STARTED_AT,
        parts=(PartConsumption(item_peca_os_id=1, peca_id=10, quantity=2),),
    )
    return command, result


def resolve_service(
    repository,
    decisions,
    lifecycle,
    service_id,
    start_message_id,
    outcome,
    message_id=None,
):
    command = context(lifecycle, message_id)
    decisions.add(command, service_id, start_message_id, outcome)
    use_case = (
        ConfirmServiceStartUseCase(repository, decisions)
        if outcome == "ACCEPTED"
        else RejectServiceStartUseCase(repository, decisions)
    )
    return command, use_case.execute(command, service_id, start_message_id)


def test_start_service_persists_pending_state_and_part_consumption(repository, lifecycle):
    command, result = start_service(repository, lifecycle, SERVICE_IDS[0])

    assert result.status is ServiceStatus.INICIO_PENDENTE
    assert result.outbox_message is None
    persisted, version = repository.load_lifecycle(lifecycle.saga_id)
    service = persisted.service(SERVICE_IDS[0])
    assert service.pending_started_at == STARTED_AT
    assert service.pending_parts == (PartConsumption(1, 10, 2),)
    assert version == 1
    assert repository.get_service_command_receipt(command.message_id) is not None


def test_start_service_is_idempotent_and_rejects_key_reuse(repository, lifecycle):
    command, _ = start_service(repository, lifecycle, SERVICE_IDS[0])
    duplicate = StartServiceUseCase(repository).execute(
        command,
        SERVICE_IDS[0],
        STARTED_AT,
        (PartConsumption(1, 10, 2),),
    )

    assert duplicate.duplicate
    assert repository.load_lifecycle(lifecycle.saga_id)[1] == 1

    with pytest.raises(IdempotencyKeyReuseError):
        StartServiceUseCase(repository).execute(
            command,
            SERVICE_IDS[0],
            STARTED_AT,
            (PartConsumption(1, 10, 3),),
        )


def test_service_start_rejects_non_pending_transition(repository, lifecycle):
    command, _ = start_service(repository, lifecycle, SERVICE_IDS[0])
    decisions = PersistedDecisionFake()
    _, _ = resolve_service(
        repository,
        decisions,
        lifecycle,
        SERVICE_IDS[0],
        command.message_id,
        "ACCEPTED",
    )

    with pytest.raises(TransitionNotAllowedError):
        StartServiceUseCase(repository).execute(
            context(lifecycle),
            SERVICE_IDS[0],
            STARTED_AT,
            (PartConsumption(item_peca_os_id=1, peca_id=10, quantity=1),),
        )


def test_start_service_rejects_invalid_identifiers_and_quantities(repository, lifecycle):
    with pytest.raises(MessageValidationError):
        invalid_context = ServiceExecutionCommandContext(
            saga_id=lifecycle.saga_id,
            execution_id=lifecycle.execution_id,
            os_id=OS_ID,
            message_id="not-a-uuid",
            request_id="request-1",
        )
        StartServiceUseCase(repository).execute(
            invalid_context,
            SERVICE_IDS[0],
            STARTED_AT,
            (),
        )

    with pytest.raises(MessageValidationError):
        StartServiceUseCase(repository).execute(
            context(lifecycle),
            True,
            STARTED_AT,
            (),
        )

    with pytest.raises(MessageValidationError):
        PartConsumption(item_peca_os_id=1, peca_id=10, quantity=0)

    with pytest.raises(MessageValidationError, match="timezone-aware"):
        StartServiceUseCase(repository).execute(
            context(lifecycle),
            SERVICE_IDS[0],
            datetime(2026, 1, 1),
            (),
        )


def test_confirm_requires_correlated_persisted_decision(repository, lifecycle):
    start_context, _ = start_service(repository, lifecycle, SERVICE_IDS[0])
    decision_context = context(lifecycle)
    decisions = PersistedDecisionFake()
    use_case = ConfirmServiceStartUseCase(repository, decisions)

    with pytest.raises(TransitionNotAllowedError, match="persisted"):
        use_case.execute(decision_context, SERVICE_IDS[0], start_context.message_id)
    persisted, version = repository.load_lifecycle(lifecycle.saga_id)
    assert persisted.service(SERVICE_IDS[0]).status is ServiceStatus.INICIO_PENDENTE
    assert repository.get_service_command_receipt(decision_context.message_id) is None
    assert version == 1

    decisions.add(
        decision_context,
        SERVICE_IDS[1],
        start_context.message_id,
        "ACCEPTED",
    )
    with pytest.raises(MessageValidationError, match="does not match"):
        use_case.execute(decision_context, SERVICE_IDS[0], start_context.message_id)


def test_confirm_redelivery_uses_receipt_when_decision_store_is_unavailable(
    repository,
    lifecycle,
):
    start_context, _ = start_service(repository, lifecycle, SERVICE_IDS[0])
    decisions = PersistedDecisionFake()
    decision_context, first_result = resolve_service(
        repository,
        decisions,
        lifecycle,
        SERVICE_IDS[0],
        start_context.message_id,
        "ACCEPTED",
    )
    calls_after_first_delivery = decisions.calls
    decisions.unavailable = True

    replay = ConfirmServiceStartUseCase(repository, decisions).execute(
        decision_context,
        SERVICE_IDS[0],
        start_context.message_id,
    )

    assert replay.duplicate
    assert replay.status is first_result.status
    assert replay.outbox_message == first_result.outbox_message
    assert decisions.calls == calls_after_first_delivery


def test_reject_redelivery_uses_receipt_when_decision_store_is_unavailable(
    repository,
    lifecycle,
):
    start_context, _ = start_service(repository, lifecycle, SERVICE_IDS[0])
    decisions = PersistedDecisionFake()
    decision_context, first_result = resolve_service(
        repository,
        decisions,
        lifecycle,
        SERVICE_IDS[0],
        start_context.message_id,
        "REJECTED",
    )
    calls_after_first_delivery = decisions.calls
    decisions.unavailable = True

    replay = RejectServiceStartUseCase(repository, decisions).execute(
        decision_context,
        SERVICE_IDS[0],
        start_context.message_id,
    )

    assert replay.duplicate
    assert replay.status is first_result.status
    assert replay.outbox_message == first_result.outbox_message
    assert decisions.calls == calls_after_first_delivery


def test_resolution_key_reuse_with_different_content_is_rejected_before_decision_lookup(
    repository,
    lifecycle,
):
    start_context, _ = start_service(repository, lifecycle, SERVICE_IDS[0])
    decisions = PersistedDecisionFake()
    decision_context, _ = resolve_service(
        repository,
        decisions,
        lifecycle,
        SERVICE_IDS[0],
        start_context.message_id,
        "ACCEPTED",
    )
    calls_after_first_delivery = decisions.calls

    with pytest.raises(IdempotencyKeyReuseError):
        ConfirmServiceStartUseCase(repository, decisions).execute(
            decision_context,
            SERVICE_IDS[1],
            start_context.message_id,
        )

    assert decisions.calls == calls_after_first_delivery


def test_confirm_records_parts_only_after_persisted_acceptance(repository, lifecycle):
    start_context, _ = start_service(repository, lifecycle, SERVICE_IDS[0])
    decisions = PersistedDecisionFake()
    decision_context, result = resolve_service(
        repository,
        decisions,
        lifecycle,
        SERVICE_IDS[0],
        start_context.message_id,
        "ACCEPTED",
    )

    persisted, _ = repository.load_lifecycle(lifecycle.saga_id)
    service = persisted.service(SERVICE_IDS[0])
    assert result.status is ServiceStatus.EM_EXECUCAO
    assert service.started_at == STARTED_AT
    assert service.parts_used == (PartConsumption(1, 10, 2),)
    assert decision_context.message_id == service.start_resolution_message_id


def test_rejection_is_explicit_and_old_redelivery_stays_idempotent(repository, lifecycle):
    start_context, _ = start_service(repository, lifecycle, SERVICE_IDS[0])
    decisions = PersistedDecisionFake()
    reject_context, rejected = resolve_service(
        repository,
        decisions,
        lifecycle,
        SERVICE_IDS[0],
        start_context.message_id,
        "REJECTED",
    )
    assert rejected.status is ServiceStatus.PENDENTE

    new_start_context, retried = start_service(
        repository,
        lifecycle,
        SERVICE_IDS[0],
    )
    assert new_start_context.message_id != start_context.message_id
    assert retried.status is ServiceStatus.INICIO_PENDENTE

    replayed = RejectServiceStartUseCase(repository, decisions).execute(
        reject_context,
        SERVICE_IDS[0],
        start_context.message_id,
    )
    assert replayed.duplicate
    assert (
        repository.load_lifecycle(lifecycle.saga_id)[0].service(SERVICE_IDS[0]).start_message_id
        == new_start_context.message_id
    )


def test_finish_last_service_persists_execucao_finalizada_once(repository, lifecycle):
    decisions = PersistedDecisionFake()
    for service_id in SERVICE_IDS:
        start_context, _ = start_service(repository, lifecycle, service_id)
        resolve_service(
            repository,
            decisions,
            lifecycle,
            service_id,
            start_context.message_id,
            "ACCEPTED",
        )

    first_finish = context(lifecycle)
    first_result = FinishServiceUseCase(repository).execute(
        first_finish,
        SERVICE_IDS[0],
        STARTED_AT + timedelta(hours=1),
    )
    assert first_result.status is ServiceStatus.CONCLUIDO
    assert first_result.outbox_message is None

    final_context = context(lifecycle)
    final_result = FinishServiceUseCase(repository).execute(
        final_context,
        SERVICE_IDS[1],
        STARTED_AT + timedelta(hours=2),
    )
    assert final_result.status is ServiceStatus.CONCLUIDO
    outbox = final_result.outbox_message
    assert outbox is not None
    assert outbox.message["type"] == "ExecucaoFinalizada"
    assert outbox.message["payload"] == {
        "execucaoId": lifecycle.execution_id,
        "finalizadaEm": (STARTED_AT + timedelta(hours=2)).isoformat(),
    }
    assert outbox.message["osId"] == lifecycle.os_id
    assert outbox.message["sagaId"] == lifecycle.saga_id
    assert outbox.input_message_id == final_context.message_id
    assert repository.get_service_command_receipt(final_context.message_id).outbox_message == outbox

    replay = FinishServiceUseCase(repository).execute(
        final_context,
        SERVICE_IDS[1],
        STARTED_AT + timedelta(hours=2),
    )
    assert replay.duplicate
    assert replay.outbox_message.message["messageId"] == outbox.message["messageId"]
    assert repository.load_lifecycle(lifecycle.saga_id)[1] == 6


def test_finish_rejects_invalid_date_and_non_running_service(repository, lifecycle):
    with pytest.raises(TransitionNotAllowedError):
        FinishServiceUseCase(repository).execute(
            context(lifecycle),
            SERVICE_IDS[0],
            STARTED_AT,
        )

    start_context, _ = start_service(repository, lifecycle, SERVICE_IDS[0])
    decisions = PersistedDecisionFake()
    resolve_service(
        repository,
        decisions,
        lifecycle,
        SERVICE_IDS[0],
        start_context.message_id,
        "ACCEPTED",
    )
    with pytest.raises(MessageValidationError, match="earlier"):
        FinishServiceUseCase(repository).execute(
            context(lifecycle),
            SERVICE_IDS[0],
            STARTED_AT - timedelta(seconds=1),
        )


def test_service_command_persistence_failure_does_not_change_stored_lifecycle(
    repository,
    lifecycle,
):
    class FailedCommitRepository(InMemoryServiceExecutionRepository):
        def load_lifecycle(self, saga_id):
            return repository.load_lifecycle(saga_id)

        def get_service_command_receipt(self, message_id):
            return repository.get_service_command_receipt(message_id)

        def commit_service_command(self, *args, **kwargs):
            raise RuntimeError("persistence failed")

    with pytest.raises(RuntimeError, match="persistence failed"):
        StartServiceUseCase(FailedCommitRepository()).execute(
            context(lifecycle),
            SERVICE_IDS[0],
            STARTED_AT,
            (),
        )

    persisted, version = repository.load_lifecycle(lifecycle.saga_id)
    assert persisted.service(SERVICE_IDS[0]).status is ServiceStatus.PENDENTE
    assert version == 0


def test_service_command_rejects_unknown_service(repository, lifecycle):
    with pytest.raises(MessageValidationError, match="does not belong"):
        StartServiceUseCase(repository).execute(
            context(lifecycle),
            999,
            STARTED_AT,
            (),
        )
