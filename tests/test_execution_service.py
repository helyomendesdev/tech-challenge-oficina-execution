from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest

from execution_service.application.use_cases import ExecutionMessageValidator
from execution_service.domain.enums import ExecutionStatus
from execution_service.domain.exceptions import (
    ExecutionCancellationUnavailableError,
    IdempotencyKeyReuseError,
    MessageValidationError,
    TransitionNotAllowedError,
    UnsupportedMessageError,
)
from execution_service.domain.policies import ExecutionStatusPolicy
from execution_service.infrastructure.fake_repository import (
    InMemoryExecutionRepository,
)
from tests.factories import (
    TRACE_ATTRIBUTES,
    TRACEPARENT,
    cancel_message,
    start_message,
)


def test_execution_policy_rejects_second_start():
    with pytest.raises(TransitionNotAllowedError):
        ExecutionStatusPolicy.validate_start(ExecutionStatus.EM_EXECUCAO)


def test_start_command_persists_execution_and_contract_response(
    processor_factory,
):
    repository = InMemoryExecutionRepository()
    use_case = processor_factory(repository)
    message = start_message()

    result = use_case.handle(message, TRACE_ATTRIBUTES)

    assert result.message_type == "ExecucaoIniciada"
    assert result.status == ExecutionStatus.EM_EXECUCAO.value

    saved = repository.get_by_saga_id(message["sagaId"])
    pending = repository.get_outbox(message["messageId"])

    assert saved is not None
    assert saved.execution_id == result.execution_id
    assert saved.services == message["payload"]["servicos"]

    assert pending is not None
    assert pending.published is False

    envelope = pending.message

    assert envelope["type"] == "ExecucaoIniciada"
    assert envelope["causationId"] == message["messageId"]
    assert envelope["requestId"] == message["requestId"]
    assert envelope["payload"]["orcamentoId"] == message["payload"]["orcamentoId"]

    assert "traceparent" not in envelope

    response_traceparent = pending.message_attributes["traceparent"]["StringValue"]

    assert response_traceparent.startswith(f"00-{TRACEPARENT.split('-')[1]}-")
    assert response_traceparent.split("-")[2] != TRACEPARENT.split("-")[2]

    assert pending.message_attributes["tracestate"]["StringValue"] == "vendor=value"


def test_application_composition_accepts_test_repository(
    execution_repository,
):
    from execution_service.app import create_processor

    use_case = create_processor(execution_repository)
    message = start_message()

    result = use_case.handle(message)

    assert result.message_type == "ExecucaoIniciada"

    saved = execution_repository.get_by_saga_id(message["sagaId"])
    pending = execution_repository.get_outbox(message["messageId"])

    assert saved is not None
    assert pending is not None
    assert pending.published is False


def test_cancel_command_is_not_confirmed_without_stock_compensation_owner(
    processor_factory,
):
    repository = InMemoryExecutionRepository()
    use_case = processor_factory(repository)

    start = start_message()
    started = use_case.handle(start)

    cancellation = cancel_message(
        start,
        started.execution_id,
    )

    with pytest.raises(ExecutionCancellationUnavailableError):
        use_case.handle(cancellation)

    saved = repository.get_by_saga_id(start["sagaId"])

    assert saved.status == ExecutionStatus.EM_EXECUCAO
    assert started.execution_id == saved.execution_id

    assert repository.get_outbox(start["messageId"]) is not None
    assert repository.get_input_fingerprint(cancellation["messageId"]) is None


def test_duplicate_redelivery_reuses_persisted_outbox(
    processor_factory,
):
    repository = InMemoryExecutionRepository()
    use_case = processor_factory(repository)
    message = start_message()

    first = use_case.handle(message)

    restarted_processor = processor_factory(repository)
    second = restarted_processor.handle(message)

    assert first.message_id == second.message_id
    assert first.execution_id == second.execution_id

    pending = repository.get_outbox(message["messageId"])

    assert pending is not None
    assert pending.published is False


def test_message_id_reuse_with_different_body_is_rejected(
    execution_processor,
):
    message = start_message()

    execution_processor.handle(message)

    changed = {
        **message,
        "requestId": "different-request",
    }

    with pytest.raises(IdempotencyKeyReuseError):
        execution_processor.handle(changed)


def test_a_second_start_for_the_same_saga_is_not_accepted(
    processor_factory,
):
    repository = InMemoryExecutionRepository()
    use_case = processor_factory(repository)

    message = start_message()
    use_case.handle(message)

    second_start = start_message(
        sagaId=message["sagaId"],
        osId=message["osId"],
    )

    with pytest.raises(TransitionNotAllowedError):
        use_case.handle(second_start)

    assert repository.get_outbox(message["messageId"]) is not None
    assert repository.get_outbox(second_start["messageId"]) is None


def test_failure_to_persist_does_not_create_idempotency_or_outbox(
    processor_factory,
):
    class FailRepository(InMemoryExecutionRepository):
        def register_command(self, *args, **kwargs):
            raise RuntimeError("DynamoDB unavailable")

    repository = FailRepository()
    message = start_message()

    with pytest.raises(RuntimeError, match="DynamoDB unavailable"):
        processor_factory(repository).handle(message)

    assert repository.get_input_fingerprint(message["messageId"]) is None
    assert repository.get_outbox(message["messageId"]) is None
    assert repository.get_by_saga_id(message["sagaId"]) is None


@pytest.mark.parametrize(
    "overrides",
    [
        {"version": 2},
        {"osId": True},
        {"sagaId": "not-a-uuid"},
        {"occurredAt": "2025-01-01"},
        {"traceparent": TRACEPARENT},
        {"extra": "not allowed"},
        {
            "payload": {
                "orcamentoId": "bad",
                "servicos": [],
                "pecas": [],
            }
        },
        {
            "payload": {
                "orcamentoId": str(uuid4()),
                "servicos": [{"servicoId": 0, "quantidade": 1}],
                "pecas": [],
            }
        },
    ],
)
def test_invalid_contract_messages_are_rejected(overrides):
    message = start_message()
    attrs = TRACE_ATTRIBUTES

    if "traceparent" in overrides:
        message["traceparent"] = overrides["traceparent"]
    else:
        message.update(overrides)

    with pytest.raises(MessageValidationError):
        ExecutionMessageValidator.validate(message, attrs)


def test_non_command_saga_message_is_rejected():
    message = start_message(type="ExecucaoFinalizada")

    with pytest.raises(UnsupportedMessageError):
        ExecutionMessageValidator.validate(message)


def test_cancel_command_schema_and_type_are_validated():
    start = start_message()
    cancellation = cancel_message(
        start,
        str(uuid4()),
    )

    validated = ExecutionMessageValidator.validate(cancellation)

    assert validated.type == "CancelarExecucao"

    with pytest.raises(MessageValidationError, match="motivo"):
        ExecutionMessageValidator.validate(
            {
                **cancellation,
                "payload": {
                    **cancellation["payload"],
                    "motivo": "INVALIDO",
                },
            }
        )


def test_concurrent_duplicate_commands_share_durable_idempotency_and_response_id(
    processor_factory,
):
    repository = InMemoryExecutionRepository()
    use_case = processor_factory(repository)
    message = start_message()

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(
            pool.map(
                lambda _index: use_case.handle(message),
                range(8),
            )
        )

    assert len({result.message_id for result in results}) == 1
    assert len({result.execution_id for result in results}) == 1

    pending = repository.get_outbox(message["messageId"])

    assert pending is not None
    assert pending.published is False

    assert len(repository.records) == 1
    assert len(repository.outbox) == 1
