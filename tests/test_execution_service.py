from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, call
from uuid import uuid4

import boto3
import pytest

from execution_service.app import create_aws_clients
from execution_service.application.use_cases import (
    ExecuteCommandUseCase,
    ExecutionMessageValidator,
)
from execution_service.domain.enums import ExecutionStatus
from execution_service.domain.exceptions import (
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


def test_aws_client_factory_uses_boto3_session_and_configured_region(monkeypatch):
    session = Mock()
    session.client.side_effect = ["sqs-client", "dynamodb-client"]
    session_factory = Mock(return_value=session)
    monkeypatch.setattr("execution_service.app.boto3.Session", session_factory)

    clients = create_aws_clients("us-west-2")

    assert clients == ("sqs-client", "dynamodb-client")
    session_factory.assert_called_once_with(region_name="us-west-2")
    session.client.assert_has_calls([call("sqs"), call("dynamodb")])


def test_boto3_default_chain_accepts_aws_academy_session_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "temporary-access-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "temporary-secret-key")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "temporary-session-token")

    session = boto3.Session(region_name="us-west-2")
    credentials = session.get_credentials()

    assert credentials is not None
    assert credentials.method == "env"
    frozen_credentials = credentials.get_frozen_credentials()
    assert frozen_credentials.access_key == "temporary-access-key"
    assert frozen_credentials.secret_key == "temporary-secret-key"
    assert frozen_credentials.token == "temporary-session-token"


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


def test_failure_before_execution_start_matches_v1_contract_and_is_idempotent():
    repository = InMemoryExecutionRepository()
    use_case = ExecuteCommandUseCase(repository)
    command = start_message()

    failed = use_case.fail_execution_start(
        command,
        "PECA_SEM_ESTOQUE",
        "Peca 40 sem saldo para reserva.",
        TRACE_ATTRIBUTES,
    )
    outbox = repository.get_outbox(command["messageId"])

    assert failed.status == ExecutionStatus.FALHOU.value
    assert failed.message_type == "ExecucaoFalhou"
    assert failed.execution_id is None
    assert repository.get_by_saga_id(command["sagaId"]) is None
    assert outbox is not None
    assert set(outbox.message) == {
        "messageId",
        "type",
        "version",
        "sagaId",
        "osId",
        "causationId",
        "requestId",
        "occurredAt",
        "payload",
    }
    assert outbox.message["type"] == "ExecucaoFalhou"
    assert outbox.message["version"] == 1
    assert outbox.message["sagaId"] == command["sagaId"]
    assert outbox.message["osId"] == command["osId"]
    assert outbox.message["causationId"] == command["messageId"]
    assert outbox.message["requestId"] == command["requestId"]
    assert outbox.message["payload"] == {
        "orcamentoId": command["payload"]["orcamentoId"],
        "execucaoId": None,
        "erro": {
            "codigo": "PECA_SEM_ESTOQUE",
            "mensagem": "Peca 40 sem saldo para reserva.",
        },
    }
    assert "traceparent" not in outbox.message
    assert "traceparent" in outbox.message_attributes

    replay = use_case.fail_execution_start(
        command,
        "OUTRO_CODIGO",
        "A resposta persistida deve prevalecer.",
        TRACE_ATTRIBUTES,
    )
    handled_redelivery = use_case.handle(command, TRACE_ATTRIBUTES)
    assert replay.message_id == outbox.message["messageId"]
    assert handled_redelivery.message_id == outbox.message["messageId"]
    assert handled_redelivery.message_type == "ExecucaoFalhou"
    assert len(repository.outbox) == 1


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


def test_cancel_command_is_persisted_and_acknowledged_with_contract_response(
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

    result = use_case.handle(cancellation)

    saved = repository.get_by_saga_id(start["sagaId"])

    assert result.message_type == "ExecucaoCancelada"
    assert saved.status == ExecutionStatus.CANCELADA
    assert started.execution_id == saved.execution_id

    assert repository.get_outbox(start["messageId"]) is not None
    pending = repository.get_outbox(cancellation["messageId"])
    assert pending.message["type"] == "ExecucaoCancelada"
    assert pending.message["payload"] == {"execucaoId": saved.execution_id}


def test_cancel_command_is_supported_after_execution_failure(processor_factory):
    repository = InMemoryExecutionRepository()
    processor = processor_factory(repository)
    start = start_message()
    started = processor.handle(start)
    repository.records[start["sagaId"]].status = ExecutionStatus.FALHOU
    cancellation = cancel_message(start, started.execution_id)
    cancellation["payload"]["motivo"] = "EXECUCAO_FALHOU"

    result = processor.handle(cancellation)

    assert result.message_type == "ExecucaoCancelada"
    assert repository.get_by_saga_id(start["sagaId"]).status is ExecutionStatus.CANCELADA


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
        {
            "payload": {
                "orcamentoId": str(uuid4()),
                "servicos": [{"servicoId": 1, "quantidade": 1}],
                "pecas": [
                    {"itemPecaOSId": 0, "pecaId": 9, "quantidade": 1},
                ],
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
