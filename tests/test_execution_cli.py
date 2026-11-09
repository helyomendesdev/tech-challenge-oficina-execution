from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock
from uuid import uuid4

import pytest

from execution_service import cli
from execution_service.application.service_execution_use_cases import ExecutionWorkUseCase
from execution_service.application.use_cases import ExecuteCommandUseCase
from execution_service.domain.enums import ExecutionStatus
from execution_service.domain.service_execution import PartConsumption
from execution_service.infrastructure.dynamodb_repository import DynamoDBExecutionRepository
from execution_service.infrastructure.fake_repository import InMemoryExecutionRepository
from execution_service.infrastructure.outbox_relay import OutboxRelay
from execution_service.infrastructure.sqs_adapter import FakeMessagePublisher
from tests.factories import TRACE_ATTRIBUTES, start_message

STARTED_AT = datetime(2026, 11, 1, 10, tzinfo=timezone.utc)


@pytest.fixture
def cli_execution():
    repository = InMemoryExecutionRepository()
    command = start_message()
    command["payload"]["servicos"] = [
        {"servicoId": 101, "quantidade": 1},
        {"servicoId": 102, "quantidade": 1},
    ]
    command["payload"]["pecas"] = [{"itemPecaOSId": 501, "pecaId": 10, "quantidade": 5}]
    result = ExecutionWorkUseCase(repository)
    ExecuteCommandUseCase(repository).handle(command, TRACE_ATTRIBUTES)
    return repository, command, result


def _run_cli(arguments, work, capsys):
    exit_code = cli.main(arguments, work_use_case_factory=lambda: work)
    captured = capsys.readouterr()
    response_text = captured.out or captured.err
    return exit_code, json.loads(response_text)


def test_cli_starts_service_and_returns_json(cli_execution, capsys):
    _repository, command, work = cli_execution
    message_id = str(uuid4())

    exit_code, response = _run_cli(
        [
            "start-service",
            "--saga-id",
            command["sagaId"],
            "--service-id",
            "101",
            "--message-id",
            message_id,
        ],
        work,
        capsys,
    )

    assert exit_code == 0
    assert response == {
        "status": "success",
        "operation": "start-service",
        "sagaId": command["sagaId"],
        "serviceId": 101,
        "serviceStatus": "EM_EXECUCAO",
        "messageId": message_id,
        "duplicate": False,
        "executionCompleted": False,
        "outboxMessageType": None,
    }


def test_cli_reuses_message_id_for_idempotent_retry(cli_execution, capsys):
    _repository, command, work = cli_execution
    arguments = [
        "start-service",
        "--saga-id",
        command["sagaId"],
        "--service-id",
        "101",
        "--started-at",
        STARTED_AT.isoformat(),
        "--message-id",
        str(uuid4()),
    ]

    first_exit, first_response = _run_cli(arguments, work, capsys)
    retry_exit, retry_response = _run_cli(arguments, work, capsys)

    assert first_exit == retry_exit == 0
    assert first_response["duplicate"] is False
    assert retry_response["duplicate"] is True
    assert retry_response["messageId"] == first_response["messageId"]


def test_cli_records_multiple_reserved_parts(cli_execution, capsys):
    repository, command, work = cli_execution
    work.start_service(command["sagaId"], 101, started_at=STARTED_AT)

    exit_code, response = _run_cli(
        [
            "record-consumption",
            "--saga-id",
            command["sagaId"],
            "--service-id",
            "101",
            "--part",
            "501:10:2",
            "--part",
            "501:10:1",
        ],
        work,
        capsys,
    )

    assert exit_code == 0
    assert response["status"] == "success"
    assert response["serviceStatus"] == "EM_EXECUCAO"
    assert repository.load_lifecycle(command["sagaId"])[0].service(101).parts_used == (
        PartConsumption(501, 10, 2),
        PartConsumption(501, 10, 1),
    )


def test_cli_finishing_last_service_reports_execution_completion(cli_execution, capsys):
    repository, command, work = cli_execution
    for service_id in (101, 102):
        work.start_service(command["sagaId"], service_id, started_at=STARTED_AT)
    work.finish_service(
        command["sagaId"],
        101,
        completed_at=STARTED_AT + timedelta(hours=1),
    )

    exit_code, response = _run_cli(
        [
            "finish-service",
            "--saga-id",
            command["sagaId"],
            "--service-id",
            "102",
        ],
        work,
        capsys,
    )

    assert exit_code == 0
    assert response["executionCompleted"] is True
    assert response["outboxMessageType"] == "ExecucaoFinalizada"
    assert repository.get_by_saga_id(command["sagaId"]).status is ExecutionStatus.CONCLUIDA


def test_cli_fails_service_and_reports_failure_event(cli_execution, capsys):
    repository, command, work = cli_execution
    work.start_service(command["sagaId"], 101, started_at=STARTED_AT)
    message_id = str(uuid4())
    arguments = [
        "fail-service",
        "--saga-id",
        command["sagaId"],
        "--service-id",
        "101",
        "--error-code",
        "SERVICO_INDISPONIVEL",
        "--error-message",
        "Serviço indisponível.",
        "--message-id",
        message_id,
    ]

    exit_code, response = _run_cli(arguments, work, capsys)
    retry_exit_code, retry_response = _run_cli(arguments, work, capsys)

    assert exit_code == retry_exit_code == 0
    assert response["operation"] == "fail-service"
    assert response["serviceStatus"] == "FALHOU"
    assert response["duplicate"] is False
    assert response["executionCompleted"] is False
    assert response["outboxMessageType"] == "ExecucaoFalhou"
    assert retry_response["duplicate"] is True
    assert retry_response["messageId"] == message_id

    saved = repository.get_by_saga_id(command["sagaId"])
    outbox = repository.get_outbox(message_id)
    assert saved.status is ExecutionStatus.FALHOU
    assert outbox is not None
    assert outbox.message == {
        "messageId": outbox.message["messageId"],
        "type": "ExecucaoFalhou",
        "version": 1,
        "sagaId": command["sagaId"],
        "osId": command["osId"],
        "causationId": command["messageId"],
        "requestId": command["requestId"],
        "occurredAt": outbox.message["occurredAt"],
        "payload": {
            "orcamentoId": command["payload"]["orcamentoId"],
            "execucaoId": saved.execution_id,
            "erro": {
                "codigo": "SERVICO_INDISPONIVEL",
                "mensagem": "Serviço indisponível.",
            },
        },
    }
    assert "traceparent" not in outbox.message
    assert outbox.message_attributes["traceparent"]["DataType"] == "String"
    assert outbox.message_attributes["traceparent"]["StringValue"].startswith(
        f"00-{TRACE_ATTRIBUTES['traceparent']['StringValue'].split('-')[1]}-"
    )
    assert outbox.message_attributes["tracestate"]["StringValue"] == "vendor=value"

    publisher = FakeMessagePublisher()
    assert OutboxRelay(repository, publisher).poll_once() == 2
    published = publisher.messages[-1]
    assert published["message"] == outbox.message
    assert published["message_group_id"] == command["sagaId"]
    assert published["message_deduplication_id"] == outbox.message["messageId"]
    assert published["message_attributes"] == outbox.message_attributes
    assert repository.get_outbox(message_id).published is True


def test_cli_rejects_invalid_arguments_as_json(capsys):
    with pytest.raises(SystemExit) as exit_info:
        cli.main(["start-service", "--saga-id", str(uuid4()), "--service-id", "0"])

    assert exit_info.value.code == 2
    response = json.loads(capsys.readouterr().err)
    assert response["error"]["code"] == "invalid_arguments"


def test_cli_rejects_malformed_consumption_part_as_json(capsys):
    with pytest.raises(SystemExit) as exit_info:
        cli.main(
            [
                "record-consumption",
                "--saga-id",
                str(uuid4()),
                "--service-id",
                "101",
                "--part",
                "501:10",
            ]
        )

    assert exit_info.value.code == 2
    response = json.loads(capsys.readouterr().err)
    assert response["error"]["code"] == "invalid_arguments"


def test_cli_reports_missing_execution(cli_execution, capsys):
    _repository, _command, work = cli_execution

    exit_code, response = _run_cli(
        ["start-service", "--saga-id", str(uuid4()), "--service-id", "101"],
        work,
        capsys,
    )

    assert exit_code == 1
    assert response["error"]["code"] == "validation_error"
    assert "lifecycle not found" in response["error"]["message"]


def test_cli_reports_missing_service(cli_execution, capsys):
    _repository, command, work = cli_execution

    exit_code, response = _run_cli(
        [
            "start-service",
            "--saga-id",
            command["sagaId"],
            "--service-id",
            "999",
        ],
        work,
        capsys,
    )

    assert exit_code == 1
    assert response["error"]["code"] == "validation_error"
    assert "does not belong" in response["error"]["message"]


def test_cli_reports_consumption_exceeding_reservation(cli_execution, capsys):
    _repository, command, work = cli_execution
    work.start_service(command["sagaId"], 101, started_at=STARTED_AT)

    exit_code, response = _run_cli(
        [
            "record-consumption",
            "--saga-id",
            command["sagaId"],
            "--service-id",
            "101",
            "--part",
            "501:10:6",
        ],
        work,
        capsys,
    )

    assert exit_code == 1
    assert response["error"]["code"] == "validation_error"
    assert "exceeds" in response["error"]["message"]


def test_cli_reports_use_case_state_transition_error(cli_execution, capsys):
    _repository, command, work = cli_execution

    exit_code, response = _run_cli(
        [
            "finish-service",
            "--saga-id",
            command["sagaId"],
            "--service-id",
            "101",
        ],
        work,
        capsys,
    )

    assert exit_code == 1
    assert response["error"]["code"] == "validation_error"
    assert response["error"]["type"] == "TransitionNotAllowedError"


def test_cli_production_composition_uses_dynamodb_repository(monkeypatch):
    monkeypatch.setenv("EXECUTION_DYNAMODB_TABLE", "execution-table")
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    session = Mock()
    client = object()
    session.client.return_value = client
    session_factory = Mock(return_value=session)
    monkeypatch.setattr(cli.boto3, "Session", session_factory)

    work = cli.create_work_use_case()

    session_factory.assert_called_once_with(region_name="us-west-2")
    session.client.assert_called_once_with("dynamodb")
    assert isinstance(work, ExecutionWorkUseCase)
    assert isinstance(work._repository, DynamoDBExecutionRepository)
    assert work._repository.table_name == "execution-table"
    assert work._repository.client is client
