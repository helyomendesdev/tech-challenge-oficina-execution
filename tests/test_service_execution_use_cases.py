from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Barrier, Lock
from uuid import uuid4

import pytest

from execution_service.application.service_execution_use_cases import ExecutionWorkUseCase
from execution_service.application.use_cases import ExecuteCommandUseCase
from execution_service.domain.enums import ExecutionStatus
from execution_service.domain.exceptions import (
    IdempotencyKeyReuseError,
    MessageValidationError,
    TransitionNotAllowedError,
)
from execution_service.domain.service_execution import PartConsumption, ServiceStatus
from execution_service.infrastructure.fake_repository import InMemoryExecutionRepository
from tests.factories import TRACE_ATTRIBUTES, start_message

STARTED_AT = datetime(2026, 11, 1, 10, tzinfo=timezone.utc)
RESERVED_PARTS = [
    {"itemPecaOSId": 501, "pecaId": 10, "quantidade": 5},
    {"itemPecaOSId": 502, "pecaId": 11, "quantidade": 2},
]


@pytest.fixture
def execution():
    repository = InMemoryExecutionRepository()
    command = start_message()
    command["payload"]["servicos"] = [
        {"servicoId": 101, "quantidade": 1},
        {"servicoId": 102, "quantidade": 1},
    ]
    command["payload"]["pecas"] = RESERVED_PARTS
    result = ExecuteCommandUseCase(repository).handle(command, TRACE_ATTRIBUTES)
    return repository, command, result, ExecutionWorkUseCase(repository)


def test_internal_start_and_consumption_are_recorded_separately(execution):
    repository, command, _result, work = execution
    message_id = str(uuid4())
    consumed = (PartConsumption(501, 10, 2),)

    result = work.start_service(
        command["sagaId"],
        101,
        started_at=STARTED_AT,
        message_id=message_id,
    )

    lifecycle, version = repository.load_lifecycle(command["sagaId"])
    service = lifecycle.service(101)
    assert result.status is ServiceStatus.EM_EXECUCAO
    assert service.started_at == STARTED_AT
    assert service.parts_used == ()
    assert version == 1
    assert repository.get_service_command_receipt(message_id) is not None

    consumption_message_id = str(uuid4())
    consumed_result = work.record_consumption(
        command["sagaId"],
        101,
        consumed,
        message_id=consumption_message_id,
    )
    lifecycle, version = repository.load_lifecycle(command["sagaId"])
    assert consumed_result.status is ServiceStatus.EM_EXECUCAO
    assert lifecycle.service(101).parts_used == consumed
    assert version == 2
    assert repository.get_service_command_receipt(consumption_message_id) is not None


def test_service_start_is_idempotent_and_validates_reserved_parts(execution):
    repository, command, _result, work = execution
    start_message_id = str(uuid4())
    consumed = (PartConsumption(501, 10, 2),)

    first = work.start_service(
        command["sagaId"],
        101,
        started_at=STARTED_AT,
        message_id=start_message_id,
    )
    replay = work.start_service(
        command["sagaId"],
        101,
        started_at=STARTED_AT,
        message_id=start_message_id,
    )
    assert not first.duplicate
    assert replay.duplicate
    assert repository.load_lifecycle(command["sagaId"])[1] == 1

    with pytest.raises(IdempotencyKeyReuseError):
        work.start_service(
            command["sagaId"],
            101,
            started_at=STARTED_AT + timedelta(seconds=1),
            message_id=start_message_id,
        )

    consumption_message_id = str(uuid4())
    first_consumption = work.record_consumption(
        command["sagaId"],
        101,
        consumed,
        message_id=consumption_message_id,
    )
    replay_consumption = work.record_consumption(
        command["sagaId"],
        101,
        consumed,
        message_id=consumption_message_id,
    )
    assert not first_consumption.duplicate
    assert replay_consumption.duplicate
    assert repository.load_lifecycle(command["sagaId"])[0].service(101).parts_used == consumed
    work.start_service(command["sagaId"], 102, started_at=STARTED_AT)

    with pytest.raises(IdempotencyKeyReuseError):
        work.record_consumption(
            command["sagaId"],
            101,
            (PartConsumption(501, 10, 1),),
            message_id=consumption_message_id,
        )

    with pytest.raises(MessageValidationError, match="not reserved"):
        work.record_consumption(
            command["sagaId"],
            102,
            (PartConsumption(999, 10, 1),),
        )

    with pytest.raises(MessageValidationError, match="exceeds"):
        work.record_consumption(
            command["sagaId"],
            102,
            (PartConsumption(501, 10, 4),),
        )


def test_finishing_one_service_does_not_finalize_execution(execution):
    repository, command, _result, work = execution
    for service_id in (101, 102):
        work.start_service(
            command["sagaId"],
            service_id,
            started_at=STARTED_AT,
        )

    result = work.finish_service(
        command["sagaId"],
        101,
        completed_at=STARTED_AT + timedelta(hours=1),
    )

    assert result.status is ServiceStatus.CONCLUIDO
    assert result.outbox_message is None
    assert repository.get_pending_outbox() == [repository.get_outbox(command["messageId"])]
    assert repository.get_by_saga_id(command["sagaId"]).status is ExecutionStatus.EM_EXECUCAO


def test_last_service_emits_execucao_finalizada_once_with_aggregated_consumption(execution):
    repository, command, _result, work = execution
    first_part = (PartConsumption(501, 10, 2),)
    second_parts = (PartConsumption(501, 10, 1), PartConsumption(502, 11, 1))
    for service_id, consumed in ((101, first_part), (102, second_parts)):
        work.start_service(
            command["sagaId"],
            service_id,
            started_at=STARTED_AT,
        )
        work.record_consumption(command["sagaId"], service_id, consumed)

    first = work.finish_service(
        command["sagaId"],
        101,
        completed_at=STARTED_AT + timedelta(hours=1),
    )
    assert first.outbox_message is None
    final = work.finish_service(
        command["sagaId"],
        102,
        completed_at=STARTED_AT + timedelta(hours=2),
    )
    outbox = final.outbox_message
    execution_id = repository.get_by_saga_id(command["sagaId"]).execution_id

    assert outbox is not None
    assert outbox.message["type"] == "ExecucaoFinalizada"
    assert outbox.message["version"] == 1
    assert outbox.message["payload"] == {
        "execucaoId": execution_id,
        "finalizadaEm": (STARTED_AT + timedelta(hours=2)).isoformat(),
        "pecasConsumidas": [
            {"itemPecaOSId": 501, "pecaId": 10, "quantidade": 3},
            {"itemPecaOSId": 502, "pecaId": 11, "quantidade": 1},
        ],
    }
    assert outbox.input_message_id is not None
    assert outbox.message_attributes["traceparent"]["StringValue"].startswith(
        f"00-{TRACE_ATTRIBUTES['traceparent']['StringValue'].split('-')[1]}-"
    )
    assert outbox.message_attributes["tracestate"] == TRACE_ATTRIBUTES["tracestate"]
    assert repository.get_by_saga_id(command["sagaId"]).status is ExecutionStatus.CONCLUIDA

    replay = work.finish_service(
        command["sagaId"],
        102,
        completed_at=STARTED_AT + timedelta(hours=2),
        message_id=outbox.input_message_id,
    )
    assert replay.duplicate
    assert replay.outbox_message.message["messageId"] == outbox.message["messageId"]
    assert (
        len(
            [
                item
                for item in repository.get_pending_outbox()
                if item.message["type"] == "ExecucaoFinalizada"
            ]
        )
        == 1
    )


def test_finish_requires_started_service_and_completed_at_not_before_start(execution):
    _repository, command, _result, work = execution
    with pytest.raises(TransitionNotAllowedError):
        work.finish_service(command["sagaId"], 101, completed_at=STARTED_AT)

    work.start_service(command["sagaId"], 101, started_at=STARTED_AT)
    with pytest.raises(MessageValidationError, match="earlier"):
        work.finish_service(
            command["sagaId"],
            101,
            completed_at=STARTED_AT - timedelta(seconds=1),
        )


def test_concurrent_finishing_of_different_services_emits_one_completion(execution):
    repository, command, _result, work = execution
    for service_id in (101, 102):
        work.start_service(command["sagaId"], service_id, started_at=STARTED_AT)

    barrier = Barrier(2)
    commit_lock = Lock()
    commit_calls = 0
    commit = repository.commit_service_command

    def synchronized_commit(*args, **kwargs):
        nonlocal commit_calls
        with commit_lock:
            commit_calls += 1
            call_number = commit_calls
        if call_number <= 2:
            barrier.wait()
        return commit(*args, **kwargs)

    repository.commit_service_command = synchronized_commit

    def finish(service_id):
        return work.finish_service(
            command["sagaId"],
            service_id,
            completed_at=STARTED_AT + timedelta(hours=1),
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(finish, (101, 102)))

    completed = [result for result in results if result.outbox_message is not None]
    assert len(completed) == 1
    assert commit_calls >= 3
    assert completed[0].outbox_message.message["type"] == "ExecucaoFinalizada"
    assert repository.get_by_saga_id(command["sagaId"]).status is ExecutionStatus.CONCLUIDA


def test_cancellation_stops_unfinished_services_and_releases_via_contract_response(execution):
    repository, command, _result, _work = execution
    cancellation = {
        **command,
        "messageId": str(uuid4()),
        "type": "CancelarExecucao",
        "causationId": command["messageId"],
        "payload": {
            "execucaoId": repository.get_by_saga_id(command["sagaId"]).execution_id,
            "motivo": "CANCELADO_PELO_CLIENTE",
        },
    }
    result = ExecuteCommandUseCase(repository).handle(cancellation, TRACE_ATTRIBUTES)

    saved = repository.get_by_saga_id(command["sagaId"])
    lifecycle, _version = repository.load_lifecycle(command["sagaId"])
    outbox = repository.get_outbox(cancellation["messageId"])
    assert result.message_type == "ExecucaoCancelada"
    assert saved.status is ExecutionStatus.CANCELADA
    assert saved.cancellation_reason == "CANCELADO_PELO_CLIENTE"
    assert lifecycle.service(101).status is ServiceStatus.CANCELADO
    assert lifecycle.service(102).status is ServiceStatus.CANCELADO
    assert outbox.message["payload"] == {"execucaoId": saved.execution_id}


def test_cancellation_after_failure_is_supported_and_idempotent(execution):
    repository, command, _result, _work = execution
    record = repository.records[command["sagaId"]]
    record.status = ExecutionStatus.FALHOU
    cancellation = {
        **command,
        "messageId": str(uuid4()),
        "type": "CancelarExecucao",
        "causationId": command["messageId"],
        "payload": {
            "execucaoId": record.execution_id,
            "motivo": "EXECUCAO_FALHOU",
        },
    }
    processor = ExecuteCommandUseCase(repository)
    result = processor.handle(cancellation)
    replay = processor.handle(cancellation)

    assert result.message_type == "ExecucaoCancelada"
    assert replay.message_id == result.message_id
    assert repository.get_by_saga_id(command["sagaId"]).status is ExecutionStatus.CANCELADA


def test_v11_item_peca_os_id_is_optional_for_legacy_start_messages():
    command = start_message()
    command["payload"]["pecas"] = [{"pecaId": 9, "quantidade": 1}]
    repository = InMemoryExecutionRepository()
    ExecuteCommandUseCase(repository).handle(command)

    lifecycle, _version = repository.load_lifecycle(command["sagaId"])
    assert lifecycle.reserved_parts == (PartConsumption(None, 9, 1),)
    assert len(lifecycle.services) == 1
