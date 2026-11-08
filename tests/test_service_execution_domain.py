from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import NAMESPACE_URL, uuid5

import pytest

from execution_service.domain.exceptions import (
    IdempotencyKeyReuseError,
    MessageValidationError,
    TransitionNotAllowedError,
)
from execution_service.domain.service_execution import (
    ExecutionLifecycle,
    PartConsumption,
    ServiceExecution,
    ServiceStatus,
)

STARTED_AT = datetime(2026, 10, 31, 10, tzinfo=timezone.utc)
RESERVED = (PartConsumption(31, 9, 3),)


def message_id(name: str) -> str:
    return str(uuid5(NAMESPACE_URL, name))


def test_service_start_and_consumption_are_separate_transitions():
    service = ServiceExecution(4)
    with pytest.raises(TransitionNotAllowedError):
        service.record_consumption(message_id("consume"), RESERVED)

    assert service.request_service_start(
        message_id("start"),
        STARTED_AT,
    )
    assert service.status is ServiceStatus.EM_EXECUCAO
    assert service.started_at == STARTED_AT
    assert service.parts_used == ()

    assert not service.request_service_start(message_id("start"), STARTED_AT)
    with pytest.raises(IdempotencyKeyReuseError):
        service.request_service_start(
            message_id("start"),
            STARTED_AT + timedelta(seconds=1),
        )
    assert service.record_consumption(message_id("consume"), RESERVED)
    assert service.parts_used == RESERVED
    assert not service.record_consumption(message_id("consume"), RESERVED)
    with pytest.raises(IdempotencyKeyReuseError):
        service.record_consumption(message_id("consume"), (PartConsumption(31, 9, 2),))


def test_finish_requires_running_service_and_monotonic_datetime():
    service = ServiceExecution(4)
    with pytest.raises(TransitionNotAllowedError):
        service.finish_service(message_id("finish"), STARTED_AT)

    service.request_service_start(message_id("start"), STARTED_AT)
    with pytest.raises(MessageValidationError, match="earlier"):
        service.finish_service(
            message_id("finish"),
            STARTED_AT - timedelta(seconds=1),
        )

    completed = STARTED_AT + timedelta(hours=1)
    assert service.finish_service(message_id("finish"), completed)
    assert service.status is ServiceStatus.CONCLUIDO
    assert service.completed_at == completed
    assert not service.finish_service(message_id("finish"), completed)


def test_lifecycle_emits_one_completion_after_every_service_and_reports_consumed_parts():
    lifecycle = ExecutionLifecycle(
        os_id=77,
        saga_id=message_id("saga"),
        execution_id=message_id("execution"),
        service_ids=[4, 5],
        reserved_parts=(PartConsumption(31, 9, 3),),
    )
    lifecycle.start_service(4, message_id("start-4"), STARTED_AT)
    lifecycle.start_service(5, message_id("start-5"), STARTED_AT)
    lifecycle.record_service_consumption(4, message_id("consume-4"), (PartConsumption(31, 9, 2),))
    lifecycle.record_service_consumption(5, message_id("consume-5"), (PartConsumption(31, 9, 1),))
    assert (
        lifecycle.finish_service(4, message_id("finish-4"), STARTED_AT + timedelta(hours=1)) is None
    )

    event = lifecycle.finish_service(
        5,
        message_id("finish-5"),
        STARTED_AT + timedelta(hours=2),
    )
    assert event is lifecycle.completion_event
    assert event.parts_consumed == (PartConsumption(31, 9, 3),)
    assert (
        lifecycle.finish_service(
            5,
            message_id("finish-5"),
            STARTED_AT + timedelta(hours=2),
        )
        is None
    )


def test_lifecycle_validates_consumption_and_service_ownership():
    lifecycle = ExecutionLifecycle(
        77,
        message_id("saga"),
        message_id("execution"),
        [4],
        reserved_parts=(PartConsumption(31, 9, 2),),
    )
    lifecycle.start_service(4, message_id("start"), STARTED_AT)
    with pytest.raises(MessageValidationError, match="not reserved"):
        lifecycle.record_service_consumption(
            4, message_id("consume-1"), (PartConsumption(32, 9, 1),)
        )
    with pytest.raises(MessageValidationError, match="exceeds"):
        lifecycle.record_service_consumption(
            4, message_id("consume-2"), (PartConsumption(31, 9, 3),)
        )
    with pytest.raises(MessageValidationError, match="does not belong"):
        lifecycle.start_service(999, message_id("start-3"), STARTED_AT)


def test_lifecycle_state_round_trip_preserves_service_and_trace_data():
    lifecycle = ExecutionLifecycle(
        77,
        message_id("saga"),
        message_id("execution"),
        [4],
        reserved_parts=RESERVED,
        traceparent="00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        tracestate="vendor=value",
    )
    lifecycle.start_service(4, message_id("start"), STARTED_AT)
    lifecycle.record_service_consumption(4, message_id("consume"), RESERVED)
    lifecycle.finish_service(
        4,
        message_id("finish"),
        STARTED_AT + timedelta(hours=1),
    )

    restored = ExecutionLifecycle.from_state(lifecycle.to_state())
    assert restored.service(4).status is ServiceStatus.CONCLUIDO
    assert restored.service(4).parts_used == RESERVED
    assert restored.completion_event.parts_consumed == RESERVED
    assert not restored.service(4).record_consumption(message_id("consume"), RESERVED)
    assert restored.traceparent == lifecycle.traceparent
    assert restored.tracestate == lifecycle.tracestate


def test_cancellation_marks_only_unfinished_services_and_reason_is_retained():
    lifecycle = ExecutionLifecycle(
        77,
        message_id("saga"),
        message_id("execution"),
        [4, 5],
        reserved_parts=RESERVED,
    )
    lifecycle.start_service(4, message_id("start-4"), STARTED_AT)
    lifecycle.finish_service(
        4,
        message_id("finish-4"),
        STARTED_AT + timedelta(hours=1),
    )
    lifecycle.cancel(message_id("cancel"), "CANCELADO_PELO_CLIENTE")

    assert lifecycle.service(4).status is ServiceStatus.CONCLUIDO
    assert lifecycle.service(5).status is ServiceStatus.CANCELADO
    assert lifecycle.cancellation_reason == "CANCELADO_PELO_CLIENTE"


@pytest.mark.parametrize(
    "values",
    [
        {"item_peca_os_id": 0, "peca_id": 9, "quantity": 1},
        {"item_peca_os_id": 31, "peca_id": 9, "quantity": 0},
        {"item_peca_os_id": True, "peca_id": 9, "quantity": 1},
    ],
)
def test_part_consumption_requires_positive_identifiers_and_quantity(values):
    with pytest.raises(MessageValidationError):
        PartConsumption(**values)
