from datetime import datetime, timedelta, timezone
from types import MappingProxyType
from uuid import NAMESPACE_URL, uuid5

import pytest

from execution_service.domain.exceptions import (
    IdempotencyKeyReuseError,
    MessageValidationError,
    TransitionNotAllowedError,
)
from execution_service.domain.service_execution import (
    DomainEventType,
    ExecutionCompleted,
    ExecutionLifecycle,
    PartConsumption,
    ServiceExecution,
    ServiceStatus,
)

STARTED_AT = datetime(2026, 10, 31, 10, tzinfo=timezone.utc)
PARTS = (PartConsumption(item_peca_os_id=31, peca_id=9, quantity=2),)


def message_id(name):
    return str(uuid5(NAMESPACE_URL, name))


def start_service(service=None, name="start-1"):
    service = service or ServiceExecution(service_id=4)
    service.request_service_start(message_id(name), STARTED_AT, PARTS)
    return service


def accept_start(service, start_name="start-1", decision_name="accepted-1"):
    return service.confirm_start_and_consumption(
        message_id(start_name),
        message_id(decision_name),
    )


def reject_start(service, start_name="start-1", decision_name="rejected-1"):
    return service.reject_start_after_os_confirmation(
        message_id(start_name),
        message_id(decision_name),
    )


def test_service_start_waits_for_explicit_start_and_consumption_acceptance():
    service = start_service()

    assert service.status is ServiceStatus.INICIO_PENDENTE
    assert service.started_at is None
    assert service.parts_used == ()
    assert service.pending_parts == PARTS

    assert accept_start(service) is True

    assert service.status is ServiceStatus.EM_EXECUCAO
    assert service.started_at == STARTED_AT
    assert service.parts_used == PARTS
    assert service.pending_started_at is None
    assert service.pending_parts == ()


def test_repeated_start_and_acceptance_are_idempotent_for_current_attempt():
    service = start_service()

    assert service.request_service_start(message_id("start-1"), STARTED_AT, PARTS) is False
    assert accept_start(service) is True
    assert accept_start(service) is False


def test_reusing_start_message_id_with_different_parts_is_rejected():
    service = start_service()
    changed_parts = (PartConsumption(item_peca_os_id=31, peca_id=9, quantity=3),)

    with pytest.raises(IdempotencyKeyReuseError):
        service.request_service_start(message_id("start-1"), STARTED_AT, changed_parts)


def test_pending_start_is_not_cleared_without_explicit_os_rejection():
    service = start_service()

    assert service.status is ServiceStatus.INICIO_PENDENTE
    assert service.start_resolution is None
    assert service.pending_parts == PARTS

    with pytest.raises(TransitionNotAllowedError):
        service.request_service_start(message_id("start-2"), STARTED_AT, PARTS)


def test_explicit_rejection_allows_a_new_start_attempt():
    service = start_service()

    assert reject_start(service) is True

    assert service.status is ServiceStatus.PENDENTE
    assert service.pending_started_at is None
    assert service.pending_parts == ()
    assert service.start_resolution == "REJECTED"
    assert service.start_resolution_message_id == message_id("rejected-1")
    assert reject_start(service) is False

    assert service.request_service_start(message_id("start-2"), STARTED_AT, PARTS) is True
    assert service.status is ServiceStatus.INICIO_PENDENTE


def test_old_start_redelivery_cannot_replace_the_new_pending_attempt():
    service = start_service(name="start-old")
    assert reject_start(service, "start-old", "reject-old") is True
    assert service.request_service_start(message_id("start-current"), STARTED_AT, PARTS) is True

    with pytest.raises(TransitionNotAllowedError):
        service.request_service_start(message_id("start-old"), STARTED_AT, PARTS)

    assert service.start_message_id == message_id("start-current")
    assert service.status is ServiceStatus.INICIO_PENDENTE


def test_start_resolution_must_match_current_attempt_and_be_consistent():
    service = start_service()

    with pytest.raises(TransitionNotAllowedError):
        service.confirm_start_and_consumption(message_id("other-start"), message_id("accepted-1"))

    accept_start(service)

    with pytest.raises(TransitionNotAllowedError):
        service.reject_start_after_os_confirmation(message_id("start-1"), message_id("rejected-1"))


@pytest.mark.parametrize(
    "values",
    [
        {"item_peca_os_id": 0, "peca_id": 9, "quantity": 1},
        {"item_peca_os_id": 31, "peca_id": 0, "quantity": 1},
        {"item_peca_os_id": 31, "peca_id": 9, "quantity": 0},
        {"item_peca_os_id": True, "peca_id": 9, "quantity": 1},
    ],
)
def test_part_consumption_requires_positive_integer_identifiers_and_quantity(values):
    with pytest.raises(MessageValidationError):
        PartConsumption(**values)


def test_start_is_not_allowed_when_service_is_running_or_completed():
    service = start_service()
    accept_start(service)

    with pytest.raises(TransitionNotAllowedError):
        service.request_service_start(message_id("start-2"), STARTED_AT, PARTS)

    service.finish_service(message_id("finish-1"), STARTED_AT + timedelta(hours=1))

    with pytest.raises(TransitionNotAllowedError):
        service.request_service_start(message_id("start-3"), STARTED_AT, PARTS)


def test_finish_requires_running_service_and_non_decreasing_dates():
    service = ServiceExecution(service_id=4)
    with pytest.raises(TransitionNotAllowedError):
        service.finish_service(message_id("finish-1"), STARTED_AT + timedelta(hours=1))

    start_service(service)
    with pytest.raises(TransitionNotAllowedError):
        service.finish_service(message_id("finish-1"), STARTED_AT + timedelta(hours=1))

    accept_start(service)
    with pytest.raises(MessageValidationError, match="earlier"):
        service.finish_service(message_id("finish-1"), STARTED_AT - timedelta(seconds=1))


def test_finish_records_completion_and_is_idempotent():
    service = start_service()
    accept_start(service)
    completed_at = STARTED_AT + timedelta(hours=1)

    assert service.finish_service(message_id("finish-1"), completed_at) is True
    assert service.status is ServiceStatus.CONCLUIDO
    assert service.completed_at == completed_at
    assert service.finish_service(message_id("finish-1"), completed_at) is False

    with pytest.raises(IdempotencyKeyReuseError):
        service.finish_service(message_id("finish-1"), completed_at + timedelta(seconds=1))


def test_lifecycle_emits_one_internal_completion_event_after_last_service():
    lifecycle = ExecutionLifecycle(
        os_id=77,
        saga_id=message_id("saga-1"),
        execution_id=message_id("execution-1"),
        service_ids=[4, 5],
    )
    for service_id in lifecycle.services:
        name = f"start-{service_id}"
        lifecycle.service(service_id).request_service_start(message_id(name), STARTED_AT, PARTS)
        lifecycle.service(service_id).confirm_start_and_consumption(
            message_id(name),
            message_id(f"accepted-{service_id}"),
        )

    assert (
        lifecycle.finish_service(4, message_id("finish-4"), STARTED_AT + timedelta(hours=1)) is None
    )
    assert lifecycle.completion_event is None

    event = lifecycle.finish_service(
        5,
        message_id("finish-5"),
        STARTED_AT + timedelta(hours=2),
    )

    assert event is lifecycle.completion_event
    assert event.type is DomainEventType.EXECUTION_COMPLETED
    assert event.os_id == 77
    assert event.saga_id == message_id("saga-1")
    assert event.execution_id == message_id("execution-1")
    assert event.completed_at == STARTED_AT + timedelta(hours=2)
    assert (
        lifecycle.finish_service(
            5,
            message_id("finish-5"),
            STARTED_AT + timedelta(hours=2),
        )
        is None
    )


def test_pending_service_prevents_execution_completion_event():
    lifecycle = ExecutionLifecycle(
        os_id=77,
        saga_id=message_id("saga-1"),
        execution_id=message_id("execution-1"),
        service_ids=[4, 5],
    )
    first = lifecycle.service(4)
    first.request_service_start(message_id("start-4"), STARTED_AT, PARTS)
    first.confirm_start_and_consumption(message_id("start-4"), message_id("accepted-4"))
    assert (
        lifecycle.finish_service(4, message_id("finish-4"), STARTED_AT + timedelta(hours=1)) is None
    )

    second = lifecycle.service(5)
    second.request_service_start(message_id("start-5"), STARTED_AT, PARTS)
    assert lifecycle.completion_event is None


def test_lifecycle_rejects_duplicate_service_ids_and_services_outside_os():
    with pytest.raises(MessageValidationError, match="unique"):
        ExecutionLifecycle(77, message_id("saga-1"), message_id("exec-1"), [4, 4])

    lifecycle = ExecutionLifecycle(77, message_id("saga-1"), message_id("exec-1"), [4])
    with pytest.raises(MessageValidationError, match="does not belong"):
        lifecycle.service(5)


@pytest.mark.parametrize("service_id", [True, False, 0, -1, "4"])
def test_lifecycle_rejects_invalid_service_ids(service_id):
    with pytest.raises(MessageValidationError):
        ExecutionLifecycle(77, message_id("saga-1"), message_id("exec-1"), [service_id])


@pytest.mark.parametrize("identifier", [True, False, 0, -1, "77"])
def test_lifecycle_rejects_invalid_os_ids(identifier):
    with pytest.raises(MessageValidationError):
        ExecutionLifecycle(identifier, message_id("saga-1"), message_id("exec-1"), [4])


@pytest.mark.parametrize(
    "saga_id,execution_id",
    [
        ("not-a-uuid", message_id("execution")),
        (message_id("saga"), ""),
        (message_id("saga"), "00000000-0000-0000-0000-000000000000"),
    ],
)
def test_lifecycle_rejects_invalid_correlation_ids(saga_id, execution_id):
    with pytest.raises(MessageValidationError):
        ExecutionLifecycle(77, saga_id, execution_id, [4])


@pytest.mark.parametrize(
    "method,args",
    [
        ("request_service_start", ("not-a-uuid", STARTED_AT, PARTS)),
        ("confirm_start_and_consumption", ("not-a-uuid", message_id("decision"))),
        ("reject_start_after_os_confirmation", (message_id("start-1"), "bad-decision-id")),
        ("finish_service", ("not-a-uuid", STARTED_AT)),
    ],
)
def test_public_commands_reject_malformed_message_identifiers(method, args):
    service = ServiceExecution(4)
    if method != "request_service_start":
        service.request_service_start(message_id("start-1"), STARTED_AT, PARTS)

    with pytest.raises(MessageValidationError):
        getattr(service, method)(*args)


def test_service_lookup_rejects_boolean_instead_of_resolving_integer_key():
    lifecycle = ExecutionLifecycle(77, message_id("saga-1"), message_id("exec-1"), [1])

    with pytest.raises(MessageValidationError):
        lifecycle.service(True)


def test_entity_construction_and_collections_preserve_invariants():
    with pytest.raises(TypeError):
        ServiceExecution(4, status=ServiceStatus.EM_EXECUCAO)

    source_parts = [PartConsumption(item_peca_os_id=31, peca_id=9, quantity=2)]
    service = ServiceExecution(4)
    service.request_service_start(message_id("start-1"), STARTED_AT, source_parts)
    source_parts.append(PartConsumption(item_peca_os_id=32, peca_id=10, quantity=5))

    assert service.pending_parts == PARTS
    with pytest.raises(AttributeError):
        service.status = ServiceStatus.EM_EXECUCAO
    with pytest.raises(AttributeError):
        service.pending_parts = ()
    with pytest.raises(AttributeError):
        service.pending_parts.append(source_parts[1])


def test_lifecycle_service_map_cannot_be_modified_externally():
    lifecycle = ExecutionLifecycle(77, message_id("saga-1"), message_id("exec-1"), [4])

    assert isinstance(lifecycle.services, MappingProxyType)
    with pytest.raises(TypeError):
        lifecycle.services[5] = ServiceExecution(5)


def test_execution_completed_validates_identifiers_and_timestamp():
    event = ExecutionCompleted(
        os_id=77,
        saga_id=message_id("saga-1"),
        execution_id=message_id("execution-1"),
        completed_at=STARTED_AT,
    )
    assert event.type is DomainEventType.EXECUTION_COMPLETED

    with pytest.raises(MessageValidationError):
        ExecutionCompleted(77, "bad-saga", message_id("execution-1"), STARTED_AT)
    with pytest.raises(MessageValidationError):
        ExecutionCompleted(77, message_id("saga-1"), message_id("execution-1"), datetime.now())
