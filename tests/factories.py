from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from execution_service.application.dto import OutboxMessage
from execution_service.domain.enums import ExecutionStatus, SagaMessageType
from execution_service.domain.models import ExecutionRecord

TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
TRACE_ATTRIBUTES = {
    "traceparent": {"DataType": "String", "StringValue": TRACEPARENT},
    "tracestate": {"DataType": "String", "StringValue": "vendor=value"},
}


def start_message(**overrides):
    message = {
        "messageId": str(uuid4()),
        "type": SagaMessageType.INICIAR_EXECUCAO.value,
        "version": 1,
        "sagaId": str(uuid4()),
        "osId": 77,
        "causationId": str(uuid4()),
        "requestId": "request-1",
        "occurredAt": datetime.now(timezone.utc).isoformat(),
        "payload": {
            "orcamentoId": str(uuid4()),
            "servicos": [{"servicoId": 8, "quantidade": 2}],
            "pecas": [{"pecaId": 9, "quantidade": 1}],
        },
    }
    message.update(overrides)
    return message


def cancel_message(start, execution_id, **overrides):
    message = start_message(
        messageId=str(uuid4()),
        type=SagaMessageType.CANCELAR_EXECUCAO.value,
        sagaId=start["sagaId"],
        osId=start["osId"],
        causationId=start["messageId"],
        payload={"execucaoId": execution_id, "motivo": "CANCELADO_PELO_CLIENTE"},
    )
    message.update(overrides)
    return message


def sqs_message(body):
    import json

    return {
        "MessageId": "input-sqs-id",
        "ReceiptHandle": "receipt",
        "Body": json.dumps(body),
        "MessageAttributes": {
            "traceparent": {
                "DataType": "String",
                "StringValue": TRACEPARENT,
            },
        },
        "Attributes": {"ApproximateReceiveCount": "1"},
    }


def execution_record(saga_id=None):
    now = datetime.now(timezone.utc)
    return ExecutionRecord(
        execution_id=str(uuid4()),
        saga_id=saga_id or str(uuid4()),
        os_id=13,
        budget_id=str(uuid4()),
        services=[{"servicoId": 2, "quantidade": 1}],
        parts=[{"pecaId": 7, "quantidade": 2}],
        status=ExecutionStatus.EM_EXECUCAO,
        started_at=now,
        created_at=now,
        updated_at=now,
    )


def outbox_message():
    return OutboxMessage(
        message={
            "messageId": str(uuid4()),
            "type": "ExecucaoIniciada",
            "version": 1,
            "sagaId": str(uuid4()),
            "osId": 13,
            "causationId": str(uuid4()),
            "requestId": "request-13",
            "occurredAt": datetime.now(timezone.utc).isoformat(),
            "payload": {"execucaoId": str(uuid4())},
        },
        message_attributes={"traceparent": {"DataType": "String", "StringValue": TRACEPARENT}},
    )
