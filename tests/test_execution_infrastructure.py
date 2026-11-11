from __future__ import annotations

import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError

from execution_service.application.service_execution_use_cases import ExecutionWorkUseCase
from execution_service.application.use_cases import ExecuteCommandUseCase
from execution_service.domain.exceptions import (
    ConcurrentExecutionUpdateError,
    IdempotencyKeyReuseError,
)
from execution_service.domain.service_execution import PartConsumption
from execution_service.infrastructure.dynamodb_repository import (
    DynamoDBExecutionRepository,
)
from execution_service.infrastructure.fake_repository import (
    InMemoryExecutionRepository,
)
from execution_service.infrastructure.outbox_relay import OutboxRelay
from execution_service.infrastructure.sqs_adapter import (
    FakeMessagePublisher,
    SQSFifoMessagePublisher,
)
from execution_service.interfaces.sqs_consumer import SQSFifoConsumer
from execution_service.logging_config import JsonLogFormatter
from tests.factories import (
    TRACE_ATTRIBUTES,
    cancel_message,
    execution_record,
    outbox_message,
    sqs_message,
    start_message,
)


class FakeSQSClient:
    def __init__(self, message):
        self.message = message
        self.receive_kwargs = {}
        self.deleted = []
        self.events = []
        self.fail_delete = False

    def receive_message(self, **kwargs):
        self.receive_kwargs = kwargs
        return {"Messages": [self.message]}

    def delete_message(self, **kwargs):
        self.events.append("delete")

        if self.fail_delete:
            raise RuntimeError("SQS delete failed")

        self.deleted.append(kwargs)


class FakePublisher(FakeMessagePublisher):
    def __init__(self):
        super().__init__()
        self.attempts = []
        self.fail_send = False

    def publish(self, message, **kwargs):
        self.attempts.append(
            {
                "message": message,
                **kwargs,
            }
        )

        if self.fail_send:
            raise RuntimeError("SQS send failed")

        return super().publish(message, **kwargs)


def test_sqs_consumer_persists_command_before_acknowledging(
    consumer_factory,
):
    body = start_message()
    sqs = FakeSQSClient(sqs_message(body))
    consumer, repository = consumer_factory(sqs)

    confirmed = consumer.poll_once()

    assert confirmed == 1
    assert sqs.receive_kwargs["VisibilityTimeout"] == 30
    assert sqs.receive_kwargs["WaitTimeSeconds"] == 20
    assert sqs.receive_kwargs["MessageAttributeNames"] == ["All"]

    assert sqs.events == ["delete"]
    assert len(sqs.deleted) == 1

    assert sqs.deleted[0]["ReceiptHandle"] == "receipt"

    saved = repository.get_by_saga_id(body["sagaId"])
    pending = repository.get_outbox(body["messageId"])

    assert saved is not None
    assert pending is not None
    assert pending.published is False


def test_invalid_message_is_not_deleted_and_is_left_for_sqs_redrive(
    consumer_factory,
):
    sqs = FakeSQSClient(sqs_message({"messageId": "missing-fields"}))
    consumer, _repository = consumer_factory(sqs)

    assert consumer.poll_once() == 0
    assert sqs.deleted == []


def test_cancellation_is_persisted_and_acknowledged(
    consumer_factory,
):
    start = start_message()
    sqs = FakeSQSClient(sqs_message(start))
    consumer, repository = consumer_factory(sqs)

    assert consumer.poll_once() == 1

    execution = repository.get_by_saga_id(start["sagaId"])
    assert execution is not None

    cancellation = cancel_message(
        start,
        execution.execution_id,
    )
    sqs.message = sqs_message(cancellation)

    assert consumer.poll_once() == 1

    assert len(sqs.deleted) == 2

    assert repository.get_by_saga_id(start["sagaId"]).status.value == "CANCELADA"

    assert repository.get_input_fingerprint(cancellation["messageId"]) is not None
    assert repository.get_outbox(cancellation["messageId"]).message["type"] == "ExecucaoCancelada"


def test_outbox_relay_recovers_pending_response_after_publish_failure(
    consumer_factory,
):
    body = start_message()
    sqs = FakeSQSClient(sqs_message(body))
    consumer, repository = consumer_factory(sqs)

    # O Consumer persiste o comando e confirma a mensagem de entrada.
    assert consumer.poll_once() == 1
    assert len(sqs.deleted) == 1

    pending = repository.get_outbox(body["messageId"])
    assert pending is not None
    assert pending.published is False

    publisher = FakePublisher()
    publisher.fail_send = True

    relay = OutboxRelay(repository, publisher)

    # A falha de publicação não perde a mensagem do Outbox.
    assert relay.poll_once() == 0
    assert repository.get_outbox(body["messageId"]).published is False
    assert len(publisher.messages) == 0

    publisher.fail_send = False

    # Uma nova tentativa publica e confirma o Outbox.
    assert relay.poll_once() == 1

    assert len(publisher.messages) == 1
    assert repository.get_outbox(body["messageId"]).published is True

    published = publisher.messages[0]

    assert published["message_group_id"] == body["sagaId"]
    assert published["message_deduplication_id"] == pending.message["messageId"]


def test_pre_execution_failure_is_published_from_outbox():
    repository = InMemoryExecutionRepository()
    command = start_message()
    result = ExecuteCommandUseCase(repository).fail_execution_start(
        command,
        "PECA_SEM_ESTOQUE",
        "Peca sem saldo para reserva.",
        TRACE_ATTRIBUTES,
    )
    publisher = FakePublisher()

    assert result.message_type == "ExecucaoFalhou"
    assert repository.get_by_saga_id(command["sagaId"]) is None
    assert OutboxRelay(repository, publisher).poll_once() == 1

    published = publisher.messages[0]["message"]
    assert published["type"] == "ExecucaoFalhou"
    assert published["sagaId"] == command["sagaId"]
    assert published["osId"] == command["osId"]
    assert published["requestId"] == command["requestId"]
    assert published["causationId"] == command["messageId"]
    assert published["payload"] == {
        "orcamentoId": command["payload"]["orcamentoId"],
        "execucaoId": None,
        "erro": {
            "codigo": "PECA_SEM_ESTOQUE",
            "mensagem": "Peca sem saldo para reserva.",
        },
    }


def test_failed_execution_compensation_flows_through_sqs_consumer_and_outbox(
    consumer_factory,
):
    command = start_message()
    command["payload"]["servicos"].append({"servicoId": 10, "quantidade": 1})
    sqs = FakeSQSClient(sqs_message(command))
    consumer, repository = consumer_factory(sqs)
    publisher = FakePublisher()
    relay = OutboxRelay(repository, publisher)

    assert consumer.poll_once() == 1
    assert relay.poll_once() == 1

    work = ExecutionWorkUseCase(repository)
    work.start_service(command["sagaId"], 8)
    failure = work.fail_service(
        command["sagaId"],
        8,
        "SERVICO_INDISPONIVEL",
        "Serviço indisponível.",
    )
    assert failure.outbox_message is not None
    failure_message = failure.outbox_message.message
    assert relay.poll_once() == 1
    assert publisher.messages[-1]["message"] == failure_message

    execution = repository.get_by_saga_id(command["sagaId"])
    cancellation = cancel_message(command, execution.execution_id)
    cancellation["causationId"] = failure_message["messageId"]
    cancellation["payload"]["motivo"] = "EXECUCAO_FALHOU"
    sqs.message = sqs_message(cancellation)

    assert consumer.poll_once() == 1
    assert relay.poll_once() == 1

    cancelled_message = publisher.messages[-1]["message"]
    assert cancelled_message["type"] == "ExecucaoCancelada"
    assert cancelled_message["causationId"] == cancellation["messageId"]
    assert cancelled_message["payload"] == {"execucaoId": execution.execution_id}

    saved = repository.get_by_saga_id(command["sagaId"])
    lifecycle, _version = repository.load_lifecycle(command["sagaId"])
    assert saved.status.value == "CANCELADA"
    assert lifecycle.service(8).status.value == "FALHOU"
    assert lifecycle.service(10).status.value == "CANCELADO"


def test_outbox_confirmation_failure_retries_with_same_response_id():
    class FailMarkOnceRepository(InMemoryExecutionRepository):
        def __init__(self):
            super().__init__()
            self.fail_mark = True

        def mark_outbox_published(self, input_message_id):
            if self.fail_mark:
                self.fail_mark = False
                raise RuntimeError("outbox confirmation failed")

            return super().mark_outbox_published(input_message_id)

    repository = FailMarkOnceRepository()
    record = execution_record()
    response = outbox_message()
    input_id = str(uuid4())

    repository.register_command(
        record,
        input_id,
        "body-fingerprint",
        response,
        None,
    )

    publisher = FakePublisher()
    relay = OutboxRelay(repository, publisher)

    # A publicação ocorre, mas a confirmação falha.
    assert relay.poll_once() == 0

    pending = repository.get_outbox(input_id)
    assert pending is not None
    assert pending.published is False

    # O relay tenta publicar novamente a mesma mensagem.
    assert relay.poll_once() == 1

    assert len(publisher.messages) == 2
    assert (
        publisher.attempts[0]["message_deduplication_id"]
        == publisher.attempts[1]["message_deduplication_id"]
    )
    assert repository.get_outbox(input_id).published is True


def test_sqs_publisher_serializes_integral_and_fractional_decimals_recursively():
    class SQS:
        def send_message(self, **kwargs):
            self.kwargs = kwargs
            return {"MessageId": "sent"}

    client = SQS()
    publisher = SQSFifoMessagePublisher(client, "responses")
    message = {
        "osId": Decimal("42"),
        "version": Decimal("1.0"),
        "messageId": "message-1",
        "type": "ExecucaoIniciada",
        "sagaId": "saga-1",
        "requestId": "request-1",
        "payload": {
            "quantidade": Decimal("1.250"),
            "nested": [Decimal("0.1"), {"precisao": Decimal("12345678901234567890.123456789")}],
        },
    }

    publisher.publish(
        message,
        message_group_id="saga-1",
        message_deduplication_id="message-1",
        message_attributes={},
    )

    assert client.kwargs["MessageBody"] == (
        '{"osId":42,"version":1,"messageId":"message-1",'
        '"type":"ExecucaoIniciada","sagaId":"saga-1","requestId":"request-1",'
        '"payload":{"quantidade":1.250,'
        '"nested":[0.1,{"precisao":12345678901234567890.123456789}]}}'
    )
    decoded = json.loads(client.kwargs["MessageBody"])
    assert decoded == {
        "osId": 42,
        "version": 1,
        "messageId": "message-1",
        "type": "ExecucaoIniciada",
        "sagaId": "saga-1",
        "requestId": "request-1",
        "payload": {
            "quantidade": 1.25,
            "nested": [0.1, {"precisao": 12345678901234567890.123456789}],
        },
    }


def test_sqs_publisher_rejects_non_finite_decimal_values():
    class SQS:
        def send_message(self, **kwargs):
            raise AssertionError(f"send_message should not be called: {kwargs}")

    publisher = SQSFifoMessagePublisher(SQS(), "responses")
    non_finite_os_id = Decimal("NaN")

    with pytest.raises(ValueError, match="non-finite"):
        publisher.publish(
            {
                "messageId": "message-1",
                "type": "ExecucaoIniciada",
                "sagaId": "saga-1",
                "osId": non_finite_os_id,
                "requestId": "request-1",
            },
            message_group_id="saga-1",
            message_deduplication_id="message-1",
            message_attributes={},
        )


def test_relay_publishes_execucao_iniciada_loaded_from_dynamodb_decimals():
    repository = DynamoDBExecutionRepository(
        "execution-table",
        FakeDynamoDBClient(),
    )
    record = execution_record()
    response = outbox_message()
    input_id = response.message["causationId"]
    repository.register_command(
        record,
        input_id,
        "body-fingerprint",
        response,
        None,
    )

    class SQS:
        def send_message(self, **kwargs):
            self.kwargs = kwargs
            return {"MessageId": "published"}

    client = SQS()
    relay = OutboxRelay(
        repository,
        SQSFifoMessagePublisher(client, "responses"),
    )

    assert relay.poll_once() == 1
    published = json.loads(client.kwargs["MessageBody"])
    assert published["type"] == "ExecucaoIniciada"
    assert published["osId"] == record.os_id
    assert published["version"] == 1
    assert repository.get_outbox(input_id).published


def test_persistence_failure_prevents_input_acknowledgement(
    consumer_factory,
):
    class FailingRepository(InMemoryExecutionRepository):
        def register_command(self, *args, **kwargs):
            raise RuntimeError("DynamoDB write failed")

    sqs = FakeSQSClient(sqs_message(start_message()))

    consumer, _repository = consumer_factory(
        sqs,
        FailingRepository(),
    )

    assert consumer.poll_once() == 0
    assert sqs.deleted == []


def test_delete_failure_allows_retry_without_duplicate_outbox(
    consumer_factory,
):
    body = start_message()
    sqs = FakeSQSClient(sqs_message(body))
    consumer, repository = consumer_factory(sqs)

    sqs.fail_delete = True

    # A persistência acontece, mas a confirmação da entrada falha.
    assert consumer.poll_once() == 0
    assert sqs.deleted == []

    pending = repository.get_outbox(body["messageId"])
    assert pending is not None

    response_message_id = pending.message["messageId"]

    # A mensagem é recebida novamente.
    sqs.fail_delete = False

    assert consumer.poll_once() == 1
    assert len(sqs.deleted) == 1

    # O processamento idempotente reutiliza o Outbox existente.
    pending_after_retry = repository.get_outbox(body["messageId"])

    assert pending_after_retry is not None
    assert pending_after_retry.message["messageId"] == response_message_id
    assert pending_after_retry.published is False


def test_sqs_publisher_sends_contract_fifo_fields_and_trace_attributes():
    class SQS:
        def send_message(self, **kwargs):
            self.kwargs = kwargs
            return {"MessageId": "sent"}

    client = SQS()
    publisher = SQSFifoMessagePublisher(client, "responses")

    message = {
        "messageId": str(uuid4()),
        "type": "ExecucaoIniciada",
        "sagaId": str(uuid4()),
        "osId": 2,
        "requestId": "request-2",
    }

    attributes = {
        "traceparent": {
            "DataType": "String",
            "StringValue": "trace",
        }
    }

    publisher.publish(
        message,
        message_group_id="saga-uuid",
        message_deduplication_id=message["messageId"],
        message_attributes=attributes,
    )

    assert client.kwargs["MessageGroupId"] == "saga-uuid"
    assert client.kwargs["MessageDeduplicationId"] == message["messageId"]
    assert client.kwargs["MessageAttributes"] == attributes
    assert json.loads(client.kwargs["MessageBody"]) == message


class FakeDynamoDBClient:
    def __init__(self):
        self.items = {}
        self.lock = threading.RLock()
        self.serializer = TypeSerializer()
        self.deserializer = TypeDeserializer()

    def get_item(self, TableName, Key, ConsistentRead):
        del TableName, ConsistentRead

        key = self._decode(Key)

        with self.lock:
            item = self.items.get((key["pk"], key["sk"]))

            return {"Item": self._encode(item)} if item else {}

    def query(self, **kwargs):
        assert kwargs["IndexName"] == "pending-outbox-index"

        expression_values = self._decode(kwargs["ExpressionAttributeValues"])
        pending_value = expression_values[":pending"]

        with self.lock:
            items = [
                self._encode(item)
                for item in self.items.values()
                if item.get("outbox_status") == pending_value
            ]

        return {"Items": items[: kwargs["Limit"]]}

    def transact_write_items(self, TransactItems):
        with self.lock:
            decoded = []

            for operation in TransactItems:
                if "Put" in operation:
                    put = operation["Put"]
                    item = self._decode(put["Item"])
                    key = (item["pk"], item["sk"])
                    old = self.items.get(key)
                    condition = put["ConditionExpression"]
                    names = put.get("ExpressionAttributeNames", {})
                    values = self._decode(put.get("ExpressionAttributeValues", {}))

                    if condition == "attribute_not_exists(pk)" and old is not None:
                        self._transaction_cancelled()
                    if "#version = :expected_version" in condition:
                        if old is None or old.get(names["#version"]) != values[":expected_version"]:
                            self._transaction_cancelled()
                    if "#status = :active" in condition:
                        if old is None or old.get(names["#status"]) != values[":active"]:
                            self._transaction_cancelled()
                    decoded.append((key, item))
                else:
                    check = operation["ConditionCheck"]
                    key_item = self._decode(check["Key"])
                    key = (key_item["pk"], key_item["sk"])
                    old = self.items.get(key)
                    names = check["ExpressionAttributeNames"]
                    values = self._decode(check["ExpressionAttributeValues"])
                    if old is None or old.get(names["#status"]) != values[":active"]:
                        self._transaction_cancelled()

            for key, item in decoded:
                self.items[key] = item

    def update_item(
        self,
        TableName,
        Key,
        UpdateExpression,
        ConditionExpression,
        ExpressionAttributeValues,
    ):
        del TableName

        assert "REMOVE outbox_status, outbox_created_at" in UpdateExpression
        assert ConditionExpression == "attribute_exists(pk)"

        key = self._decode(Key)
        values = self._decode(ExpressionAttributeValues)

        with self.lock:
            item = self.items.get((key["pk"], key["sk"]))

            if item is None:
                raise ClientError(
                    {
                        "Error": {
                            "Code": "ConditionalCheckFailedException",
                            "Message": "missing",
                        }
                    },
                    "UpdateItem",
                )

            item["published"] = values[":published"]
            item["published_at"] = values[":published_at"]
            item.pop("outbox_status", None)
            item.pop("outbox_created_at", None)

    def _encode(self, item):
        return {key: self.serializer.serialize(value) for key, value in item.items()}

    def _decode(self, item):
        return {key: self.deserializer.deserialize(value) for key, value in item.items()}

    @staticmethod
    def _transaction_cancelled():
        raise ClientError(
            {
                "Error": {
                    "Code": "TransactionCanceledException",
                    "Message": "condition failed",
                }
            },
            "TransactWriteItems",
        )


def test_dynamodb_backed_integrated_start_work_completion_and_relay():
    dynamodb = FakeDynamoDBClient()
    repository = DynamoDBExecutionRepository("execution-table", dynamodb)
    command = start_message()
    command["payload"]["servicos"] = [
        {"servicoId": 101, "quantidade": 1},
        {"servicoId": 102, "quantidade": 1},
    ]
    command["payload"]["pecas"] = [
        {"itemPecaOSId": 501, "pecaId": 10, "quantidade": 3},
    ]
    incoming = sqs_message(command)
    incoming["MessageAttributes"] = TRACE_ATTRIBUTES
    consumer_client = FakeSQSClient(incoming)
    consumer = SQSFifoConsumer(
        consumer_client,
        "commands",
        ExecuteCommandUseCase(repository),
    )

    assert consumer.poll_once() == 1
    execution = repository.get_by_saga_id(command["sagaId"])
    assert execution is not None
    start_outbox = repository.get_outbox(command["messageId"])
    assert start_outbox is not None
    assert start_outbox.message["type"] == "ExecucaoIniciada"
    assert repository.get_input_fingerprint(command["messageId"]) is not None
    assert repository.load_lifecycle(command["sagaId"])[1] == 0

    work = ExecutionWorkUseCase(repository)
    started_at = execution.started_at + timedelta(minutes=1)
    work.start_service(
        command["sagaId"],
        101,
        started_at=started_at,
    )
    work.record_consumption(
        command["sagaId"],
        101,
        (PartConsumption(501, 10, 2),),
    )
    work.start_service(
        command["sagaId"],
        102,
        started_at=started_at,
    )
    work.record_consumption(
        command["sagaId"],
        102,
        (PartConsumption(501, 10, 1),),
    )
    work.finish_service(
        command["sagaId"],
        101,
        completed_at=started_at + timedelta(hours=1),
    )
    final = work.finish_service(
        command["sagaId"],
        102,
        completed_at=started_at + timedelta(hours=2),
    )
    assert final.outbox_message is not None

    class SQS:
        def __init__(self):
            self.messages = []

        def send_message(self, **kwargs):
            self.messages.append(kwargs)
            return {"MessageId": f"sqs-{len(self.messages)}"}

    sqs = SQS()
    relay = OutboxRelay(
        repository,
        SQSFifoMessagePublisher(sqs, "responses.fifo"),
    )
    assert relay.poll_once() == 2
    decoded_messages = [json.loads(item["MessageBody"]) for item in sqs.messages]
    started = next(item for item in decoded_messages if item["type"] == "ExecucaoIniciada")
    finished = next(item for item in decoded_messages if item["type"] == "ExecucaoFinalizada")
    assert started["osId"] == command["osId"]
    assert finished["payload"]["pecasConsumidas"] == [
        {"itemPecaOSId": 501, "pecaId": 10, "quantidade": 3}
    ]
    assert len([item for item in decoded_messages if item["type"] == "ExecucaoFinalizada"]) == 1
    assert all(
        item["MessageGroupId"] == command["sagaId"]
        and item["MessageDeduplicationId"] == decoded["messageId"]
        for item, decoded in zip(sqs.messages, decoded_messages)
    )
    assert all(
        item["MessageAttributes"]["traceparent"]["DataType"] == "String" for item in sqs.messages
    )
    assert all(
        item["MessageAttributes"]["tracestate"]["StringValue"]
        == TRACE_ATTRIBUTES["tracestate"]["StringValue"]
        for item in sqs.messages
    )
    assert repository.get_by_saga_id(command["sagaId"]).status.value == "CONCLUIDA"


def test_dynamodb_repository_atomically_persists_execution_inbox_and_outbox():
    client = FakeDynamoDBClient()
    repository = DynamoDBExecutionRepository(
        "execution-table",
        client,
    )

    record = execution_record()
    response = outbox_message()
    input_id = str(uuid4())

    assert repository.register_command(
        record,
        input_id,
        "body-hash",
        response,
        None,
    )

    saved = repository.get_by_saga_id(record.saga_id)
    pending = repository.get_outbox(input_id)

    assert saved is not None
    assert saved.execution_id == record.execution_id
    assert repository.get_input_fingerprint(input_id) == "body-hash"
    assert pending is not None
    assert pending.published is False

    repository.mark_outbox_published(input_id)

    assert repository.get_outbox(input_id).published is True
    assert repository.get_pending_outbox() == []
    assert len(client.items) == 3


def test_outbox_relay_recovers_pending_response_after_service_restart():
    client = FakeDynamoDBClient()
    repository = DynamoDBExecutionRepository(
        "execution-table",
        client,
    )

    record = execution_record()
    pending_response = outbox_message()
    input_id = str(uuid4())

    repository.register_command(
        record,
        input_id,
        "body-fingerprint",
        pending_response,
        None,
    )

    restarted_repository = DynamoDBExecutionRepository(
        "execution-table",
        client,
    )

    publisher = FakeMessagePublisher()
    relay = OutboxRelay(restarted_repository, publisher)

    assert relay.poll_once() == 1
    assert len(publisher.messages) == 1

    published = publisher.messages[0]

    assert published["message"] == pending_response.message
    assert published["message_group_id"] == pending_response.message["sagaId"]
    assert published["message_deduplication_id"] == pending_response.message["messageId"]

    assert restarted_repository.get_pending_outbox() == []


def test_outbox_relay_retry_keeps_failed_item_pending():
    client = FakeDynamoDBClient()
    repository = DynamoDBExecutionRepository(
        "execution-table",
        client,
    )

    record = execution_record()
    response = outbox_message()
    input_id = str(uuid4())

    repository.register_command(
        record,
        input_id,
        "body-fingerprint",
        response,
        None,
    )

    class FailOncePublisher(FakeMessagePublisher):
        def __init__(self):
            super().__init__()
            self.fail = True

        def publish(self, message, **kwargs):
            if self.fail:
                self.fail = False
                raise RuntimeError("response queue unavailable")

            return super().publish(message, **kwargs)

    publisher = FailOncePublisher()
    relay = OutboxRelay(repository, publisher)

    assert relay.poll_once() == 0
    assert repository.get_pending_outbox()[0].input_message_id == input_id

    assert relay.poll_once() == 1
    assert repository.get_pending_outbox() == []


def test_dynamodb_repository_enforces_atomic_create_and_idempotency_key():
    client = FakeDynamoDBClient()
    repository = DynamoDBExecutionRepository(
        "execution-table",
        client,
    )

    record = execution_record()
    input_id = str(uuid4())

    repository.register_command(
        record,
        input_id,
        "same-body",
        outbox_message(),
        None,
    )

    assert not repository.register_command(
        record,
        input_id,
        "same-body",
        outbox_message(),
        None,
    )

    conflicting_record = execution_record(record.saga_id)
    conflicting_input_id = str(uuid4())
    conflicting_outbox = outbox_message()
    with pytest.raises(ConcurrentExecutionUpdateError):
        repository.register_command(
            conflicting_record,
            conflicting_input_id,
            "another-body",
            conflicting_outbox,
            None,
        )

    mismatched_outbox = outbox_message()
    with pytest.raises(IdempotencyKeyReuseError):
        repository.register_command(
            record,
            input_id,
            "different-body",
            mismatched_outbox,
            None,
        )


def test_dynamodb_transaction_prevents_concurrent_duplicate_execution_creation():
    client = FakeDynamoDBClient()
    repository = DynamoDBExecutionRepository(
        "execution-table",
        client,
    )

    record = execution_record()
    input_id = str(uuid4())
    response = outbox_message()

    def register_command(_index):
        return repository.register_command(
            record,
            input_id,
            "same-body",
            response,
            None,
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(register_command, range(8)))

    assert sum(results) == 1
    assert len(client.items) == 3


def test_json_log_formatter_includes_correlation_fields():
    record = logging.LogRecord(
        name="execution",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="command processed",
        args=(),
        exc_info=None,
    )

    record.request_id = "request-id"
    record.saga_id = "saga-id"

    formatted = json.loads(JsonLogFormatter().format(record))

    assert formatted["message"] == "command processed"
    assert formatted["request_id"] == "request-id"
    assert formatted["saga_id"] == "saga-id"
