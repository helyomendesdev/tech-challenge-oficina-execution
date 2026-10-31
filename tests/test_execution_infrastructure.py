from __future__ import annotations

import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError

from execution_service.domain.exceptions import IdempotencyKeyReuseError
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
from execution_service.logging_config import JsonLogFormatter
from tests.factories import (
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


def test_unsupported_cancellation_is_not_persisted_or_acknowledged(
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

    assert consumer.poll_once() == 0

    assert len(sqs.deleted) == 1

    assert repository.get_by_saga_id(start["sagaId"]).status.value == "EM_EXECUCAO"

    assert repository.get_input_fingerprint(cancellation["messageId"]) is None


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
                put = operation["Put"]
                item = self._decode(put["Item"])
                key = (item["pk"], item["sk"])
                old = self.items.get(key)
                condition = put["ConditionExpression"]

                if condition == "attribute_not_exists(pk)" and old is not None:
                    self._transaction_cancelled()

                if condition == "#version = :expected_version":
                    names = put["ExpressionAttributeNames"]
                    values = self._decode(put["ExpressionAttributeValues"])

                    if old is None or old[names["#version"]] != values[":expected_version"]:
                        self._transaction_cancelled()

                decoded.append((key, item))

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

    with pytest.raises(ClientError):
        repository.register_command(
            execution_record(record.saga_id),
            str(uuid4()),
            "another-body",
            outbox_message(),
            None,
        )

    with pytest.raises(IdempotencyKeyReuseError):
        repository.register_command(
            record,
            input_id,
            "different-body",
            outbox_message(),
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
