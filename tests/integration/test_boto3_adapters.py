from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import boto3
from botocore.stub import Stubber

from execution_service.application.dto import OutboxMessage
from execution_service.domain.service_execution import (
    ExecutionLifecycle,
    PartConsumption,
    ServiceStatus,
)
from execution_service.infrastructure.dynamodb_repository import DynamoDBExecutionRepository
from execution_service.infrastructure.sqs_adapter import SQSFifoMessagePublisher
from tests.factories import execution_record, outbox_message


def boto_client(service_name):
    return boto3.client(
        service_name,
        region_name="us-east-1",
        aws_access_key_id="test-access-key",
        aws_secret_access_key="test-secret-key",
    )


def test_sqs_publisher_uses_fifo_and_message_attribute_api_shape():
    client = boto_client("sqs")
    response = outbox_message()
    publisher = SQSFifoMessagePublisher(
        client, "https://sqs.us-east-1.amazonaws.com/1/responses.fifo"
    )
    expected_body = json.dumps(
        response.message,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    expected_parameters = {
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/1/responses.fifo",
        "MessageBody": expected_body,
        "MessageGroupId": response.message["sagaId"],
        "MessageDeduplicationId": response.message["messageId"],
        "MessageAttributes": response.message_attributes,
    }

    with Stubber(client) as stubber:
        stubber.add_response(
            "send_message",
            {
                "MessageId": "sqs-response-id",
                "MD5OfMessageBody": "0" * 32,
                "SequenceNumber": "123456789",
            },
            expected_parameters,
        )
        publisher.publish(
            response.message,
            message_group_id=response.message["sagaId"],
            message_deduplication_id=response.message["messageId"],
            message_attributes=response.message_attributes,
        )
        stubber.assert_no_pending_responses()


def test_dynamodb_repository_uses_atomic_transaction_and_pending_outbox_index():
    client = boto_client("dynamodb")
    repository = DynamoDBExecutionRepository("execution-service-test", client)
    record = execution_record()
    response = outbox_message()
    input_message_id = response.message["causationId"]
    lifecycle = ExecutionLifecycle(
        os_id=record.os_id,
        saga_id=record.saga_id,
        execution_id=record.execution_id,
        service_ids=[service["servicoId"] for service in record.services],
        reserved_parts=(PartConsumption(701, 7, 2),),
    )
    captured = []
    client.meta.events.register_first(
        "before-parameter-build.dynamodb.TransactWriteItems",
        lambda params, **_kwargs: captured.append(params),
    )

    with Stubber(client) as stubber:
        stubber.add_response("transact_write_items", {})
        stubber.add_response("query", {"Items": []})
        repository.register_command(
            record=record,
            input_message_id=input_message_id,
            input_fingerprint="fingerprint",
            response=response,
            expected_version=None,
            lifecycle=lifecycle,
        )
        assert len(captured[0]["TransactItems"]) == 4
        assert all("Put" in operation for operation in captured[0]["TransactItems"])
        assert repository.get_pending_outbox() == []
        stubber.assert_no_pending_responses()


def test_dynamodb_service_completion_commits_lifecycle_receipt_execution_and_outbox_together():
    client = boto_client("dynamodb")
    repository = DynamoDBExecutionRepository("execution-service-test", client)
    record = execution_record()
    lifecycle = ExecutionLifecycle(
        os_id=record.os_id,
        saga_id=record.saga_id,
        execution_id=record.execution_id,
        service_ids=[2],
        reserved_parts=(PartConsumption(701, 7, 2),),
    )
    started_at = datetime(2026, 11, 1, 10, tzinfo=timezone.utc)
    lifecycle.start_service(
        2,
        "a122f6d5-6dca-49d2-b94e-5e8a9ab9499a",
        started_at,
    )
    lifecycle.record_service_consumption(
        2,
        "a122f6d5-6dca-49d2-b94e-5e8a9ab9499c",
        (PartConsumption(701, 7, 1),),
    )
    lifecycle.finish_service(
        2,
        "a122f6d5-6dca-49d2-b94e-5e8a9ab9499b",
        started_at + timedelta(hours=1),
    )
    source = outbox_message()
    response = OutboxMessage(
        message={
            **source.message,
            "type": "ExecucaoFinalizada",
            "payload": {
                "execucaoId": record.execution_id,
                "finalizadaEm": (started_at + timedelta(hours=1)).isoformat(),
                "pecasConsumidas": [{"itemPecaOSId": 701, "pecaId": 7, "quantidade": 1}],
            },
        },
        message_attributes=source.message_attributes,
    )
    command_id = "a122f6d5-6dca-49d2-b94e-5e8a9ab9499b"
    captured = []
    client.meta.events.register_first(
        "before-parameter-build.dynamodb.TransactWriteItems",
        lambda params, **_kwargs: captured.append(params),
    )

    with Stubber(client) as stubber:
        stubber.add_response(
            "get_item",
            {
                "Item": repository._serialize_item(
                    repository._lifecycle_item(record.saga_id, lifecycle, 4)
                )
            },
            {
                "TableName": "execution-service-test",
                "Key": repository._serialize_item(
                    {"pk": f"EXEC#{record.saga_id}", "sk": "LIFECYCLE"}
                ),
                "ConsistentRead": True,
            },
        )
        stubber.add_response(
            "get_item",
            {"Item": repository._serialize_item(repository._record_item(record))},
            {
                "TableName": "execution-service-test",
                "Key": repository._serialize_item({"pk": f"EXEC#{record.saga_id}", "sk": "RECORD"}),
                "ConsistentRead": True,
            },
        )
        stubber.add_response("transact_write_items", {})

        assert repository.commit_service_command(
            lifecycle,
            command_id,
            "service-fingerprint",
            ServiceStatus.CONCLUIDO,
            response,
            expected_version=4,
        )
        operations = captured[0]["TransactItems"]
        assert len(operations) == 4
        assert operations[0]["Put"]["ConditionExpression"] == "#version = :expected_version"
        assert operations[1]["Put"]["ConditionExpression"] == "attribute_not_exists(pk)"
        assert (
            operations[2]["Put"]["ConditionExpression"]
            == "#version = :expected_version AND #status = :active"
        )
        assert operations[3]["Put"]["ConditionExpression"] == "attribute_not_exists(pk)"
        assert (
            repository._deserialize_item(operations[3]["Put"]["Item"])["message"]["payload"]
            == response.message["payload"]
        )
        stubber.assert_no_pending_responses()
