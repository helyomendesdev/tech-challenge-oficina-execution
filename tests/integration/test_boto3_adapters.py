from __future__ import annotations

import json

import boto3
from botocore.stub import Stubber

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

    with Stubber(client) as stubber:
        stubber.add_response("transact_write_items", {})
        stubber.add_response("query", {"Items": []})
        repository.register_command(
            record=record,
            input_message_id=input_message_id,
            input_fingerprint="fingerprint",
            response=response,
            expected_version=None,
        )
        assert repository.get_pending_outbox() == []
        stubber.assert_no_pending_responses()
