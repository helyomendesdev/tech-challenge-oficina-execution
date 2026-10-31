from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import boto3
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError

from execution_service.application.dto import OutboxMessage
from execution_service.application.ports import ExecutionRepositoryPort
from execution_service.domain.enums import ExecutionStatus
from execution_service.domain.exceptions import IdempotencyKeyReuseError
from execution_service.domain.models import ExecutionRecord


class DynamoDBExecutionRepository(ExecutionRepositoryPort):
    """Persists execution state, inbox keys, and outbox messages atomically."""

    def __init__(self, table_name: str | None = None, client: Any | None = None):
        self.table_name = table_name or os.getenv("EXECUTION_DYNAMODB_TABLE")
        if not self.table_name:
            raise ValueError("EXECUTION_DYNAMODB_TABLE must be configured")
        self.client = client or boto3.client(
            "dynamodb", region_name=os.getenv("AWS_REGION", "us-east-1")
        )
        self._serializer = TypeSerializer()
        self._deserializer = TypeDeserializer()

    def get_by_saga_id(self, saga_id: str) -> ExecutionRecord | None:
        item = self._get_item(f"EXEC#{saga_id}", "RECORD")
        return self._record_from_item(item) if item else None

    def get_input_fingerprint(self, message_id: str) -> str | None:
        item = self._get_item(f"MSG#{message_id}", "IDEMPOTENCY")
        return item.get("input_fingerprint") if item else None

    def register_command(
        self,
        record: ExecutionRecord,
        input_message_id: str,
        input_fingerprint: str,
        response: OutboxMessage,
        expected_version: int | None,
    ) -> bool:
        message_key = f"MSG#{input_message_id}"
        outbox_key = f"OUTBOX#{input_message_id}"
        record_item = self._record_item(record)
        idempotency_item = {
            "pk": message_key,
            "sk": "IDEMPOTENCY",
            "entity_type": "IDEMPOTENCY",
            "input_fingerprint": input_fingerprint,
            "saga_id": record.saga_id,
        }
        outbox_item = {
            "pk": outbox_key,
            "sk": "EVENT",
            "entity_type": "OUTBOX",
            "schema_version": 1,
            "outbox_status": "PENDING",
            "outbox_created_at": datetime.now(timezone.utc).isoformat() + f"#{input_message_id}",
            "input_message_id": input_message_id,
            "published": False,
            "message": response.message,
            "message_attributes": response.message_attributes,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        record_condition = (
            "attribute_not_exists(pk)"
            if expected_version is None
            else "#version = :expected_version"
        )
        record_put: dict[str, Any] = {
            "TableName": self.table_name,
            "Item": self._serialize_item(record_item),
            "ConditionExpression": record_condition,
        }
        if expected_version is not None:
            record_put["ExpressionAttributeNames"] = {"#version": "version"}
            record_put["ExpressionAttributeValues"] = self._serialize_item(
                {":expected_version": expected_version}
            )

        try:
            self.client.transact_write_items(
                TransactItems=[
                    {"Put": record_put},
                    {
                        "Put": {
                            "TableName": self.table_name,
                            "Item": self._serialize_item(idempotency_item),
                            "ConditionExpression": "attribute_not_exists(pk)",
                        }
                    },
                    {
                        "Put": {
                            "TableName": self.table_name,
                            "Item": self._serialize_item(outbox_item),
                            "ConditionExpression": "attribute_not_exists(pk)",
                        }
                    },
                ]
            )
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") not in {
                "TransactionCanceledException",
                "ConditionalCheckFailedException",
            }:
                raise
            previous = self.get_input_fingerprint(input_message_id)
            if previous is not None:
                if previous != input_fingerprint:
                    raise IdempotencyKeyReuseError(
                        f"messageId={input_message_id} was reused with a different body"
                    ) from exc
                return False
            raise
        return True

    def get_outbox(self, input_message_id: str) -> OutboxMessage | None:
        item = self._get_item(f"OUTBOX#{input_message_id}", "EVENT")
        if item is None:
            return None
        return OutboxMessage(
            message=item["message"],
            message_attributes=item["message_attributes"],
            published=bool(item["published"]),
            input_message_id=input_message_id,
        )

    def get_pending_outbox(self, limit: int = 10) -> list[OutboxMessage]:
        if limit < 1:
            raise ValueError("limit must be greater than zero")
        response = self.client.query(
            TableName=self.table_name,
            IndexName="pending-outbox-index",
            KeyConditionExpression="#outbox_status = :pending",
            ExpressionAttributeNames={"#outbox_status": "outbox_status"},
            ExpressionAttributeValues=self._serialize_item({":pending": "PENDING"}),
            Limit=limit,
        )
        return [
            OutboxMessage(
                message=item["message"],
                message_attributes=item["message_attributes"],
                published=False,
                input_message_id=item["input_message_id"],
            )
            for item in map(self._deserialize_item, response.get("Items", []))
        ]

    def mark_outbox_published(self, input_message_id: str) -> None:
        try:
            self.client.update_item(
                TableName=self.table_name,
                Key=self._serialize_item({"pk": f"OUTBOX#{input_message_id}", "sk": "EVENT"}),
                UpdateExpression=(
                    "SET published = :published, published_at = :published_at "
                    "REMOVE outbox_status, outbox_created_at"
                ),
                ConditionExpression="attribute_exists(pk)",
                ExpressionAttributeValues=self._serialize_item(
                    {
                        ":published": True,
                        ":published_at": datetime.now(timezone.utc).isoformat(),
                    }
                ),
            )
        except ClientError:
            item = self._get_item(f"OUTBOX#{input_message_id}", "EVENT")
            if item is None or not item.get("published"):
                raise

    def _get_item(self, pk: str, sk: str) -> dict[str, Any] | None:
        response = self.client.get_item(
            TableName=self.table_name,
            Key=self._serialize_item({"pk": pk, "sk": sk}),
            ConsistentRead=True,
        )
        item = response.get("Item")
        return self._deserialize_item(item) if item else None

    def _serialize_item(self, item: dict[str, Any]) -> dict[str, Any]:
        return {key: self._serializer.serialize(value) for key, value in item.items()}

    def _deserialize_item(self, item: dict[str, Any]) -> dict[str, Any]:
        return {key: self._deserializer.deserialize(value) for key, value in item.items()}

    @staticmethod
    def _record_item(record: ExecutionRecord) -> dict[str, Any]:
        return {
            "pk": f"EXEC#{record.saga_id}",
            "sk": "RECORD",
            "entity_type": "EXECUTION",
            "schema_version": 1,
            "execution_id": record.execution_id,
            "saga_id": record.saga_id,
            "os_id": record.os_id,
            "budget_id": record.budget_id,
            "services": record.services,
            "parts": record.parts,
            "status": record.status.value,
            "started_at": record.started_at.isoformat(),
            "created_at": record.created_at.isoformat(),
            "updated_at": record.updated_at.isoformat(),
            "version": record.version,
        }

    @staticmethod
    def _record_from_item(item: dict[str, Any]) -> ExecutionRecord:
        return ExecutionRecord(
            execution_id=item["execution_id"],
            saga_id=item["saga_id"],
            os_id=int(item["os_id"]),
            budget_id=item["budget_id"],
            services=item["services"],
            parts=item["parts"],
            status=ExecutionStatus(item["status"]),
            started_at=datetime.fromisoformat(item["started_at"]),
            created_at=datetime.fromisoformat(item["created_at"]),
            updated_at=datetime.fromisoformat(item["updated_at"]),
            version=int(item["version"]),
        )
