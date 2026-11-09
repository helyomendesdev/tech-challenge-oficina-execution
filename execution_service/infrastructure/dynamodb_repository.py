from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import boto3
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError

from execution_service.application.dto import OutboxMessage, ServiceCommandReceipt
from execution_service.application.ports import ExecutionRepositoryPort
from execution_service.domain.enums import ExecutionStatus
from execution_service.domain.exceptions import (
    ConcurrentExecutionUpdateError,
    IdempotencyKeyReuseError,
    TransitionNotAllowedError,
)
from execution_service.domain.models import ExecutionRecord
from execution_service.domain.service_execution import ExecutionLifecycle, ServiceStatus

ATTRIBUTE_NOT_EXISTS_PK_CONDITION = "attribute_not_exists(pk)"
VERSION_ATTRIBUTE_NAME = "#version"
STATUS_ATTRIBUTE_NAME = "#status"
EXPECTED_VERSION_VALUE = ":expected_version"
ACTIVE_STATUS_VALUE = ":active"
VERSION_MATCH_CONDITION = f"{VERSION_ATTRIBUTE_NAME} = {EXPECTED_VERSION_VALUE}"
ACTIVE_STATUS_CONDITION = f"{STATUS_ATTRIBUTE_NAME} = {ACTIVE_STATUS_VALUE}"
VERSION_AND_ACTIVE_STATUS_CONDITION = f"{VERSION_MATCH_CONDITION} AND {ACTIVE_STATUS_CONDITION}"


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
        lifecycle: ExecutionLifecycle | None = None,
        expected_lifecycle_version: int | None = None,
    ) -> bool:
        operations = self._command_operations(
            record,
            input_message_id,
            input_fingerprint,
            response,
            expected_version,
        )
        lifecycle_put = self._lifecycle_put(
            record.saga_id,
            lifecycle,
            expected_lifecycle_version,
        )
        if lifecycle_put is not None:
            operations.append({"Put": lifecycle_put})

        try:
            self.client.transact_write_items(TransactItems=operations)
        except ClientError as exc:
            if self._handle_register_command_error(
                exc,
                input_message_id,
                input_fingerprint,
            ):
                return False
            raise
        return True

    def _command_operations(
        self,
        record: ExecutionRecord,
        input_message_id: str,
        input_fingerprint: str,
        response: OutboxMessage,
        expected_version: int | None,
    ) -> list[dict[str, Any]]:
        message_key = f"MSG#{input_message_id}"
        idempotency_item = {
            "pk": message_key,
            "sk": "IDEMPOTENCY",
            "entity_type": "IDEMPOTENCY",
            "input_fingerprint": input_fingerprint,
            "saga_id": record.saga_id,
        }
        return [
            {"Put": self._record_put(record, expected_version)},
            {
                "Put": {
                    "TableName": self.table_name,
                    "Item": self._serialize_item(idempotency_item),
                    "ConditionExpression": ATTRIBUTE_NOT_EXISTS_PK_CONDITION,
                }
            },
            {
                "Put": {
                    "TableName": self.table_name,
                    "Item": self._serialize_item(self._outbox_item(input_message_id, response)),
                    "ConditionExpression": ATTRIBUTE_NOT_EXISTS_PK_CONDITION,
                }
            },
        ]

    def _record_put(
        self,
        record: ExecutionRecord,
        expected_version: int | None,
    ) -> dict[str, Any]:
        record_condition = (
            ATTRIBUTE_NOT_EXISTS_PK_CONDITION
            if expected_version is None
            else VERSION_MATCH_CONDITION
        )
        put: dict[str, Any] = {
            "TableName": self.table_name,
            "Item": self._serialize_item(self._record_item(record)),
            "ConditionExpression": record_condition,
        }
        if expected_version is not None:
            put["ExpressionAttributeNames"] = {VERSION_ATTRIBUTE_NAME: "version"}
            put["ExpressionAttributeValues"] = self._serialize_item(
                {EXPECTED_VERSION_VALUE: expected_version}
            )
        return put

    def _lifecycle_put(
        self,
        saga_id: str,
        lifecycle: ExecutionLifecycle | None,
        expected_lifecycle_version: int | None,
    ) -> dict[str, Any] | None:
        if lifecycle is None:
            if expected_lifecycle_version is not None:
                raise ValueError("expected_lifecycle_version requires a lifecycle")
            return None

        lifecycle_put: dict[str, Any] = {
            "TableName": self.table_name,
            "Item": self._serialize_item(
                self._lifecycle_item(
                    saga_id,
                    lifecycle,
                    0 if expected_lifecycle_version is None else expected_lifecycle_version + 1,
                )
            ),
            "ConditionExpression": (
                ATTRIBUTE_NOT_EXISTS_PK_CONDITION
                if expected_lifecycle_version is None
                else VERSION_MATCH_CONDITION
            ),
        }
        if expected_lifecycle_version is not None:
            lifecycle_put["ExpressionAttributeNames"] = {VERSION_ATTRIBUTE_NAME: "version"}
            lifecycle_put["ExpressionAttributeValues"] = self._serialize_item(
                {EXPECTED_VERSION_VALUE: expected_lifecycle_version}
            )
        return lifecycle_put

    def _handle_register_command_error(
        self,
        error: ClientError,
        input_message_id: str,
        input_fingerprint: str,
    ) -> bool:
        if error.response.get("Error", {}).get("Code") not in {
            "TransactionCanceledException",
            "ConditionalCheckFailedException",
        }:
            return False
        previous = self.get_input_fingerprint(input_message_id)
        if previous is not None:
            if previous != input_fingerprint:
                raise IdempotencyKeyReuseError(
                    f"messageId={input_message_id} was reused with a different body"
                ) from error
            return True
        raise ConcurrentExecutionUpdateError(
            "Concurrent execution state change; retry the command"
        ) from error

    def load_lifecycle(self, saga_id: str) -> tuple[ExecutionLifecycle, int] | None:
        item = self._get_item(f"EXEC#{saga_id}", "LIFECYCLE")
        if item is None:
            return None
        lifecycle_state = item.get("lifecycle")
        if not isinstance(lifecycle_state, dict):
            raise RuntimeError(f"Persisted lifecycle is invalid for sagaId={saga_id}")
        return ExecutionLifecycle.from_state(lifecycle_state), int(item["version"])

    def get_service_command_receipt(self, message_id: str) -> ServiceCommandReceipt | None:
        item = self._get_item(f"MSG#{message_id}", "SERVICE_COMMAND")
        if item is None:
            return None
        outbox = None
        if item.get("has_outbox"):
            outbox = self.get_outbox(message_id)
            if outbox is None:
                raise RuntimeError(f"Service outbox record missing for messageId={message_id}")
        return ServiceCommandReceipt(
            fingerprint=item["input_fingerprint"],
            status=ServiceStatus(item["status"]),
            outbox_message=outbox,
        )

    def commit_service_command(
        self,
        lifecycle: ExecutionLifecycle,
        message_id: str,
        fingerprint: str,
        status: ServiceStatus,
        outbox_message: OutboxMessage | None,
        expected_version: int,
        execution_status: ExecutionStatus | None = None,
    ) -> bool:
        lifecycle_key = {"pk": f"EXEC#{lifecycle.saga_id}", "sk": "LIFECYCLE"}
        stored_lifecycle = self._get_item(
            lifecycle_key["pk"],
            lifecycle_key["sk"],
        )
        if stored_lifecycle is None:
            raise RuntimeError(f"Execution lifecycle missing for sagaId={lifecycle.saga_id}")
        record = self.get_by_saga_id(lifecycle.saga_id)
        if record is None:
            raise RuntimeError(f"Execution record missing for sagaId={lifecycle.saga_id}")
        if record.status is not ExecutionStatus.EM_EXECUCAO:
            raise TransitionNotAllowedError("Execution is no longer accepting service changes")

        operations: list[dict[str, Any]] = [
            {
                "Put": {
                    "TableName": self.table_name,
                    "Item": self._serialize_item(
                        self._lifecycle_item(
                            lifecycle.saga_id,
                            lifecycle,
                            expected_version + 1,
                        )
                    ),
                    "ConditionExpression": VERSION_MATCH_CONDITION,
                    "ExpressionAttributeNames": {VERSION_ATTRIBUTE_NAME: "version"},
                    "ExpressionAttributeValues": self._serialize_item(
                        {EXPECTED_VERSION_VALUE: expected_version}
                    ),
                }
            },
            {
                "Put": {
                    "TableName": self.table_name,
                    "Item": self._serialize_item(
                        {
                            "pk": f"MSG#{message_id}",
                            "sk": "SERVICE_COMMAND",
                            "entity_type": "SERVICE_COMMAND",
                            "input_fingerprint": fingerprint,
                            "status": status.value,
                            "has_outbox": outbox_message is not None,
                        }
                    ),
                    "ConditionExpression": ATTRIBUTE_NOT_EXISTS_PK_CONDITION,
                }
            },
        ]

        if outbox_message is None:
            operations.append(
                {
                    "ConditionCheck": {
                        "TableName": self.table_name,
                        "Key": self._serialize_item(
                            {"pk": f"EXEC#{lifecycle.saga_id}", "sk": "RECORD"}
                        ),
                        "ConditionExpression": ACTIVE_STATUS_CONDITION,
                        "ExpressionAttributeNames": {STATUS_ATTRIBUTE_NAME: "status"},
                        "ExpressionAttributeValues": self._serialize_item(
                            {ACTIVE_STATUS_VALUE: ExecutionStatus.EM_EXECUCAO.value}
                        ),
                    }
                }
            )
        else:
            record.status = execution_status or ExecutionStatus.CONCLUIDA
            record.updated_at = datetime.now(timezone.utc)
            record.version += 1
            operations.append(
                {
                    "Put": {
                        "TableName": self.table_name,
                        "Item": self._serialize_item(self._record_item(record)),
                        "ConditionExpression": VERSION_AND_ACTIVE_STATUS_CONDITION,
                        "ExpressionAttributeNames": {
                            VERSION_ATTRIBUTE_NAME: "version",
                            STATUS_ATTRIBUTE_NAME: "status",
                        },
                        "ExpressionAttributeValues": self._serialize_item(
                            {
                                EXPECTED_VERSION_VALUE: record.version - 1,
                                ACTIVE_STATUS_VALUE: ExecutionStatus.EM_EXECUCAO.value,
                            }
                        ),
                    }
                }
            )
            operations.append(
                {
                    "Put": {
                        "TableName": self.table_name,
                        "Item": self._serialize_item(self._outbox_item(message_id, outbox_message)),
                        "ConditionExpression": ATTRIBUTE_NOT_EXISTS_PK_CONDITION,
                    }
                }
            )

        try:
            self.client.transact_write_items(TransactItems=operations)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code not in {"TransactionCanceledException", "ConditionalCheckFailedException"}:
                raise
            receipt = self.get_service_command_receipt(message_id)
            if receipt is not None:
                if receipt.fingerprint != fingerprint:
                    raise IdempotencyKeyReuseError(
                        f"messageId={message_id} was reused with a different body"
                    ) from exc
                return False
            raise ConcurrentExecutionUpdateError(
                "Concurrent service execution state change; retry the command"
            ) from exc
        return True

    def register_failure_response(
        self,
        input_message_id: str,
        input_fingerprint: str,
        response: OutboxMessage,
    ) -> bool:
        message_key = f"MSG#{input_message_id}"
        saga_id = response.message["sagaId"]
        operations = [
            {
                "Put": {
                    "TableName": self.table_name,
                    "Item": self._serialize_item(
                        {
                            "pk": message_key,
                            "sk": "IDEMPOTENCY",
                            "entity_type": "IDEMPOTENCY",
                            "input_fingerprint": input_fingerprint,
                            "saga_id": saga_id,
                        }
                    ),
                    "ConditionExpression": ATTRIBUTE_NOT_EXISTS_PK_CONDITION,
                }
            },
            {
                "Put": {
                    "TableName": self.table_name,
                    "Item": self._serialize_item(self._outbox_item(input_message_id, response)),
                    "ConditionExpression": ATTRIBUTE_NOT_EXISTS_PK_CONDITION,
                }
            },
            {
                "ConditionCheck": {
                    "TableName": self.table_name,
                    "Key": self._serialize_item({"pk": f"EXEC#{saga_id}", "sk": "RECORD"}),
                    "ConditionExpression": ATTRIBUTE_NOT_EXISTS_PK_CONDITION,
                }
            },
        ]
        try:
            self.client.transact_write_items(TransactItems=operations)
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
            raise ConcurrentExecutionUpdateError(
                "Execution has already been accepted for this saga"
            ) from exc
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
            "cancellation_reason": record.cancellation_reason,
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
            cancellation_reason=item.get("cancellation_reason"),
        )

    @staticmethod
    def _lifecycle_item(
        saga_id: str,
        lifecycle: ExecutionLifecycle,
        version: int,
    ) -> dict[str, Any]:
        return {
            "pk": f"EXEC#{saga_id}",
            "sk": "LIFECYCLE",
            "entity_type": "EXECUTION_LIFECYCLE",
            "version": version,
            "lifecycle": lifecycle.to_state(),
        }

    @staticmethod
    def _outbox_item(input_message_id: str, response: OutboxMessage) -> dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        return {
            "pk": f"OUTBOX#{input_message_id}",
            "sk": "EVENT",
            "entity_type": "OUTBOX",
            "schema_version": 1,
            "outbox_status": "PENDING",
            "outbox_created_at": now + f"#{input_message_id}",
            "input_message_id": input_message_id,
            "published": False,
            "message": response.message,
            "message_attributes": response.message_attributes,
            "created_at": now,
        }
