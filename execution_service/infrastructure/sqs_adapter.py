from __future__ import annotations

import json
import logging
from decimal import Decimal
from typing import Any

from execution_service.application.ports import MessagePublisherPort

logger = logging.getLogger(__name__)


class SQSFifoMessagePublisher(MessagePublisherPort):
    def __init__(self, client: Any, queue_url: str):
        if not queue_url:
            raise ValueError("SAGA_RESPONSES_QUEUE_URL must be configured")
        self.client = client
        self.queue_url = queue_url

    def publish(
        self,
        message: dict[str, Any],
        *,
        message_group_id: str,
        message_deduplication_id: str,
        message_attributes: dict[str, dict[str, str]],
    ) -> None:
        response = self.client.send_message(
            QueueUrl=self.queue_url,
            MessageBody=_dumps_message(message),
            MessageGroupId=message_group_id,
            MessageDeduplicationId=message_deduplication_id,
            MessageAttributes=message_attributes,
        )
        logger.info(
            "execution_response_published",
            extra={
                "message_id": message["messageId"],
                "message_type": message["type"],
                "saga_id": message["sagaId"],
                "os_id": message["osId"],
                "request_id": message["requestId"],
                "sqs_message_id": response.get("MessageId"),
            },
        )


def _dumps_message(value: Any) -> str:
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("SQS message cannot contain a non-finite Decimal")
        if value == value.to_integral_value():
            return str(int(value))
        return format(value, "f")
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("SQS message object keys must be strings")
        entries = (
            f"{json.dumps(key, ensure_ascii=False)}:{_dumps_message(item)}"
            for key, item in value.items()
        )
        return "{" + ",".join(entries) + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_dumps_message(item) for item in value) + "]"
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


class FakeMessagePublisher(MessagePublisherPort):
    def __init__(self):
        self.messages: list[dict[str, Any]] = []

    def publish(
        self,
        message: dict[str, Any],
        *,
        message_group_id: str,
        message_deduplication_id: str,
        message_attributes: dict[str, dict[str, str]],
    ) -> None:
        self.messages.append(
            {
                "message": message,
                "message_group_id": message_group_id,
                "message_deduplication_id": message_deduplication_id,
                "message_attributes": message_attributes,
            }
        )
