from __future__ import annotations

import json
import logging
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
            MessageBody=json.dumps(message, ensure_ascii=False, separators=(",", ":")),
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
