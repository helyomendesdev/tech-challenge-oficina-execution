from __future__ import annotations

import json
import logging
from threading import Event
from typing import Any

from execution_service.application.use_cases import ExecuteCommandUseCase

logger = logging.getLogger(__name__)


class SQSFifoConsumer:
    def __init__(
        self,
        client: Any,
        queue_url: str,
        processor: ExecuteCommandUseCase,
        visibility_timeout: int = 30,
        wait_time_seconds: int = 20,
    ):
        if not queue_url:
            raise ValueError("EXECUTION_COMMANDS_QUEUE_URL must be configured")
        if not 0 < visibility_timeout <= 43200:
            raise ValueError("visibility_timeout must be between 1 and 43200 seconds")
        if not 0 <= wait_time_seconds <= 20:
            raise ValueError("wait_time_seconds must be between 0 and 20 seconds")

        self.client = client
        self.queue_url = queue_url
        self.processor = processor
        self.visibility_timeout = visibility_timeout
        self.wait_time_seconds = wait_time_seconds

    def poll_once(self) -> int:
        response = self.client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=self.wait_time_seconds,
            VisibilityTimeout=self.visibility_timeout,
            MessageAttributeNames=["All"],
            AttributeNames=["ApproximateReceiveCount"],
        )

        confirmed = 0

        for message in response.get("Messages", []):
            receipt_handle = message.get("ReceiptHandle")
            message_id = message.get("MessageId", "unknown")
            body: Any = {}

            try:
                if not receipt_handle:
                    raise ValueError("SQS message did not include a ReceiptHandle")

                body = json.loads(message["Body"])

                self.processor.handle(
                    body,
                    message.get("MessageAttributes", {}),
                )

                self.client.delete_message(
                    QueueUrl=self.queue_url,
                    ReceiptHandle=receipt_handle,
                )

                confirmed += 1

                logger.info(
                    "execution_command_acknowledged",
                    extra={
                        "sqs_message_id": message_id,
                        "request_id": (body.get("requestId") if isinstance(body, dict) else None),
                        "saga_id": (body.get("sagaId") if isinstance(body, dict) else None),
                    },
                )

            except Exception:
                logger.exception(
                    "execution_command_not_acknowledged",
                    extra={
                        "sqs_message_id": message_id,
                        "request_id": (body.get("requestId") if isinstance(body, dict) else None),
                        "saga_id": (body.get("sagaId") if isinstance(body, dict) else None),
                        "approximate_receive_count": message.get("Attributes", {}).get(
                            "ApproximateReceiveCount"
                        ),
                    },
                )

                # Stop processing this batch after a failure to avoid
                # advancing past a failed message in a FIFO message group.
                break

        return confirmed

    def run_forever(
        self,
        stop_event: Event,
        error_backoff_seconds: float = 2.0,
    ) -> None:
        while not stop_event.is_set():
            try:
                self.poll_once()
            except Exception:
                logger.exception("execution_sqs_poll_failed")
                stop_event.wait(error_backoff_seconds)
