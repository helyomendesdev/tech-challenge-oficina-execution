from __future__ import annotations

import json
import logging
from threading import Event
from typing import Any

from execution_service.application.use_cases import ExecuteCommandUseCase
from execution_service.tracing import accept_trace_context, background_transaction

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
            body: Any = {}
            try:
                body = self._process_message(message)
            except Exception:
                logger.exception(
                    "execution_command_not_acknowledged",
                    extra={
                        "sqs_message_id": message.get("MessageId", "unknown"),
                        **_correlation(body),
                        "approximate_receive_count": message.get("Attributes", {}).get(
                            "ApproximateReceiveCount"
                        ),
                    },
                )

                # Stop processing this batch after a failure to avoid
                # advancing past a failed message in a FIFO message group.
                break

            confirmed += 1
            logger.info(
                "execution_command_acknowledged",
                extra={
                    "sqs_message_id": message.get("MessageId", "unknown"),
                    **_correlation(body),
                },
            )

        return confirmed

    def _process_message(self, message: dict[str, Any]) -> Any:
        """Handle one SQS message inside a New Relic transaction and delete it.

        Raises on any failure, leaving the message in the queue (redelivery).
        Returns the parsed body for correlation logging.
        """
        receipt_handle = message.get("ReceiptHandle")
        if not receipt_handle:
            raise ValueError("SQS message did not include a ReceiptHandle")

        body = json.loads(message["Body"])
        attributes = message.get("MessageAttributes", {})
        message_type = body.get("type") if isinstance(body, dict) else None

        with background_transaction(message_type or "IniciarExecucao"):
            accept_trace_context(attributes)
            self.processor.handle(body, attributes)

        self.client.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt_handle,
        )
        return body

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


def _correlation(body: Any) -> dict[str, Any]:
    if not isinstance(body, dict):
        return {"request_id": None, "saga_id": None}
    return {"request_id": body.get("requestId"), "saga_id": body.get("sagaId")}
