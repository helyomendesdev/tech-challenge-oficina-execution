from __future__ import annotations

import logging
from threading import Event

from botocore.exceptions import BotoCoreError, ClientError

from execution_service.application.ports import ExecutionRepositoryPort, MessagePublisherPort

logger = logging.getLogger(__name__)


class OutboxRelay:
    def __init__(
        self,
        repository: ExecutionRepositoryPort,
        publisher: MessagePublisherPort,
        batch_size: int = 10,
    ):
        if batch_size < 1:
            raise ValueError("batch_size must be greater than zero")
        self.repository = repository
        self.publisher = publisher
        self.batch_size = batch_size

    def poll_once(self) -> int:
        published = 0
        for outbox in self.repository.get_pending_outbox(self.batch_size):
            if not outbox.input_message_id:
                raise RuntimeError("Pending outbox record is missing its input message id")
            message = outbox.message
            try:
                self.publisher.publish(
                    message,
                    message_group_id=message["sagaId"],
                    message_deduplication_id=message["messageId"],
                    message_attributes=outbox.message_attributes,
                )
                self.repository.mark_outbox_published(outbox.input_message_id)
                published += 1
                logger.info(
                    "execution_pending_outbox_published",
                    extra={
                        "message_id": message["messageId"],
                        "message_type": message["type"],
                        "request_id": message["requestId"],
                        "saga_id": message["sagaId"],
                        "os_id": message["osId"],
                    },
                )
            except (BotoCoreError, ClientError, RuntimeError, KeyError, TypeError, ValueError):
                logger.exception(
                    "execution_pending_outbox_not_published",
                    extra={
                        "message_id": message.get("messageId"),
                        "request_id": message.get("requestId"),
                        "saga_id": message.get("sagaId"),
                        "os_id": message.get("osId"),
                    },
                )
        return published

    def run_forever(
        self,
        stop_event: Event,
        poll_interval_seconds: float = 5.0,
    ) -> None:
        while not stop_event.is_set():
            try:
                self.poll_once()
            except (BotoCoreError, ClientError, RuntimeError):
                logger.exception("execution_outbox_poll_failed")
            stop_event.wait(poll_interval_seconds)
