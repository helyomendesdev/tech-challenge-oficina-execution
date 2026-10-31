from __future__ import annotations

import logging
import signal
import sys
from threading import Event, Thread

import boto3

from execution_service.application.ports import ExecutionRepositoryPort
from execution_service.application.use_cases import ExecuteCommandUseCase
from execution_service.config import get_settings
from execution_service.infrastructure.dynamodb_repository import (
    DynamoDBExecutionRepository,
)
from execution_service.infrastructure.outbox_relay import OutboxRelay
from execution_service.infrastructure.sqs_adapter import SQSFifoMessagePublisher
from execution_service.interfaces.sqs_consumer import SQSFifoConsumer
from execution_service.logging_config import JsonLogFormatter

logger = logging.getLogger(__name__)


def configure_logging(log_level: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonLogFormatter())

    logging.basicConfig(
        level=log_level,
        handlers=[handler],
        force=True,
    )


def create_processor(
    repository: ExecutionRepositoryPort,
) -> ExecuteCommandUseCase:
    return ExecuteCommandUseCase(repository=repository)


def main() -> None:
    settings = get_settings()

    if not settings.commands_queue_url:
        raise ValueError("EXECUTION_COMMANDS_QUEUE_URL must be configured")

    if not settings.responses_queue_url:
        raise ValueError("SAGA_RESPONSES_QUEUE_URL must be configured")

    if not settings.dynamodb_table:
        raise ValueError("EXECUTION_DYNAMODB_TABLE must be configured")

    configure_logging(settings.log_level)

    logger.info("execution_service_starting")

    sqs_client = boto3.client(
        "sqs",
        region_name=settings.aws_region,
    )

    dynamodb_client = boto3.client(
        "dynamodb",
        region_name=settings.aws_region,
    )

    repository = DynamoDBExecutionRepository(
        table_name=settings.dynamodb_table,
        client=dynamodb_client,
    )

    publisher = SQSFifoMessagePublisher(
        client=sqs_client,
        queue_url=settings.responses_queue_url,
    )

    processor = create_processor(repository)

    consumer = SQSFifoConsumer(
        client=sqs_client,
        queue_url=settings.commands_queue_url,
        processor=processor,
        visibility_timeout=settings.visibility_timeout,
        wait_time_seconds=settings.wait_time_seconds,
    )

    relay = OutboxRelay(
        repository=repository,
        publisher=publisher,
    )

    stop_event = Event()

    def handle_shutdown(_signum: int, _frame: object) -> None:
        logger.info("execution_service_shutdown_requested")
        stop_event.set()

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    relay_thread = Thread(
        target=relay.run_forever,
        args=(
            stop_event,
            settings.outbox_poll_interval_seconds,
        ),
        name="execution-outbox-relay",
        daemon=True,
    )

    relay_thread.start()

    try:
        logger.info("execution_service_started")

        consumer.run_forever(
            stop_event,
            settings.poll_error_backoff_seconds,
        )
    finally:
        logger.info("execution_service_stopping")

        stop_event.set()

        relay_thread.join(
            timeout=settings.outbox_poll_interval_seconds + 1,
        )

        if relay_thread.is_alive():
            logger.warning("execution_outbox_relay_shutdown_timeout")

        logger.info("execution_service_stopped")


if __name__ == "__main__":
    main()
