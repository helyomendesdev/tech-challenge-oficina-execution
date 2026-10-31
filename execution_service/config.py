from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    commands_queue_url: str
    responses_queue_url: str
    dynamodb_table: str
    aws_region: str
    log_level: str
    visibility_timeout: int
    wait_time_seconds: int
    poll_error_backoff_seconds: float
    outbox_poll_interval_seconds: float

    def __post_init__(self) -> None:
        if not 1 <= self.visibility_timeout <= 43200:
            raise ValueError("EXECUTION_VISIBILITY_TIMEOUT_SECONDS must be between 1 and 43200")

        if not 0 <= self.wait_time_seconds <= 20:
            raise ValueError("EXECUTION_SQS_WAIT_TIME_SECONDS must be between 0 and 20")

        if self.poll_error_backoff_seconds <= 0:
            raise ValueError("EXECUTION_SQS_ERROR_BACKOFF_SECONDS must be greater than zero")

        if self.outbox_poll_interval_seconds <= 0:
            raise ValueError("EXECUTION_OUTBOX_POLL_INTERVAL_SECONDS must be greater than zero")

        if not self.aws_region.strip():
            raise ValueError("AWS_REGION must not be empty")

        if self.log_level.upper() not in {
            "DEBUG",
            "INFO",
            "WARNING",
            "ERROR",
            "CRITICAL",
        }:
            raise ValueError("EXECUTION_LOG_LEVEL must be DEBUG, INFO, WARNING, ERROR or CRITICAL")


def get_settings() -> Settings:
    return Settings(
        commands_queue_url=os.getenv("EXECUTION_COMMANDS_QUEUE_URL", "").strip(),
        responses_queue_url=os.getenv("SAGA_RESPONSES_QUEUE_URL", "").strip(),
        dynamodb_table=os.getenv("EXECUTION_DYNAMODB_TABLE", "").strip(),
        aws_region=os.getenv("AWS_REGION", "us-east-1").strip(),
        log_level=os.getenv("EXECUTION_LOG_LEVEL", "INFO").strip().upper(),
        visibility_timeout=int(os.getenv("EXECUTION_VISIBILITY_TIMEOUT_SECONDS", "30")),
        wait_time_seconds=int(os.getenv("EXECUTION_SQS_WAIT_TIME_SECONDS", "20")),
        poll_error_backoff_seconds=float(os.getenv("EXECUTION_SQS_ERROR_BACKOFF_SECONDS", "2")),
        outbox_poll_interval_seconds=float(
            os.getenv("EXECUTION_OUTBOX_POLL_INTERVAL_SECONDS", "5")
        ),
    )
