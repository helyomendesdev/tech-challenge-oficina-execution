from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable, Sequence
from datetime import datetime
from typing import TextIO
from uuid import uuid4

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from execution_service.application.service_execution_use_cases import ExecutionWorkUseCase
from execution_service.domain.exceptions import (
    ConcurrentExecutionUpdateError,
    MessageValidationError,
)
from execution_service.domain.service_execution import PartConsumption
from execution_service.infrastructure.dynamodb_repository import DynamoDBExecutionRepository


class _JsonArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        _write_json(
            sys.stderr,
            {
                "status": "error",
                "error": {
                    "code": "invalid_arguments",
                    "message": message,
                },
            },
        )
        raise SystemExit(2)


def _positive_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _part_consumption(value: str) -> PartConsumption:
    fields = value.split(":")
    if len(fields) != 3:
        raise argparse.ArgumentTypeError("must use ITEM_PECA_OS_ID:PECA_ID:QUANTITY")
    try:
        item_peca_os_id, peca_id, quantity = (_positive_integer(field) for field in fields)
    except argparse.ArgumentTypeError as exc:
        raise argparse.ArgumentTypeError(
            "must use positive integers: ITEM_PECA_OS_ID:PECA_ID:QUANTITY"
        ) from exc
    return PartConsumption(item_peca_os_id, peca_id, quantity)


def _aware_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an ISO 8601 datetime") from exc
    if parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("must include a timezone offset")
    return parsed


def _build_parser() -> argparse.ArgumentParser:
    parser = _JsonArgumentParser(prog="python -m execution_service.cli")
    commands = parser.add_subparsers(dest="command", required=True)

    start = commands.add_parser("start-service")
    _add_service_arguments(start)
    start.add_argument("--started-at", type=_aware_datetime)

    consume = commands.add_parser("record-consumption")
    _add_service_arguments(consume)
    consume.add_argument(
        "--part",
        type=_part_consumption,
        action="append",
        required=True,
        metavar="ITEM_PECA_OS_ID:PECA_ID:QUANTITY",
    )

    finish = commands.add_parser("finish-service")
    _add_service_arguments(finish)
    finish.add_argument("--completed-at", type=_aware_datetime)
    return parser


def _add_service_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--saga-id", required=True)
    parser.add_argument("--service-id", type=_positive_integer, required=True)
    parser.add_argument("--request-id", default="execution-cli")
    parser.add_argument("--message-id")


def create_work_use_case() -> ExecutionWorkUseCase:
    table_name = os.getenv("EXECUTION_DYNAMODB_TABLE", "").strip()
    if not table_name:
        raise ValueError("EXECUTION_DYNAMODB_TABLE must be configured")
    region_name = os.getenv("AWS_REGION", "us-east-1").strip() or "us-east-1"
    dynamodb_client = boto3.Session(region_name=region_name).client("dynamodb")
    repository = DynamoDBExecutionRepository(
        table_name=table_name,
        client=dynamodb_client,
    )
    return ExecutionWorkUseCase(repository)


def main(
    argv: Sequence[str] | None = None,
    *,
    work_use_case_factory: Callable[[], ExecutionWorkUseCase] | None = None,
) -> int:
    args = _build_parser().parse_args(argv)
    message_id = args.message_id or str(uuid4())

    try:
        work = (work_use_case_factory or create_work_use_case)()
        if args.command == "start-service":
            result = work.start_service(
                args.saga_id,
                args.service_id,
                started_at=args.started_at,
                request_id=args.request_id,
                message_id=message_id,
            )
        elif args.command == "record-consumption":
            result = work.record_consumption(
                args.saga_id,
                args.service_id,
                args.part,
                request_id=args.request_id,
                message_id=message_id,
            )
        else:
            result = work.finish_service(
                args.saga_id,
                args.service_id,
                completed_at=args.completed_at,
                request_id=args.request_id,
                message_id=message_id,
            )
    except MessageValidationError as exc:
        return _write_error("validation_error", exc)
    except ConcurrentExecutionUpdateError as exc:
        return _write_error("concurrent_update", exc)
    except (ClientError, BotoCoreError) as exc:
        return _write_error("aws_error", exc)
    except ValueError as exc:
        return _write_error("configuration_or_input_error", exc)
    except RuntimeError as exc:
        return _write_error("operation_error", exc)

    outbox = result.outbox_message
    _write_json(
        sys.stdout,
        {
            "status": "success",
            "operation": args.command,
            "sagaId": args.saga_id,
            "serviceId": args.service_id,
            "serviceStatus": result.status.value,
            "messageId": message_id,
            "duplicate": result.duplicate,
            "executionCompleted": outbox is not None,
            "outboxMessageType": outbox.message.get("type") if outbox else None,
        },
    )
    return 0


def _write_error(code: str, error: Exception) -> int:
    _write_json(
        sys.stderr,
        {
            "status": "error",
            "error": {
                "code": code,
                "type": type(error).__name__,
                "message": str(error),
            },
        },
    )
    return 1


def _write_json(stream: TextIO, response: dict[str, object]) -> None:
    stream.write(json.dumps(response, ensure_ascii=False, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
