from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any


class JsonLogFormatter(logging.Formatter):
    _CONTEXT_FIELDS = (
        "message_id",
        "message_type",
        "request_id",
        "saga_id",
        "os_id",
        "sqs_message_id",
        "approximate_receive_count",
    )

    def format(self, record: logging.LogRecord) -> str:
        event: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created,
                timezone.utc,
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        for field in self._CONTEXT_FIELDS:
            value = getattr(record, field, None)

            if value is not None:
                event[field] = value

        if record.exc_info:
            event["exception"] = self.formatException(record.exc_info)

        return json.dumps(
            event,
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        )
