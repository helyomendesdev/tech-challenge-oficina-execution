from __future__ import annotations

from .enums import ExecutionStatus
from .exceptions import TransitionNotAllowedError


class ExecutionStatusPolicy:
    @staticmethod
    def validate_start(current: ExecutionStatus) -> None:
        raise TransitionNotAllowedError(
            f"Cannot start a second execution while status is {current.value}"
        )
