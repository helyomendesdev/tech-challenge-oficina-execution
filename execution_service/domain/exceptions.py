class MessageValidationError(ValueError):
    """Envelope or contract validation failed."""


class IdempotencyKeyReuseError(MessageValidationError):
    """A messageId was reused with a different message body."""


class TransitionNotAllowedError(MessageValidationError):
    """An execution state transition is not permitted."""


class UnsupportedMessageError(MessageValidationError):
    """The message type is not a command consumed by the Execution Service."""


class ExecutionCancellationUnavailableError(RuntimeError):
    """Cancellation requires a defined worker and stock-compensation owner."""
