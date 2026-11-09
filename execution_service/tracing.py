from __future__ import annotations

from contextlib import AbstractContextManager, nullcontext
from typing import Any

try:  # O agente New Relic é opcional: sem o pacote, nada muda.
    import newrelic.agent as _agent
except ImportError:  # pragma: no cover - depende do ambiente
    _agent = None

TRACE_HEADERS = ("traceparent", "tracestate")


def background_transaction(name: str, group: str = "Message/SQS") -> AbstractContextManager[Any]:
    """Transação de background do agente; sem agente, um contexto vazio."""
    if _agent is None:
        return nullcontext()
    return _agent.BackgroundTask(_agent.application(), name=name, group=group)


def accept_trace_context(message_attributes: dict[str, Any] | None) -> None:
    """Liga a transação corrente ao traceparent/tracestate da mensagem SQS."""
    if _agent is None or not message_attributes:
        return
    headers = []
    for key in TRACE_HEADERS:
        value = (message_attributes.get(key) or {}).get("StringValue")
        if value:
            headers.append((key, value))
    if headers:
        _agent.accept_distributed_trace_headers(headers, transport_type="Queue")
