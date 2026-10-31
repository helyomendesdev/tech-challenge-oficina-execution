from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from execution_service.application.dto import (
    ExecutionEnvelope,
    ExecutionResult,
    OutboxMessage,
)
from execution_service.application.ports import ExecutionRepositoryPort
from execution_service.domain.enums import ExecutionStatus, SagaMessageType
from execution_service.domain.exceptions import (
    ExecutionCancellationUnavailableError,
    IdempotencyKeyReuseError,
    MessageValidationError,
    UnsupportedMessageError,
)
from execution_service.domain.models import ExecutionRecord
from execution_service.domain.policies import ExecutionStatusPolicy

_UUID = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-"
    r"[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)

_RFC3339 = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"([.][0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2})$"
)

_TRACEPARENT = re.compile(r"^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$")

_MOTIVES = {
    "EXECUCAO_FALHOU",
    "CANCELADO_PELO_CLIENTE",
    "TIMEOUT",
}

_ENVELOPE_FIELDS = {
    "messageId",
    "type",
    "version",
    "sagaId",
    "osId",
    "causationId",
    "requestId",
    "occurredAt",
    "payload",
}


class ExecutionMessageValidator:
    @classmethod
    def validate(
        cls,
        envelope: dict[str, Any],
        message_attributes: dict[str, Any] | None = None,
    ) -> ExecutionEnvelope:
        if not isinstance(envelope, dict):
            raise MessageValidationError("Message body must be a JSON object")

        if set(envelope) != _ENVELOPE_FIELDS:
            missing = sorted(_ENVELOPE_FIELDS - set(envelope))
            extra = sorted(set(envelope) - _ENVELOPE_FIELDS)

            raise MessageValidationError(
                f"Invalid envelope fields; missing={missing}, additional={extra}"
            )

        cls._uuid(envelope["messageId"], "messageId")
        cls._uuid(envelope["sagaId"], "sagaId")

        if envelope["causationId"] is not None:
            cls._uuid(envelope["causationId"], "causationId")

        if isinstance(envelope["version"], bool) or envelope["version"] != 1:
            raise MessageValidationError("Only message version 1 is supported")

        if isinstance(envelope["osId"], bool) or not isinstance(envelope["osId"], int):
            raise MessageValidationError("osId must be an integer")

        if envelope["osId"] < 1:
            raise MessageValidationError("osId must be greater than zero")

        request_id = envelope["requestId"]

        if not isinstance(request_id, str) or not 1 <= len(request_id) <= 128:
            raise MessageValidationError("requestId must contain between 1 and 128 characters")

        occurred_at = envelope["occurredAt"]

        if not isinstance(occurred_at, str) or not _RFC3339.fullmatch(occurred_at):
            raise MessageValidationError("occurredAt must be an RFC 3339 date-time")

        try:
            parsed_occurred_at = datetime.fromisoformat(occurred_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise MessageValidationError("occurredAt is not a valid date-time") from exc

        if parsed_occurred_at.utcoffset() is None:
            raise MessageValidationError("occurredAt must include a UTC offset")

        if not isinstance(envelope["payload"], dict):
            raise MessageValidationError("payload must be an object")

        message_type = envelope["type"]

        if message_type == SagaMessageType.INICIAR_EXECUCAO.value:
            cls._validate_start_payload(envelope["payload"])
        elif message_type == SagaMessageType.CANCELAR_EXECUCAO.value:
            cls._validate_cancel_payload(envelope["payload"])
        else:
            raise UnsupportedMessageError(f"Execution does not consume type {message_type!r}")

        if message_attributes is not None and not isinstance(message_attributes, dict):
            raise MessageValidationError("SQS message attributes must be an object")

        attributes = message_attributes or {}

        traceparent = cls._string_attribute(attributes, "traceparent")
        tracestate = cls._string_attribute(attributes, "tracestate")

        if traceparent is not None:
            cls._validate_traceparent(traceparent)

        if tracestate is not None:
            if traceparent is None:
                raise MessageValidationError("tracestate requires traceparent")

            if not tracestate or len(tracestate) > 512 or not tracestate.isascii():
                raise MessageValidationError(
                    "tracestate must be non-empty ASCII and no longer than 512 characters"
                )

        return ExecutionEnvelope(
            message_id=envelope["messageId"],
            type=message_type,
            version=1,
            saga_id=envelope["sagaId"],
            os_id=envelope["osId"],
            causation_id=envelope["causationId"],
            request_id=request_id,
            occurred_at=parsed_occurred_at,
            payload=envelope["payload"],
            traceparent=traceparent,
            tracestate=tracestate,
        )

    @staticmethod
    def _uuid(value: Any, name: str) -> None:
        if not isinstance(value, str) or not _UUID.fullmatch(value):
            raise MessageValidationError(f"{name} must be a lowercase RFC 4122 UUID")

    @staticmethod
    def _string_attribute(
        attributes: dict[str, Any],
        name: str,
    ) -> str | None:
        value = attributes.get(name)

        if value is None:
            return None

        if not isinstance(value, dict) or value.get("DataType") != "String":
            raise MessageValidationError(f"{name} must be an SQS String message attribute")

        string_value = value.get("StringValue")

        if not isinstance(string_value, str):
            raise MessageValidationError(f"{name} must have a StringValue")

        return string_value

    @staticmethod
    def _validate_traceparent(value: str) -> None:
        match = _TRACEPARENT.fullmatch(value)

        if match is None:
            raise MessageValidationError("traceparent does not match the W3C Trace Context format")

        version, trace_id, span_id, flags = match.groups()

        if version == "ff" or trace_id == "0" * 32 or span_id == "0" * 16:
            raise MessageValidationError(
                "traceparent contains a prohibited zero identifier or version"
            )

        if version == "00" and len(value) != 55:
            raise MessageValidationError(
                "traceparent version 00 must contain exactly 55 characters"
            )

        if int(flags, 16) > 255:
            raise MessageValidationError("traceparent flags are invalid")

    @classmethod
    def _validate_start_payload(cls, payload: dict[str, Any]) -> None:
        if set(payload) != {"orcamentoId", "servicos", "pecas"}:
            raise MessageValidationError(
                "IniciarExecucao payload requires only orcamentoId, servicos and pecas"
            )

        cls._uuid(payload["orcamentoId"], "payload.orcamentoId")

        for field_name, id_name in (
            ("servicos", "servicoId"),
            ("pecas", "pecaId"),
        ):
            entries = payload[field_name]

            if not isinstance(entries, list):
                raise MessageValidationError(f"payload.{field_name} must be an array")

            for index, entry in enumerate(entries):
                if not isinstance(entry, dict) or set(entry) != {
                    id_name,
                    "quantidade",
                }:
                    raise MessageValidationError(
                        f"payload.{field_name}[{index}] has invalid fields"
                    )

                for key in (id_name, "quantidade"):
                    value = entry[key]

                    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                        raise MessageValidationError(
                            f"payload.{field_name}[{index}].{key} must be a positive integer"
                        )

    @classmethod
    def _validate_cancel_payload(cls, payload: dict[str, Any]) -> None:
        if set(payload) != {"execucaoId", "motivo"}:
            raise MessageValidationError(
                "CancelarExecucao payload requires only execucaoId and motivo"
            )

        cls._uuid(payload["execucaoId"], "payload.execucaoId")

        if not isinstance(payload["motivo"], str) or payload["motivo"] not in _MOTIVES:
            raise MessageValidationError("payload.motivo is not a permitted reversal reason")


class ExecuteCommandUseCase:
    def __init__(self, repository: ExecutionRepositoryPort):
        self.repository = repository

    def handle(
        self,
        body: dict[str, Any],
        message_attributes: dict[str, Any] | None = None,
    ) -> ExecutionResult:
        command = ExecutionMessageValidator.validate(
            body,
            message_attributes,
        )

        body_fingerprint = hashlib.sha256(
            json.dumps(
                body,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
        ).hexdigest()

        existing_result = self._existing_result(
            command,
            body_fingerprint,
        )

        if existing_result is not None:
            return existing_result

        current = self.repository.get_by_saga_id(command.saga_id)

        if current is not None:
            existing_result = self._existing_result(
                command,
                body_fingerprint,
            )

            if existing_result is not None:
                return existing_result

        expected_version = current.version if current else None

        if command.type == SagaMessageType.INICIAR_EXECUCAO.value:
            if current is not None:
                ExecutionStatusPolicy.validate_start(current.status)

            record, response = self._start_execution(command, current)
        else:
            raise ExecutionCancellationUnavailableError(
                "CancelarExecucao is blocked until the execution worker and the "
                "owner of reserved-stock compensation are defined"
            )

        self.repository.register_command(
            record=record,
            input_message_id=command.message_id,
            input_fingerprint=body_fingerprint,
            response=response,
            expected_version=expected_version,
        )

        outbox = self.repository.get_outbox(command.message_id)

        if outbox is None:
            raise RuntimeError(f"Outbox record missing for messageId={command.message_id}")

        return self._result(record, outbox)

    def _existing_result(
        self,
        command: ExecutionEnvelope,
        input_fingerprint: str,
    ) -> ExecutionResult | None:
        previous_fingerprint = self.repository.get_input_fingerprint(command.message_id)

        if previous_fingerprint is None:
            return None

        if previous_fingerprint != input_fingerprint:
            raise IdempotencyKeyReuseError(
                f"messageId={command.message_id} was reused with a different body"
            )

        outbox = self.repository.get_outbox(command.message_id)

        if outbox is None:
            raise RuntimeError(f"Outbox record missing for messageId={command.message_id}")

        record = self.repository.get_by_saga_id(command.saga_id)

        if record is None:
            raise RuntimeError(f"Execution record missing for sagaId={command.saga_id}")

        return self._result(record, outbox)

    @staticmethod
    def _result(
        record: ExecutionRecord,
        outbox: OutboxMessage,
    ) -> ExecutionResult:
        return ExecutionResult(
            status=record.status.value,
            message_type=outbox.message["type"],
            execution_id=outbox.message["payload"].get(
                "execucaoId",
                record.execution_id,
            ),
            saga_id=record.saga_id,
            os_id=record.os_id,
            payload=outbox.message["payload"],
            message_id=outbox.message["messageId"],
        )

    @staticmethod
    def _start_execution(
        command: ExecutionEnvelope,
        current: ExecutionRecord | None,
    ) -> tuple[ExecutionRecord, OutboxMessage]:
        if current is not None:
            raise RuntimeError("Start transition validation should reject an existing execution")

        now = datetime.now(timezone.utc)
        execution_id = str(uuid4())

        record = ExecutionRecord(
            execution_id=execution_id,
            saga_id=command.saga_id,
            os_id=command.os_id,
            budget_id=command.payload["orcamentoId"],
            services=command.payload["servicos"],
            parts=command.payload["pecas"],
            status=ExecutionStatus.EM_EXECUCAO,
            started_at=now,
            updated_at=now,
            version=1,
        )

        response = ExecuteCommandUseCase._response(
            command,
            SagaMessageType.EXECUCAO_INICIADA.value,
            {
                "execucaoId": execution_id,
                "orcamentoId": record.budget_id,
                "iniciadaEm": now.isoformat(),
            },
            command.traceparent,
            command.tracestate,
        )

        return record, response

    @staticmethod
    def _response(
        command: ExecutionEnvelope,
        message_type: str,
        payload: dict[str, Any],
        traceparent: str | None,
        tracestate: str | None,
    ) -> OutboxMessage:
        response_traceparent = _child_traceparent(traceparent)

        attributes = {
            "traceparent": {
                "DataType": "String",
                "StringValue": response_traceparent,
            }
        }

        if tracestate:
            attributes["tracestate"] = {
                "DataType": "String",
                "StringValue": tracestate,
            }

        message = {
            "messageId": str(uuid4()),
            "type": message_type,
            "version": 1,
            "sagaId": command.saga_id,
            "osId": command.os_id,
            "causationId": command.message_id,
            "requestId": command.request_id,
            "occurredAt": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
        }

        return OutboxMessage(
            message=message,
            message_attributes=attributes,
        )


def _child_traceparent(parent: str | None) -> str:
    if parent is None:
        return f"00-{uuid4().hex}-{uuid4().hex[:16]}-01"

    match = _TRACEPARENT.fullmatch(parent)

    if match is None:
        raise MessageValidationError("traceparent does not match the W3C Trace Context format")

    version, trace_id, _parent_span_id, flags = match.groups()

    return f"{version}-{trace_id}-{uuid4().hex[:16]}-{flags}"
