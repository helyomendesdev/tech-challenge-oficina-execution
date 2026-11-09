from __future__ import annotations

from types import SimpleNamespace

import pytest

from execution_service import tracing
from execution_service.infrastructure.outbox_relay import OutboxRelay
from tests.factories import TRACEPARENT, sqs_message, start_message
from tests.test_execution_infrastructure import FakePublisher, FakeSQSClient


class FakeBackgroundTask:
    def __init__(self, calls, application, name, group):
        self.calls = calls
        self.record = {"application": application, "name": name, "group": group}

    def __enter__(self):
        self.calls["events"].append("enter")
        self.calls["transactions"].append(self.record)
        return self

    def __exit__(self, exc_type, exc, tb):
        self.calls["events"].append("exit")
        return False


@pytest.fixture
def fake_agent(monkeypatch):
    calls = {"transactions": [], "accepted": [], "events": []}

    agent = SimpleNamespace(
        application=lambda: "app",
        BackgroundTask=lambda application, name, group: FakeBackgroundTask(
            calls, application, name, group
        ),
        accept_distributed_trace_headers=lambda headers, transport_type="HTTP": (
            calls["events"].append("accept"),
            calls["accepted"].append((list(headers), transport_type)),
        ),
    )
    monkeypatch.setattr(tracing, "_agent", agent)
    return calls


def test_consumer_runs_message_in_background_transaction_and_accepts_trace(
    consumer_factory, fake_agent
):
    body = start_message()
    sqs = FakeSQSClient(sqs_message(body))
    consumer, _repository = consumer_factory(sqs)

    assert consumer.poll_once() == 1

    assert fake_agent["transactions"] == [
        {"application": "app", "name": body["type"], "group": "Message/SQS"}
    ]
    assert fake_agent["accepted"] == [([("traceparent", TRACEPARENT)], "Queue")]
    # A transação abre antes de aceitar o trace e só fecha depois de processar.
    assert fake_agent["events"][:2] == ["enter", "accept"]
    assert fake_agent["events"][-1] == "exit"


def test_consumer_without_trace_attributes_does_not_accept(consumer_factory, fake_agent):
    message = sqs_message(start_message())
    message["MessageAttributes"] = {}
    consumer, _repository = consumer_factory(FakeSQSClient(message))

    assert consumer.poll_once() == 1
    assert len(fake_agent["transactions"]) == 1
    assert fake_agent["accepted"] == []


def test_consumer_failure_still_leaves_message_unacknowledged(consumer_factory, fake_agent):
    sqs = FakeSQSClient(sqs_message({"messageId": "missing-fields"}))
    consumer, _repository = consumer_factory(sqs)

    assert consumer.poll_once() == 0
    assert sqs.deleted == []
    assert fake_agent["events"][-1] == "exit"


def test_relay_publishes_inside_background_transaction(consumer_factory, fake_agent):
    body = start_message()
    sqs = FakeSQSClient(sqs_message(body))
    consumer, repository = consumer_factory(sqs)
    consumer.poll_once()
    fake_agent["events"].clear()
    fake_agent["transactions"].clear()

    publisher = FakePublisher()
    assert OutboxRelay(repository, publisher).poll_once() == 1

    assert len(fake_agent["transactions"]) == 1
    assert fake_agent["transactions"][0]["group"] == "Message/SQS"
    assert fake_agent["events"] == ["enter", "exit"]
    # O traceparent calculado pelo caso de uso segue intacto.
    attributes = publisher.attempts[0]["message_attributes"]
    assert attributes["traceparent"]["StringValue"].startswith(
        "00-4bf92f3577b34da6a3ce929d0e0e4736-"
    )


def test_without_agent_nothing_changes(consumer_factory, monkeypatch):
    monkeypatch.setattr(tracing, "_agent", None)
    body = start_message()
    sqs = FakeSQSClient(sqs_message(body))
    consumer, repository = consumer_factory(sqs)

    assert consumer.poll_once() == 1
    assert OutboxRelay(repository, FakePublisher()).poll_once() == 1
