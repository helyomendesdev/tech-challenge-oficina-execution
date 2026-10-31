from __future__ import annotations

import pytest

from execution_service.application.use_cases import ExecuteCommandUseCase
from execution_service.infrastructure.fake_repository import InMemoryExecutionRepository
from execution_service.interfaces.sqs_consumer import SQSFifoConsumer


@pytest.fixture
def execution_repository():
    return InMemoryExecutionRepository()


@pytest.fixture
def processor_factory():
    def build_processor(repository=None):
        return ExecuteCommandUseCase(
            repository=repository or InMemoryExecutionRepository(),
        )

    return build_processor


@pytest.fixture
def execution_processor(processor_factory):
    return processor_factory()


@pytest.fixture
def consumer_factory(processor_factory):
    def build_consumer(client, repository=None):
        repository = repository or InMemoryExecutionRepository()

        consumer = SQSFifoConsumer(
            client=client,
            queue_url="https://sqs/commands.fifo",
            processor=processor_factory(repository=repository),
            visibility_timeout=30,
            wait_time_seconds=20,
        )

        return consumer, repository

    return build_consumer
