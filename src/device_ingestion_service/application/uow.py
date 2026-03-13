from abc import ABC, abstractmethod

from device_ingestion_service.domain.repositories import (
    DeadLetterRepository,
    OutboxRepository,
    TelemetryRepository,
)


class UnitOfWork(ABC):
    telemetry: TelemetryRepository
    outbox: OutboxRepository
    dead_letters: DeadLetterRepository

    @abstractmethod
    def __enter__(self) -> "UnitOfWork":
        raise NotImplementedError

    @abstractmethod
    def __exit__(self, exc_type, exc, tb) -> None:
        raise NotImplementedError

    @abstractmethod
    def commit(self) -> None:
        raise NotImplementedError

    def rollback(self) -> None:
        raise NotImplementedError
