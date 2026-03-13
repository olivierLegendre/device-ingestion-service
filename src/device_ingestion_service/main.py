import logging

from fastapi import FastAPI

from device_ingestion_service.adapters.inbound.http.router import create_router
from device_ingestion_service.adapters.inbound.mqtt.protocols import (
    LoRaWanParser,
    Zigbee2MqttParser,
)
from device_ingestion_service.adapters.outbound.in_memory import InMemoryUnitOfWork
from device_ingestion_service.adapters.outbound.postgres import PostgresUnitOfWork
from device_ingestion_service.application.uow import UnitOfWork
from device_ingestion_service.application.use_cases import DeviceIngestionUseCases, IngestionMetrics
from device_ingestion_service.settings import Settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


def create_runtime(settings: Settings | None = None) -> tuple[Settings, DeviceIngestionUseCases]:
    active_settings = settings or Settings()

    uow: UnitOfWork
    if active_settings.persistence_backend == "postgres":
        uow = PostgresUnitOfWork(
            active_settings.postgres_dsn,
            auto_init_schema=active_settings.postgres_auto_init,
        )
    else:
        uow = InMemoryUnitOfWork()

    parsers = {
        "zigbee2mqtt": Zigbee2MqttParser(),
        "lorawan": LoRaWanParser(),
    }
    metrics = IngestionMetrics()

    use_cases = DeviceIngestionUseCases(
        uow=uow,
        parsers=parsers,
        dedup_window_seconds=active_settings.dedup_window_seconds,
        metrics=metrics,
    )
    return active_settings, use_cases


def create_app() -> FastAPI:
    settings, use_cases = create_runtime()

    app = FastAPI(
        title="Device Ingestion Service",
        version="1.0.0",
        description=(
            "Wave 3 baseline for protocol ingestion, normalization, deduplication, "
            "dead-letter policy, and outbox persistence."
        ),
    )
    app.include_router(create_router(use_cases, settings))
    return app


app = create_app()
