from fastapi import APIRouter, Query

from device_ingestion_service.application.use_cases import DeviceIngestionUseCases, IngestionRequest
from device_ingestion_service.settings import Settings

from .schemas import (
    DeadLetterResponse,
    HealthResponse,
    IngestionRequestBody,
    IngestionResponse,
    IngestItemResponse,
    IngestSummaryResponse,
    MetricsResponse,
)


def create_router(use_cases: DeviceIngestionUseCases, settings: Settings) -> APIRouter:
    router = APIRouter()

    @router.get("/healthz", response_model=HealthResponse)
    def health() -> HealthResponse:
        return HealthResponse(
            status="ok",
            service=settings.app_name,
            env=settings.app_env,
            persistence_backend=settings.persistence_backend,
        )

    @router.post("/api/v1/ingestion/events", response_model=IngestionResponse, status_code=202)
    def ingest_event(body: IngestionRequestBody) -> IngestionResponse:
        result = use_cases.ingest(
            IngestionRequest(
                organization_id=body.organization_id,
                site_id=body.site_id,
                protocol=body.protocol,
                topic=body.topic,
                payload=body.payload,
            )
        )

        return IngestionResponse(
            summary=IngestSummaryResponse(
                accepted=result.accepted_count,
                duplicate=result.duplicate_count,
                dead_letter=result.dead_letter_count,
            ),
            items=[
                IngestItemResponse(
                    status=item.status.value,
                    protocol=item.protocol,
                    point_key=item.point_key,
                    telemetry_id=item.telemetry_id,
                    duplicate_of=item.duplicate_of,
                    outbox_event_id=item.outbox_event_id,
                    dead_letter_id=item.dead_letter_id,
                    reason=item.reason,
                )
                for item in result.items
            ],
        )

    @router.get("/api/v1/ingestion/dead-letters", response_model=list[DeadLetterResponse])
    def list_dead_letters(
        organization_id: str = Query(min_length=1),
        site_id: str = Query(min_length=1),
        limit: int = Query(default=settings.dead_letter_list_default_limit, ge=1, le=1000),
    ) -> list[DeadLetterResponse]:
        rows = use_cases.list_dead_letters(
            organization_id=organization_id,
            site_id=site_id,
            limit=limit,
        )
        return [
            DeadLetterResponse(
                dead_letter_id=row.dead_letter_id,
                organization_id=row.organization_id,
                site_id=row.site_id,
                protocol=row.protocol,
                topic=row.topic,
                reason=row.reason,
                payload=row.payload,
                received_at=row.received_at,
            )
            for row in rows
        ]

    @router.get("/api/v1/ingestion/metrics", response_model=MetricsResponse)
    def metrics() -> MetricsResponse:
        snapshot = use_cases.metrics_snapshot()
        return MetricsResponse.model_validate(snapshot)

    return router
