import pytest
from fastapi.testclient import TestClient

from device_ingestion_service.main import create_app


@pytest.fixture
def in_memory_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("DEVICE_INGESTION_PERSISTENCE_BACKEND", "in_memory")
    monkeypatch.setenv("DEVICE_INGESTION_POSTGRES_AUTO_INIT", "false")

    app = create_app()
    with TestClient(app) as client:
        yield client
