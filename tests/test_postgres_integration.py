import os

import pytest
from fastapi.testclient import TestClient

from device_ingestion_service.main import create_app


@pytest.mark.postgres_integration
def test_postgres_ingestion_roundtrip(monkeypatch: pytest.MonkeyPatch) -> None:
    dsn = os.getenv("DEVICE_INGESTION_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("DEVICE_INGESTION_TEST_POSTGRES_DSN is not set")

    monkeypatch.setenv("DEVICE_INGESTION_PERSISTENCE_BACKEND", "postgres")
    monkeypatch.setenv("DEVICE_INGESTION_POSTGRES_DSN", dsn)
    monkeypatch.setenv("DEVICE_INGESTION_POSTGRES_AUTO_INIT", "true")

    client = TestClient(create_app())

    ingest = client.post(
        "/api/v1/ingestion/events",
        json={
            "organization_id": "org-pg",
            "site_id": "site-pg",
            "protocol": "lorawan",
            "topic": "lorawan/device-pg",
            "payload": {
                "devEui": "0001",
                "message_id": "msg-pg-1",
                "received_at": "2026-03-11T12:00:00Z",
                "decoded_payload": {"co2": 612, "temperature": 24.2},
            },
        },
    )
    assert ingest.status_code == 202
    assert ingest.json()["summary"]["accepted"] == 2

    duplicate = client.post(
        "/api/v1/ingestion/events",
        json={
            "organization_id": "org-pg",
            "site_id": "site-pg",
            "protocol": "lorawan",
            "topic": "lorawan/device-pg",
            "payload": {
                "devEui": "0001",
                "message_id": "msg-pg-1",
                "received_at": "2026-03-11T12:00:00Z",
                "decoded_payload": {"co2": 612, "temperature": 24.2},
            },
        },
    )
    assert duplicate.status_code == 202
    assert duplicate.json()["summary"]["duplicate"] == 2

    dead = client.post(
        "/api/v1/ingestion/events",
        json={
            "organization_id": "org-pg",
            "site_id": "site-pg",
            "protocol": "lorawan",
            "topic": "lorawan/device-pg",
            "payload": {
                "message_id": "dead-pg-1",
                "decoded_payload": {"battery": 90},
            },
        },
    )
    assert dead.status_code == 202
    assert dead.json()["summary"]["dead_letter"] == 1

    dead_letters = client.get(
        "/api/v1/ingestion/dead-letters",
        params={"organization_id": "org-pg", "site_id": "site-pg", "limit": 10},
    )
    assert dead_letters.status_code == 200
    rows = dead_letters.json()
    assert rows
    assert rows[0]["reason"] in {"unprocessablepayload", "invalidpayload"}
