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


@pytest.mark.postgres_integration
def test_postgres_dead_letter_scope_isolation(monkeypatch: pytest.MonkeyPatch) -> None:
    dsn = os.getenv("DEVICE_INGESTION_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("DEVICE_INGESTION_TEST_POSTGRES_DSN is not set")

    monkeypatch.setenv("DEVICE_INGESTION_PERSISTENCE_BACKEND", "postgres")
    monkeypatch.setenv("DEVICE_INGESTION_POSTGRES_DSN", dsn)
    monkeypatch.setenv("DEVICE_INGESTION_POSTGRES_AUTO_INIT", "true")

    client = TestClient(create_app())

    dead = client.post(
        "/api/v1/ingestion/events",
        json={
            "organization_id": "org-a",
            "site_id": "site-a",
            "protocol": "lorawan",
            "topic": "lorawan/device-a",
            "payload": {
                "message_id": "dead-tenant-a",
                "decoded_payload": {"battery": 91},
            },
        },
    )
    assert dead.status_code == 202
    assert dead.json()["summary"]["dead_letter"] == 1
    dead_letter_id = dead.json()["items"][0]["dead_letter_id"]

    list_ok = client.get(
        "/api/v1/ingestion/dead-letters",
        params={"organization_id": "org-a", "site_id": "site-a", "limit": 10},
    )
    assert list_ok.status_code == 200
    assert any(row["dead_letter_id"] == dead_letter_id for row in list_ok.json())

    list_wrong_org = client.get(
        "/api/v1/ingestion/dead-letters",
        params={"organization_id": "org-b", "site_id": "site-a", "limit": 10},
    )
    assert list_wrong_org.status_code == 200
    assert list_wrong_org.json() == []

    list_wrong_site = client.get(
        "/api/v1/ingestion/dead-letters",
        params={"organization_id": "org-a", "site_id": "site-b", "limit": 10},
    )
    assert list_wrong_site.status_code == 200
    assert list_wrong_site.json() == []
