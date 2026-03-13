from fastapi.testclient import TestClient


def test_healthz(in_memory_client: TestClient) -> None:
    response = in_memory_client.get("/healthz")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["service"] == "device-ingestion-service"
    assert body["persistence_backend"] == "in_memory"


def test_zigbee_ingestion_and_dedup(in_memory_client: TestClient) -> None:
    payload = {
        "organization_id": "org-1",
        "site_id": "site-1",
        "protocol": "zigbee2mqtt",
        "topic": "zigbee2mqtt/sensor-a",
        "payload": {
            "message_id": "msg-zb-1",
            "timestamp": "2026-03-11T10:00:00Z",
            "temperature": 21.7,
            "humidity": 43.2,
            "linkquality": 88,
        },
    }

    first = in_memory_client.post("/api/v1/ingestion/events", json=payload)
    assert first.status_code == 202
    first_body = first.json()
    assert first_body["summary"] == {"accepted": 2, "duplicate": 0, "dead_letter": 0}
    assert {item["status"] for item in first_body["items"]} == {"accepted"}

    second = in_memory_client.post("/api/v1/ingestion/events", json=payload)
    assert second.status_code == 202
    second_body = second.json()
    assert second_body["summary"] == {"accepted": 0, "duplicate": 2, "dead_letter": 0}
    assert {item["status"] for item in second_body["items"]} == {"duplicate"}
    assert all(item["duplicate_of"] for item in second_body["items"])


def test_unprocessable_payload_goes_to_dead_letter(in_memory_client: TestClient) -> None:
    response = in_memory_client.post(
        "/api/v1/ingestion/events",
        json={
            "organization_id": "org-1",
            "site_id": "site-1",
            "protocol": "zigbee2mqtt",
            "topic": "zigbee2mqtt/sensor-dead",
            "payload": {
                "message_id": "dead-1",
                "linkquality": 95,
                "battery": 89,
            },
        },
    )
    assert response.status_code == 202
    body = response.json()
    assert body["summary"] == {"accepted": 0, "duplicate": 0, "dead_letter": 1}
    assert body["items"][0]["status"] == "dead_letter"


def test_metrics_endpoint_tracks_status_counts(in_memory_client: TestClient) -> None:
    in_memory_client.post(
        "/api/v1/ingestion/events",
        json={
            "organization_id": "org-1",
            "site_id": "site-1",
            "protocol": "lorawan",
            "topic": "lorawan/dev-1",
            "payload": {
                "devEui": "ABC",
                "received_at": "2026-03-11T10:05:00Z",
                "decoded_payload": {"co2": 550},
            },
        },
    )

    response = in_memory_client.get("/api/v1/ingestion/metrics")
    assert response.status_code == 200
    body = response.json()
    assert body["status"]["accepted"] >= 1
    assert body["protocol"]["lorawan"] >= 1


def test_dead_letter_scope_is_site_limited(in_memory_client: TestClient) -> None:
    for site_id in ["site-a", "site-b"]:
        in_memory_client.post(
            "/api/v1/ingestion/events",
            json={
                "organization_id": "org-1",
                "site_id": site_id,
                "protocol": "unknown-proto",
                "topic": "unknown/topic",
                "payload": {"x": 1},
            },
        )

    response = in_memory_client.get(
        "/api/v1/ingestion/dead-letters",
        params={"organization_id": "org-1", "site_id": "site-a", "limit": 10},
    )
    assert response.status_code == 200
    rows = response.json()
    assert rows
    assert all(row["site_id"] == "site-a" for row in rows)
