import json
from pathlib import Path

from fastapi.testclient import TestClient


def test_poc_golden_regression(in_memory_client: TestClient) -> None:
    fixture = Path(__file__).resolve().parent / "fixtures" / "poc_ingestion_golden.json"
    cases = json.loads(fixture.read_text(encoding="utf-8"))

    for case in cases:
        response = in_memory_client.post("/api/v1/ingestion/events", json=case["request"])
        assert response.status_code == 202, case["name"]
        body = response.json()

        assert body["summary"] == case["expected_summary"], case["name"]
        statuses = [item["status"] for item in body["items"]]
        assert statuses == case["expected_statuses"], case["name"]
