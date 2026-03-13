import json
from pathlib import Path

import pytest

from device_ingestion_service.main import create_app

EXPECTED_PATHS = {
    "/healthz",
    "/api/v1/ingestion/events",
    "/api/v1/ingestion/dead-letters",
    "/api/v1/ingestion/metrics",
}


def test_openapi_contains_expected_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEVICE_INGESTION_PERSISTENCE_BACKEND", "in_memory")
    monkeypatch.setenv("DEVICE_INGESTION_POSTGRES_AUTO_INIT", "false")
    app = create_app()
    schema = app.openapi()
    assert EXPECTED_PATHS.issubset(schema["paths"].keys())


def test_openapi_contract_file_exists_and_is_consistent(monkeypatch: pytest.MonkeyPatch) -> None:
    contract_path = Path(__file__).resolve().parent.parent / "contracts" / "openapi-v1.json"
    assert contract_path.exists(), "Run scripts/export_openapi.py to create the contract file"

    monkeypatch.setenv("DEVICE_INGESTION_PERSISTENCE_BACKEND", "in_memory")
    monkeypatch.setenv("DEVICE_INGESTION_POSTGRES_AUTO_INIT", "false")
    app = create_app()
    current = app.openapi()
    baseline = json.loads(contract_path.read_text(encoding="utf-8"))

    assert set(current["paths"].keys()) == set(baseline["paths"].keys())
