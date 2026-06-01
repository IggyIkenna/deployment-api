"""Unit tests for GET /api/data-status/honest-coverage.

The endpoint reads coverage.json from GCS, returns it verbatim on success,
and returns 404/500 on missing/malformed data.

Plan: deployment_and_qg_strategy_implementation_2026_05_13.md Phase 4.C.
"""

from __future__ import annotations

import json
import re
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from deployment_api.main import app

SAMPLE_COVERAGE: dict[str, object] = {
    "generated_at": "2026-05-15T06:00:00Z",
    "date": "2026-05-15",
    "by_asset_group": {
        "cefi": {
            "captured": 495,
            "empty_confirmed": 0,
            "attempted_failed": 10,
            "expected_unattempted": 495,
            "total": 1000,
            "coverage_pct": 49.5,
        },
    },
    "by_venue": {},
    "by_venue_data_type": {},
}

_PATCH = "deployment_api.utils.storage_facade.read_object_text"


@pytest.fixture(scope="module")
def api_client() -> TestClient:
    return TestClient(app, raise_server_exceptions=False)


class TestGetHonestCoverageRoute:
    def test_returns_coverage_json_on_success(self, api_client: TestClient) -> None:
        """200 + verbatim JSON when the GCS blob is present."""
        with patch(_PATCH, return_value=json.dumps(SAMPLE_COVERAGE)):
            resp = api_client.get("/api/data-status/honest-coverage?date=2026-05-15")

        assert resp.status_code == 200
        body = resp.json()
        assert body["date"] == "2026-05-15"
        assert "cefi" in body["by_asset_group"]
        assert body["by_asset_group"]["cefi"]["coverage_pct"] == pytest.approx(49.5)

    def test_returns_404_when_blob_absent(self, api_client: TestClient) -> None:
        """404 when the cron VM hasn't written today's coverage yet."""
        with patch(_PATCH, side_effect=FileNotFoundError("blob not found")):
            resp = api_client.get("/api/data-status/honest-coverage?date=2026-05-15")

        assert resp.status_code == 404
        assert "not available" in resp.text

    def test_returns_500_when_json_malformed(self, api_client: TestClient) -> None:
        """500 when the blob exists but contains invalid JSON."""
        with patch(_PATCH, return_value="not-valid-json{{{"):
            resp = api_client.get("/api/data-status/honest-coverage?date=2026-05-15")

        assert resp.status_code == 500
        assert "malformed" in resp.text

    def test_reads_correct_bucket_and_path(self, api_client: TestClient) -> None:
        """Storage call uses central-element-323112-honest-coverage and date/coverage.json."""
        calls: list[tuple[str, str]] = []

        def _capture(bucket: str, path: str) -> str:
            calls.append((bucket, path))
            return json.dumps(SAMPLE_COVERAGE)

        with patch(_PATCH, side_effect=_capture):
            api_client.get("/api/data-status/honest-coverage?date=2026-05-15")

        assert len(calls) == 1
        bucket, path = calls[0]
        assert bucket == "central-element-323112-honest-coverage"
        assert path == "2026-05-15/coverage.json"

    def test_defaults_to_today_when_no_date_param(self, api_client: TestClient) -> None:
        """Date defaults to today UTC when the ?date= param is omitted."""
        calls: list[tuple[str, str]] = []

        def _capture(bucket: str, path: str) -> str:
            calls.append((bucket, path))
            return json.dumps(SAMPLE_COVERAGE)

        with patch(_PATCH, side_effect=_capture):
            resp = api_client.get("/api/data-status/honest-coverage")

        assert resp.status_code == 200
        assert len(calls) == 1
        _, path = calls[0]
        assert re.match(r"\d{4}-\d{2}-\d{2}/coverage\.json", path)
