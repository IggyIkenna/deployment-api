"""Unit tests for GET /api/data-status/honest-coverage.

The endpoint reads coverage.json from GCS, returns it verbatim on success,
and returns 404/500 on missing/malformed data.

Plan: deployment_and_qg_strategy_implementation_2026_05_13.md Phase 4.C.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, date, datetime, timedelta
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
        """Storage call uses the config-derived {project}-honest-coverage bucket and date/coverage.json."""
        calls: list[tuple[str, str]] = []

        def _capture(bucket: str, path: str) -> str:
            calls.append((bucket, path))
            return json.dumps(SAMPLE_COVERAGE)

        with patch(_PATCH, side_effect=_capture):
            api_client.get("/api/data-status/honest-coverage?date=2026-05-15")

        assert len(calls) == 1
        bucket, path = calls[0]
        # Derived from GCP_PROJECT_ID (tests/unit/conftest.py sets test-project) --
        # pins the f"{project_id}-honest-coverage" derivation, no hardcoded prod id.
        assert bucket == "test-project-honest-coverage"
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

    def test_falls_back_to_latest_available_when_today_absent(self, api_client: TestClient) -> None:
        """An un-dated request walks BACK to the most recent measured day instead of
        404ing for the whole morning window before the daily cron runs."""
        paths: list[str] = []

        def _capture(_bucket: str, path: str) -> str:
            paths.append(path)
            if len(paths) < 3:  # today + today-1 absent; today-2 present
                raise FileNotFoundError("blob not found")
            return json.dumps(SAMPLE_COVERAGE)

        with patch(_PATCH, side_effect=_capture):
            resp = api_client.get("/api/data-status/honest-coverage")

        assert resp.status_code == 200
        assert len(paths) == 3  # walked back two days to the first hit
        walked = [date.fromisoformat(p.split("/", 1)[0]) for p in paths]
        assert walked[0] - walked[1] == timedelta(days=1)  # strictly descending, 1 day apart
        assert walked[1] - walked[2] == timedelta(days=1)

    def test_explicit_date_does_not_fall_back(self, api_client: TestClient) -> None:
        """An explicit ?date= is honoured exactly — a miss is 404 with NO silent
        substitution of a day the caller did not ask for (single read, no walk-back)."""
        paths: list[str] = []

        def _capture(_bucket: str, path: str) -> str:
            paths.append(path)
            raise FileNotFoundError("blob not found")

        with patch(_PATCH, side_effect=_capture):
            resp = api_client.get("/api/data-status/honest-coverage?date=2026-05-15")

        assert resp.status_code == 404
        assert paths == ["2026-05-15/coverage.json"]  # exactly one read, the requested date


class TestFallbackProvenanceFields:
    """`requested_date`/`resolved_date` let the card state "today's file" vs a
    14-day-fallback file exactly, instead of inferring staleness from `date`."""

    def test_direct_hit_reports_requested_equal_to_resolved(self, api_client: TestClient) -> None:
        """The requested day's own file: both fields are that day -> not a fallback."""
        with patch(_PATCH, return_value=json.dumps(SAMPLE_COVERAGE)):
            resp = api_client.get("/api/data-status/honest-coverage?date=2026-05-15")

        assert resp.status_code == 200
        body = resp.json()
        assert body["requested_date"] == "2026-05-15"
        assert body["resolved_date"] == "2026-05-15"

    def test_walk_back_reports_the_day_actually_served(self, api_client: TestClient) -> None:
        """An un-dated request that falls back reports the requested day (today)
        AND the older day whose file was really served — the pair the card needs."""
        paths: list[str] = []

        def _capture(_bucket: str, path: str) -> str:
            paths.append(path)
            if len(paths) < 3:  # today + today-1 absent; today-2 present
                raise FileNotFoundError("blob not found")
            return json.dumps(SAMPLE_COVERAGE)

        with patch(_PATCH, side_effect=_capture):
            resp = api_client.get("/api/data-status/honest-coverage")

        assert resp.status_code == 200
        body = resp.json()
        today = datetime.now(UTC).date()
        assert body["requested_date"] == today.isoformat()
        assert body["resolved_date"] == (today - timedelta(days=2)).isoformat()
        # The served file is genuinely older than the day asked for -> a fallback.
        assert body["resolved_date"] < body["requested_date"]

    def test_provenance_is_additive_and_preserves_the_payload(self, api_client: TestClient) -> None:
        """Every pre-existing field survives untouched — the writer's own `date`
        is NOT overwritten by the resolved date, and the coverage math is intact."""
        with patch(_PATCH, return_value=json.dumps(SAMPLE_COVERAGE)):
            resp = api_client.get("/api/data-status/honest-coverage?date=2026-05-15")

        body = resp.json()
        for key, value in SAMPLE_COVERAGE.items():
            assert body[key] == value, f"{key} was mutated by the provenance enrichment"
        assert set(body) - set(SAMPLE_COVERAGE) == {"requested_date", "resolved_date"}

    def test_non_object_payload_is_served_verbatim(self, api_client: TestClient) -> None:
        """A non-object JSON payload has nothing to hang provenance on — it is
        passed through unreshaped rather than being wrapped or rejected."""
        with patch(_PATCH, return_value="[1, 2, 3]"):
            resp = api_client.get("/api/data-status/honest-coverage?date=2026-05-15")

        assert resp.status_code == 200
        assert resp.json() == [1, 2, 3]
