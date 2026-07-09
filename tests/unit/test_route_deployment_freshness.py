"""Unit tests for GET /api/deployments/{id}/freshness — per-deployment manifest freshness.

Covers the responsibility → freshness mapping at the route + ``compute_freshness`` seam:
a NONE (gateway) deployment is liveness-only; a capture deployment reads its asset_group's
consolidated-index posture (manifest-derived freshness); an unknown asset_group degrades to
"unknown"; an unclassifiable id 404s. Mock-mode is deterministic + GCS-free.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

os.environ.setdefault("CLOUD_PROVIDER", "local")
os.environ.setdefault("CLOUD_MOCK_MODE", "true")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("DISABLE_AUTH", "true")

pytestmark = [pytest.mark.timeout(60)]

_NOW = datetime(2026, 6, 24, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
def client_freshness() -> TestClient:
    from deployment_api.routes.deployment_freshness import router

    app = FastAPI()
    app.include_router(router, prefix="/api")
    return TestClient(app, raise_server_exceptions=False)


def test_mock_mode_gateway_is_liveness_only(client_freshness: TestClient) -> None:
    """Mock mode: a gateway/api deployment resolves to liveness_only (no false fresh)."""
    resp = client_freshness.get("/api/deployments/deployment-api-gateway/freshness")
    assert resp.status_code == 200
    body = resp.json()
    assert body["responsibility"] == "none"
    assert body["freshness_status"] == "liveness_only"
    assert body["asset_group"] is None


def test_mock_mode_capture_is_fresh(client_freshness: TestClient) -> None:
    """Mock mode: a data-plane capture deployment carries a real freshness posture."""
    resp = client_freshness.get("/api/deployments/cefi-binance-spot-20260624/freshness")
    assert resp.status_code == 200
    body = resp.json()
    assert body["responsibility"] == "asset_group_capture"
    assert body["asset_group"] == "cefi"
    assert body["freshness_status"] == "fresh"
    assert body["staleness_budget_seconds"] == 86400


def test_compute_freshness_none_is_liveness_only() -> None:
    """A NONE responsibility (control-plane) → liveness_only without reading the manifest."""
    from unified_api_contracts import ShardResponsibility, ShardResponsibilityKind

    from deployment_api.routes import deployment_freshness as mod

    none_resp = ShardResponsibility(kind=ShardResponsibilityKind.NONE)
    with (
        patch.object(mod, "classify_vm_target", return_value=object()),
        patch.object(mod, "responsibility_for_deployment", return_value=none_resp),
        patch.object(mod, "consolidator_posture") as posture,
    ):
        out = mod.compute_freshness("execution-service-live", _NOW)
    assert out.freshness_status == "liveness_only"
    assert out.responsibility == "none"
    posture.assert_not_called()  # NONE never touches the manifest reader


def test_compute_freshness_capture_reads_index_posture() -> None:
    """A capture responsibility maps the asset_group's consolidator posture to freshness."""
    from unified_api_contracts import ShardResponsibility, ShardResponsibilityKind

    from deployment_api.routes import deployment_freshness as mod
    from deployment_api.routes.health_consolidator import ConsolidatorAgHealth

    cap = ShardResponsibility(kind=ShardResponsibilityKind.ASSET_GROUP_CAPTURE, asset_group="defi")
    posture = ConsolidatorAgHealth(
        asset_group="defi",
        bucket="market-data-tick-defi-prd",
        status="critical",
        index_age_seconds=9000.0,
        staleness_budget_seconds=120,
        per_vm_shard_fallback_active=True,
        last_successful_run_at="2026-06-24T09:30:00+00:00",
        detail="index 9000s (> 120s budget) while per-VM shards exist — consolidator behind/DOWN",
    )
    with (
        patch.object(mod, "classify_vm_target", return_value=object()),
        patch.object(mod, "responsibility_for_deployment", return_value=cap),
        patch.object(mod, "consolidator_posture", return_value=posture),
    ):
        out = mod.compute_freshness("defi-mtds-capture-20260624", _NOW)
    assert out.responsibility == "asset_group_capture"
    assert out.asset_group == "defi"
    assert out.freshness_status == "stale"  # critical index → stale
    assert out.per_vm_shard_fallback_active is True
    assert out.index_age_seconds == 9000.0
    assert out.oldest_available_at == "2026-06-24T09:30:00+00:00"


def test_compute_freshness_unknown_asset_group() -> None:
    """A capture responsibility for an asset_group with no availability-index → unknown."""
    from unified_api_contracts import ShardResponsibility, ShardResponsibilityKind

    from deployment_api.routes import deployment_freshness as mod

    cap = ShardResponsibility(kind=ShardResponsibilityKind.ASSET_GROUP_CAPTURE, asset_group="weather")
    with (
        patch.object(mod, "classify_vm_target", return_value=object()),
        patch.object(mod, "responsibility_for_deployment", return_value=cap),
        patch.object(mod, "consolidator_posture") as posture,
    ):
        out = mod.compute_freshness("weather-capture", _NOW)
    assert out.freshness_status == "unknown"
    posture.assert_not_called()


def test_object_delta_for_bucket_diffs_two_most_recent_dates() -> None:
    """Object-delta = manifest lookup: sums captured row_count per date, diffs the latest two."""
    import pandas as pd

    from deployment_api.routes.deployment_freshness import _object_delta_for_bucket

    index = pd.DataFrame(
        {
            "date": ["2026-06-22", "2026-06-23", "2026-06-24"],
            "row_count": [3900, 4000, 4128],
            "instrument_count": [3900, 4000, 4128],
            "capture_status": ["captured", "captured", "captured"],
        }
    )
    with patch(
        "deployment_api.routes.deployment_freshness.read_availability_index",
        return_value=index,
    ):
        delta, detail = _object_delta_for_bucket("market-data-tick-defi-prd")
    assert delta == 128
    assert "2026-06-24" in detail and "2026-06-23" in detail


def test_object_delta_for_bucket_empty_index_is_none() -> None:
    """An empty / not-yet-written manifest index → (None, reason), never a false zero."""
    import pandas as pd

    from deployment_api.routes.deployment_freshness import _object_delta_for_bucket

    with patch(
        "deployment_api.routes.deployment_freshness.read_availability_index",
        return_value=pd.DataFrame(columns=["date", "row_count", "instrument_count", "capture_status"]),
    ):
        delta, detail = _object_delta_for_bucket("market-data-tick-defi-prd")
    assert delta is None
    assert detail


def test_object_delta_for_bucket_single_date_is_none() -> None:
    """Only one distinct written date so far → nothing to diff → (None, reason)."""
    import pandas as pd

    from deployment_api.routes.deployment_freshness import _object_delta_for_bucket

    index = pd.DataFrame(
        {
            "date": ["2026-06-24"],
            "row_count": [4128],
            "instrument_count": [4128],
            "capture_status": ["captured"],
        }
    )
    with patch(
        "deployment_api.routes.deployment_freshness.read_availability_index",
        return_value=index,
    ):
        delta, detail = _object_delta_for_bucket("market-data-tick-defi-prd")
    assert delta is None
    assert "1 distinct" in detail


def test_object_delta_for_bucket_read_failure_is_none() -> None:
    """A manifest read failure degrades honestly to (None, reason), never a crash."""
    from deployment_api.routes.deployment_freshness import _object_delta_for_bucket

    with patch(
        "deployment_api.routes.deployment_freshness.read_availability_index",
        side_effect=OSError("gcs unavailable"),
    ):
        delta, detail = _object_delta_for_bucket("market-data-tick-defi-prd")
    assert delta is None
    assert "manifest read failed" in detail


def test_compute_freshness_capture_includes_object_delta() -> None:
    """compute_freshness wires the posture's bucket into the object-delta manifest lookup."""
    import pandas as pd
    from unified_api_contracts import ShardResponsibility, ShardResponsibilityKind

    from deployment_api.routes import deployment_freshness as mod
    from deployment_api.routes.health_consolidator import ConsolidatorAgHealth

    cap = ShardResponsibility(kind=ShardResponsibilityKind.ASSET_GROUP_CAPTURE, asset_group="defi")
    posture = ConsolidatorAgHealth(
        asset_group="defi",
        bucket="market-data-tick-defi-prd",
        status="ok",
        index_age_seconds=42.0,
        staleness_budget_seconds=86400,
        per_vm_shard_fallback_active=False,
        last_successful_run_at="2026-06-24T09:30:00+00:00",
        detail="index heartbeat 42s old (<= 86400s budget)",
    )
    index = pd.DataFrame(
        {
            "date": ["2026-06-23", "2026-06-24"],
            "row_count": [4000, 4128],
            "instrument_count": [4000, 4128],
            "capture_status": ["captured", "captured"],
        }
    )
    with (
        patch.object(mod, "classify_vm_target", return_value=object()),
        patch.object(mod, "responsibility_for_deployment", return_value=cap),
        patch.object(mod, "consolidator_posture", return_value=posture),
        patch.object(mod, "read_availability_index", return_value=index),
    ):
        out = mod.compute_freshness("defi-mtds-capture-20260624", _NOW)
    assert out.object_delta == 128


def test_route_unclassifiable_id_404(client_freshness: TestClient) -> None:
    """A non-mock unclassifiable deployment id → 404 (no silent default)."""
    from deployment_service.deployment_classification import UnclassifiedDeploymentError

    from deployment_api.routes import deployment_freshness as mod

    with (
        patch.object(mod, "_cfg") as mock_cfg,
        patch.object(mod, "classify_vm_target", side_effect=UnclassifiedDeploymentError("no prefix")),
    ):
        mock_cfg.is_mock_mode.return_value = False
        resp = client_freshness.get("/api/deployments/garbage-name/freshness")
    assert resp.status_code == 404
