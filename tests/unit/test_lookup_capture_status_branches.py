"""Unit tests for the early-return branches of
``lookup_capture_status_for_shard``.

This complements ``test_legacy_reason_classifier_wiring.py`` (which covers
the happy paths) by exercising the four "never_attempted" early returns:

1. ``build_bucket_name`` raises ``ValueError`` (unknown service).
2. ``read_availability_index`` raises ``OSError`` / ``RuntimeError`` /
   ``ValueError`` (transient GCS failure).
3. Manifest is empty / missing the ``date`` filter column.
4. Filter masks down to zero rows (no manifest entry for the requested
   axis-tuple).

Each branch must return the canonical
``{"status": "never_attempted", "error_reason": "", "attempted_at": "",
"written_at": ""}`` dict so the caller can branch into the HTTP-404 path.
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd

from deployment_api.services.data_status_drilldown import lookup_capture_status_for_shard

_NEVER_ATTEMPTED = {
    "status": "never_attempted",
    "error_reason": "",
    "attempted_at": "",
    "written_at": "",
}


def test_lookup_capture_status_unknown_service_returns_never_attempted() -> None:
    """``build_bucket_name`` raises ValueError on unknown service → returns
    canonical never_attempted dict."""
    with patch(
        "deployment_api.services.data_status_drilldown.build_bucket_name",
        side_effect=ValueError("Unknown service: bogus-service"),
    ):
        result = lookup_capture_status_for_shard(
            service="bogus-service",
            asset_group="cefi",
            day="2026-05-07",
            venue="BYBIT",
        )
    assert result == _NEVER_ATTEMPTED


def test_lookup_capture_status_manifest_read_oserror_returns_never_attempted() -> None:
    """Transient GCS failure (OSError) → returns never_attempted (logs warning)."""
    with (
        patch(
            "deployment_api.services.data_status_drilldown.build_bucket_name",
            return_value="some-bucket",
        ),
        patch(
            "deployment_api.services.data_status_drilldown.read_availability_index",
            side_effect=OSError("connection refused"),
        ),
    ):
        result = lookup_capture_status_for_shard(
            service="market-tick-data-service",
            asset_group="cefi",
            day="2026-05-07",
            venue="BYBIT",
            data_type="trades",
        )
    assert result == _NEVER_ATTEMPTED


def test_lookup_capture_status_manifest_read_runtime_error_returns_never_attempted() -> None:
    """Manifest read RuntimeError → never_attempted."""
    with (
        patch(
            "deployment_api.services.data_status_drilldown.build_bucket_name",
            return_value="some-bucket",
        ),
        patch(
            "deployment_api.services.data_status_drilldown.read_availability_index",
            side_effect=RuntimeError("manifest corrupt"),
        ),
    ):
        result = lookup_capture_status_for_shard(
            service="market-tick-data-service",
            asset_group="cefi",
            day="2026-05-07",
            venue="BYBIT",
        )
    assert result == _NEVER_ATTEMPTED


def test_lookup_capture_status_empty_manifest_returns_never_attempted() -> None:
    """Empty DataFrame → never_attempted."""
    with (
        patch(
            "deployment_api.services.data_status_drilldown.build_bucket_name",
            return_value="some-bucket",
        ),
        patch(
            "deployment_api.services.data_status_drilldown.read_availability_index",
            return_value=pd.DataFrame(),
        ),
    ):
        result = lookup_capture_status_for_shard(
            service="market-tick-data-service",
            asset_group="cefi",
            day="2026-05-07",
            venue="BYBIT",
        )
    assert result == _NEVER_ATTEMPTED


def test_lookup_capture_status_manifest_missing_date_column_returns_never_attempted() -> None:
    """Manifest without ``date`` column (legacy v3 shape) → never_attempted."""
    legacy_manifest = pd.DataFrame(
        [
            {"venue": "BYBIT", "data_type": "trades", "capture_status": "captured"},
        ]
    )
    with (
        patch(
            "deployment_api.services.data_status_drilldown.build_bucket_name",
            return_value="some-bucket",
        ),
        patch(
            "deployment_api.services.data_status_drilldown.read_availability_index",
            return_value=legacy_manifest,
        ),
    ):
        result = lookup_capture_status_for_shard(
            service="market-tick-data-service",
            asset_group="cefi",
            day="2026-05-07",
            venue="BYBIT",
        )
    assert result == _NEVER_ATTEMPTED


def test_lookup_capture_status_no_matching_row_returns_never_attempted() -> None:
    """Manifest has rows but none match the (date, venue) filter → never_attempted."""
    manifest = pd.DataFrame(
        [
            {
                "date": "2026-05-06",  # different day
                "service_name": "market-tick-data-service",
                "venue": "BYBIT",
                "data_type": "trades",
                "capture_status": "captured",
                "error_reason": "",
                "attempted_at": "2026-05-06T12:00:00Z",
                "written_at": "2026-05-06T12:00:00Z",
            },
        ]
    )
    with (
        patch(
            "deployment_api.services.data_status_drilldown.build_bucket_name",
            return_value="some-bucket",
        ),
        patch(
            "deployment_api.services.data_status_drilldown.read_availability_index",
            return_value=manifest,
        ),
    ):
        result = lookup_capture_status_for_shard(
            service="market-tick-data-service",
            asset_group="cefi",
            day="2026-05-07",  # filter doesn't match
            venue="BYBIT",
        )
    assert result == _NEVER_ATTEMPTED
