"""Unit tests for the capture-status CSV body builders + response dispatcher
in ``routes.data_status``.

These pure helpers compose the operator-visible empty-state download bodies
(comment-row CSV with X-Capture-Status header) and the dispatcher that
routes to the right body / 404 based on the manifest's capture_status.
Pure / no IO — testable without a TestClient.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException, Response

from deployment_api.routes.data_status import (
    _attempted_failed_csv_body,
    _capture_status_response,
    _empty_confirmed_csv_body,
    _path_drift_response,
)

# ---------------------------------------------------------------------------
# _empty_confirmed_csv_body
# ---------------------------------------------------------------------------


def test_empty_confirmed_csv_body_includes_all_metadata() -> None:
    body = _empty_confirmed_csv_body(
        service="market-tick-data-service",
        asset_group="cefi",
        day="2026-05-07",
        error_reason="EXPECTED_HOLIDAY",
        attempted_at="2026-05-07T12:00:00Z",
    )
    assert "# CAPTURE_STATUS: empty_confirmed" in body
    assert "# service: market-tick-data-service" in body
    assert "# asset_group: cefi" in body
    assert "# day: 2026-05-07" in body
    assert "# error_reason: EXPECTED_HOLIDAY" in body
    assert "# attempted_at: 2026-05-07T12:00:00Z" in body


def test_empty_confirmed_csv_body_blank_reason_renders_friendly_default() -> None:
    """Blank error_reason → friendly explanation rendered in the body."""
    body = _empty_confirmed_csv_body(
        service="instruments-service",
        asset_group="sports",
        day="2017-06-15",
        error_reason="",
        attempted_at="2017-06-15T12:00:00Z",
    )
    assert "(none — source returned 0 rows)" in body


# ---------------------------------------------------------------------------
# _attempted_failed_csv_body
# ---------------------------------------------------------------------------


def test_attempted_failed_csv_body_includes_metadata() -> None:
    body = _attempted_failed_csv_body(
        service="market-tick-data-service",
        asset_group="cefi",
        day="2026-05-07",
        error_reason="RateLimitError(retry_after=60)",
        attempted_at="2026-05-07T12:00:00Z",
    )
    assert "# CAPTURE_STATUS: attempted_failed" in body
    assert "# error_reason: RateLimitError(retry_after=60)" in body
    assert "# day: 2026-05-07" in body


def test_attempted_failed_csv_body_blank_reason_renders_unclassified() -> None:
    body = _attempted_failed_csv_body(
        service="market-tick-data-service",
        asset_group="cefi",
        day="2026-05-07",
        error_reason="",
        attempted_at="2026-05-07T12:00:00Z",
    )
    assert "(unclassified)" in body


# ---------------------------------------------------------------------------
# _capture_status_response — dispatcher branching
# ---------------------------------------------------------------------------


def test_capture_status_response_captured_returns_none() -> None:
    """Happy path → caller falls through to parquet-read."""
    out = _capture_status_response(
        service="market-tick-data-service",
        asset_group="cefi",
        day="2026-05-07",
        capture_meta={"status": "captured", "error_reason": "", "attempted_at": ""},
        filename_stem="bybit_trades",
    )
    assert out is None


def test_capture_status_response_never_attempted_raises_404() -> None:
    with pytest.raises(HTTPException) as excinfo:
        _capture_status_response(
            service="market-tick-data-service",
            asset_group="cefi",
            day="2026-05-07",
            capture_meta={"status": "never_attempted", "error_reason": "", "attempted_at": ""},
            filename_stem="bybit_trades",
        )
    assert excinfo.value.status_code == 404
    assert "never ran for this shard" in excinfo.value.detail
    assert excinfo.value.headers is not None
    assert excinfo.value.headers["X-Capture-Status"] == "never_attempted"


def test_capture_status_response_empty_confirmed_returns_csv_response() -> None:
    out = _capture_status_response(
        service="market-tick-data-service",
        asset_group="cefi",
        day="2026-05-07",
        capture_meta={
            "status": "empty_confirmed",
            "error_reason": "EXPECTED_WEEKEND",
            "attempted_at": "2026-05-07T12:00:00Z",
        },
        filename_stem="cme_trades",
    )
    assert isinstance(out, Response)
    assert out.headers["X-Capture-Status"] == "empty_confirmed"
    assert "empty_confirmed" in out.headers["Content-Disposition"]
    assert out.headers["X-Row-Count"] == "0"
    body_text = out.body.decode("utf-8") if isinstance(out.body, bytes) else str(out.body)
    assert "EXPECTED_WEEKEND" in body_text


def test_capture_status_response_attempted_failed_returns_csv_response() -> None:
    out = _capture_status_response(
        service="market-tick-data-service",
        asset_group="cefi",
        day="2026-05-07",
        capture_meta={
            "status": "attempted_failed",
            "error_reason": "ConnectionError",
            "attempted_at": "2026-05-07T12:00:00Z",
        },
        filename_stem="okx_trades",
    )
    assert isinstance(out, Response)
    assert out.headers["X-Capture-Status"] == "attempted_failed"
    assert "attempted_failed" in out.headers["Content-Disposition"]
    body_text = out.body.decode("utf-8") if isinstance(out.body, bytes) else str(out.body)
    assert "ConnectionError" in body_text


def test_capture_status_response_unknown_status_raises_500() -> None:
    """Defensive: unknown capture_status from a future writer → 500."""
    with pytest.raises(HTTPException) as excinfo:
        _capture_status_response(
            service="market-tick-data-service",
            asset_group="cefi",
            day="2026-05-07",
            capture_meta={
                "status": "totally_new_status",
                "error_reason": "",
                "attempted_at": "",
            },
            filename_stem="x",
        )
    assert excinfo.value.status_code == 500
    assert "totally_new_status" in excinfo.value.detail


# ---------------------------------------------------------------------------
# _path_drift_response
# ---------------------------------------------------------------------------


def test_path_drift_response_returns_502_with_path_hint() -> None:
    exc = _path_drift_response(
        service="market-tick-data-service",
        asset_group="cefi",
        day="2026-05-07",
        expected_path_hint="gs://market-data-tick-cefi-pid/by_date/day=2026-05-07/...",
    )
    assert isinstance(exc, HTTPException)
    assert exc.status_code == 502
    assert "captured" in exc.detail
    assert "gs://market-data-tick-cefi-pid" in exc.detail
