"""Unit tests — daily deployment-estate digest (parity #5).

Covers the message builder (folds per-umbrella rollups into the digest text) and
the cron core's status branches (empty / dry_run / emitted), asserting the emit
goes through UTL ``log_event`` as an INFO ``DEPLOYMENT_DIGEST`` event → Pub/Sub →
ni-service → Slack (no HTTP URL).

Plan: deployment_observability_parity_live_batch_paper_2026_06_22.md #5.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

from deployment_api.routes.deployment_digest import (
    build_deployment_digest_message,
    run_deployment_digest,
)
from deployment_api.routes.deployments_inventory import (
    UmbrellaStatusFailure,
    UmbrellaSummaryResponse,
)

_MODULE = "deployment_api.routes.deployment_digest"


def _summary(
    umbrella: str,
    *,
    running: int = 0,
    succeeded: int = 0,
    failed: int = 0,
    stale: int = 0,
    last_failure: str | None = None,
) -> UmbrellaSummaryResponse:
    counts: dict[str, int] = {}
    if running:
        counts["running"] = running
    if succeeded:
        counts["succeeded"] = succeeded
    if failed:
        counts["failed"] = failed
    if stale:
        counts["stale"] = stale
    return UmbrellaSummaryResponse(
        umbrella=umbrella,
        total=running + succeeded + failed + stale,
        counts_by_status=counts,
        stale_count=stale,
        last_failure=(
            UmbrellaStatusFailure(name=last_failure, exit_code=1, last_run_at="2026-07-13T06:00:00Z")
            if last_failure
            else None
        ),
    )


def _estate() -> list[UmbrellaSummaryResponse]:
    return [
        _summary("LIVE", running=12),
        _summary("BATCH", succeeded=44, failed=3, stale=1, last_failure="tradfi-cme-cl-backfill"),
        _summary("PAPER", running=5),
    ]


# ---------------------------------------------------------------------------
# message builder
# ---------------------------------------------------------------------------


def test_build_digest_message_folds_all_umbrellas() -> None:
    msg = build_deployment_digest_message(_estate(), digest_date=date(2026, 7, 13))
    assert "DEPLOYMENT DIGEST" in msg
    assert "65 targets across 3 umbrellas, 3 failed" in msg
    assert "LIVE: 12 total" in msg
    assert "BATCH: 48 total" in msg
    assert "PAPER: 5 total" in msg
    assert "2026-07-13" in msg
    assert "tradfi-cme-cl-backfill" in msg


def test_build_digest_message_clean_estate_zero_failed() -> None:
    clean = [_summary("LIVE", running=10), _summary("BATCH", succeeded=20), _summary("PAPER", running=3)]
    msg = build_deployment_digest_message(clean, digest_date=date(2026, 7, 13))
    assert "0 failed" in msg


# ---------------------------------------------------------------------------
# cron core — status branches
# ---------------------------------------------------------------------------


def test_run_digest_empty_estate_is_honest_noop() -> None:
    empty = [_summary("LIVE"), _summary("BATCH"), _summary("PAPER")]
    with (
        patch(f"{_MODULE}.build_estate_summaries", return_value=empty),
        patch(f"{_MODULE}.log_event") as mock_log_event,
    ):
        out = run_deployment_digest(dry_run=False)
    assert out["status"] == "empty"
    assert out["total_targets"] == 0
    mock_log_event.assert_not_called()


def test_run_digest_dry_run_builds_but_does_not_emit() -> None:
    with (
        patch(f"{_MODULE}.build_estate_summaries", return_value=_estate()),
        patch(f"{_MODULE}.log_event") as mock_log_event,
        patch(f"{_MODULE}._ensure_live_events") as mock_setup,
    ):
        out = run_deployment_digest(dry_run=True)
    assert out["status"] == "dry_run"
    assert out["total_targets"] == 65
    assert "DEPLOYMENT DIGEST" in str(out["message"])
    mock_log_event.assert_not_called()
    mock_setup.assert_not_called()


def test_run_digest_emits_deployment_digest_event() -> None:
    with (
        patch(f"{_MODULE}.build_estate_summaries", return_value=_estate()),
        patch(f"{_MODULE}._ensure_live_events", return_value=True),
        patch(f"{_MODULE}.log_event") as mock_log_event,
    ):
        out = run_deployment_digest(dry_run=False)
    assert out["status"] == "emitted"
    assert out["total_targets"] == 65
    mock_log_event.assert_called_once()
    (event_name,), kwargs = mock_log_event.call_args
    assert event_name == "DEPLOYMENT_DIGEST"
    assert kwargs["severity"] == "INFO"
    assert "DEPLOYMENT DIGEST" in str(kwargs["details"]["message"])
    assert kwargs["details"]["failed"] == 3
    assert kwargs["details"]["source"] == "deployment-digest"


def test_run_digest_not_configured_when_events_setup_fails() -> None:
    with (
        patch(f"{_MODULE}.build_estate_summaries", return_value=_estate()),
        patch(f"{_MODULE}._ensure_live_events", return_value=False),
        patch(f"{_MODULE}.log_event") as mock_log_event,
    ):
        out = run_deployment_digest(dry_run=False)
    assert out["status"] == "not_configured"
    mock_log_event.assert_not_called()
