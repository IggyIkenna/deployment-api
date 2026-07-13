"""Unit tests — daily deployment-estate digest (parity #5).

Covers the pure builder (folds per-umbrella rollups into an INFO AlertEvent), the
poster (honest no-op when the URL is unset; HTTP POST when set), and the cron
endpoint's status branches (empty / dry_run / posted / no_url).

Plan: deployment_observability_parity_live_batch_paper_2026_06_22.md #5.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

from unified_api_contracts import AlertCode

from deployment_api.routes.deployment_digest import (
    build_deployment_digest_event,
    post_deployment_digest,
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
# builder
# ---------------------------------------------------------------------------


def test_build_digest_event_folds_all_umbrellas() -> None:
    event = build_deployment_digest_event(_estate(), digest_date=date(2026, 7, 13))

    assert event.severity == "INFO"
    assert event.code is AlertCode.DEPLOYMENT_DIGEST
    assert event.rule_id == "DEPLOYMENT_DIGEST"
    # metric = estate-wide failed count (BATCH's 3).
    assert event.metric_value == 3.0
    assert event.threshold == 0.0
    # Message carries every umbrella + the date + the last-failure hint.
    assert "LIVE: 12 total" in event.message
    assert "BATCH: 48 total" in event.message
    assert "PAPER: 5 total" in event.message
    assert "2026-07-13" in event.message
    assert "tradfi-cme-cl-backfill" in event.message


def test_build_digest_clean_estate_zero_metric() -> None:
    clean = [_summary("LIVE", running=10), _summary("BATCH", succeeded=20), _summary("PAPER", running=3)]
    event = build_deployment_digest_event(clean, digest_date=date(2026, 7, 13))
    assert event.metric_value == 0.0
    assert "0 failed" in event.message


# ---------------------------------------------------------------------------
# poster
# ---------------------------------------------------------------------------


def test_post_digest_noop_when_url_empty() -> None:
    event = build_deployment_digest_event(_estate(), digest_date=date(2026, 7, 13))
    with patch(f"{_MODULE}.httpx.post") as mock_post:
        result = post_deployment_digest(event, alerting_service_url="")
    assert result is None
    mock_post.assert_not_called()


def test_post_digest_posts_alertevent_when_url_set() -> None:
    event = build_deployment_digest_event(_estate(), digest_date=date(2026, 7, 13))
    resp = MagicMock()
    with patch(f"{_MODULE}.httpx.post", return_value=resp) as mock_post:
        result = post_deployment_digest(event, alerting_service_url="http://alerting-service:8080/")
    assert result is event
    resp.raise_for_status.assert_called_once()
    (url,), kwargs = mock_post.call_args
    assert url == "http://alerting-service:8080/api/v1/alerts/rules/recent"
    assert kwargs["json"]["code"] == "DEPLOYMENT_DIGEST"
    assert kwargs["json"]["severity"] == "INFO"


# ---------------------------------------------------------------------------
# cron endpoint — status branches
# ---------------------------------------------------------------------------


def test_run_digest_empty_estate_is_honest_noop() -> None:
    empty = [_summary("LIVE"), _summary("BATCH"), _summary("PAPER")]
    with patch(f"{_MODULE}.build_estate_summaries", return_value=empty), patch(f"{_MODULE}.httpx.post") as mock_post:
        out = run_deployment_digest(dry_run=False)
    assert out["status"] == "empty"
    assert out["total_targets"] == 0
    mock_post.assert_not_called()


def test_run_digest_dry_run_builds_but_does_not_post() -> None:
    with (
        patch(f"{_MODULE}.build_estate_summaries", return_value=_estate()),
        patch(f"{_MODULE}.httpx.post") as mock_post,
    ):
        out = run_deployment_digest(dry_run=True)
    assert out["status"] == "dry_run"
    assert out["total_targets"] == 65
    assert "DEPLOYMENT DIGEST" in str(out["message"])
    mock_post.assert_not_called()


def test_run_digest_posts_when_url_configured() -> None:
    resp = MagicMock()
    with (
        patch(f"{_MODULE}.build_estate_summaries", return_value=_estate()),
        patch(f"{_MODULE}.settings.ALERTING_SERVICE_URL", "http://alerting-service:8080"),
        patch(f"{_MODULE}.httpx.post", return_value=resp) as mock_post,
    ):
        out = run_deployment_digest(dry_run=False)
    assert out["status"] == "posted"
    assert out["total_targets"] == 65
    mock_post.assert_called_once()


def test_run_digest_no_url_reports_no_url_status() -> None:
    with (
        patch(f"{_MODULE}.build_estate_summaries", return_value=_estate()),
        patch(f"{_MODULE}.settings.ALERTING_SERVICE_URL", ""),
        patch(f"{_MODULE}.httpx.post") as mock_post,
    ):
        out = run_deployment_digest(dry_run=False)
    assert out["status"] == "no_url"
    mock_post.assert_not_called()
