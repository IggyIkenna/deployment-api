"""Daily deployment-estate digest → AlertEvent(INFO) → alerting-service.

Parity #5 of ``deployment_observability_parity_live_batch_paper_2026_06_22.md``:
a once-a-day Slack digest that rolls the deployment estate up per **umbrella** —
LIVE (targets running/up), BATCH (completions + failures), PAPER (run status) —
plus the most-recent failing target per umbrella. The operator gets ONE morning
glance at "is everything that should be running, running? what completed/failed
overnight?" instead of watching the real-time lifecycle stream all day.

This reuses the client-reporting ``core/daily_ledger_digest.py`` pattern (build a
pure INFO ``AlertEvent`` → POST it to alerting-service over HTTP; alerting-service
routes it via the catch-all INFO rule to Slack). It is the deployment-estate
companion to ``DAILY_LEDGER_DIGEST`` (the per-client book digest).

Typed HTTP client (httpx) — NO cross-service Python import of alerting-service
(the T4 service↔service ban). The contract is the UAC ``AlertEvent`` schema, the
same ingress client-reporting posts to.

The data source is the deployment-api inventory itself (``_load_inventory`` +
``build_umbrella_summary``) — deployment-api already owns the per-umbrella
rollups (``GET /api/deployments/umbrella/{umbrella}/summary``), so the digest
adds no new data logic, only the scheduled fold + emit.

Cron: a daily Cloud Scheduler job POSTs ``/api/deployments/digest/run`` (see
``deployment-service/terraform/gcp/deployment_digest_scheduler.tf``). Honest
no-op when the estate is empty or ``ALERTING_SERVICE_URL`` is unset — never a
silent failure, never a fabricated digest.

SSOT: ``plans/active/deployment_observability_parity_live_batch_paper_2026_06_22.md`` #5
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, date, datetime

import httpx
from fastapi import APIRouter, Query
from unified_api_contracts import AlertCode, DeploymentUmbrella
from unified_api_contracts.internal import AlertEvent

from deployment_api import settings
from deployment_api.routes.deployments_inventory import (
    UmbrellaSummaryResponse,
    _load_inventory,
    build_umbrella_summary,
)

logger = logging.getLogger(__name__)

router = APIRouter()

# The alerting-service AlertEvent ingress — identical to the one client-reporting
# posts its DAILY_LEDGER_DIGEST to (core/daily_ledger_digest.py).
_ALERTS_INGRESS_PATH = "/api/v1/alerts/rules/recent"
_RULE_ID = "DEPLOYMENT_DIGEST"
_POST_TIMEOUT_SECONDS = 15
_DEFAULT_CHANNEL = "#data-pipeline-alerts"

# The umbrellas the digest rolls up, in operator reading order. EXPERIMENT folds
# under BATCH in the UI; NONE (services) is not a run-lifecycle umbrella — the
# digest is about the live/batch/paper RUN estate, mirroring the /deployments tabs.
_DIGEST_UMBRELLAS: tuple[str, ...] = (
    DeploymentUmbrella.LIVE.value,
    DeploymentUmbrella.BATCH.value,
    DeploymentUmbrella.PAPER.value,
)


def _umbrella_line(summary: UmbrellaSummaryResponse) -> str:
    """One human line summarising an umbrella's rollup for the digest message."""
    counts = summary.counts_by_status
    running = counts.get("running", 0)
    succeeded = counts.get("succeeded", 0)
    failed = counts.get("failed", 0)
    stale = summary.stale_count
    fail_hint = ""
    if summary.last_failure is not None:
        fail_hint = f" (last fail: {summary.last_failure.name})"
    return (
        f"{summary.umbrella}: {summary.total} total — {running} running, "
        f"{succeeded} succeeded, {failed} failed, {stale} stale{fail_hint}"
    )


def build_deployment_digest_event(
    summaries: list[UmbrellaSummaryResponse],
    *,
    digest_date: date,
    channel: str = _DEFAULT_CHANNEL,
) -> AlertEvent:
    """Build the deployment-estate digest ``AlertEvent`` (always INFO).

    Folds the per-umbrella rollups into a concise Slack message (per-umbrella
    totals + running/succeeded/failed/stale counts + last-failure hint). The
    ``metric_value`` is the estate-wide failed-target count (0.0 on a clean
    estate) so the operator can eyeball "did anything fail overnight" at a
    glance, and ``threshold`` is 0.0 (any failure is worth a look).

    Args:
        summaries: the per-umbrella rollups (LIVE / BATCH / PAPER).
        digest_date: the day the digest summarises (the estate as-of date).
        channel: Slack channel hint, embedded in the message like the
            client-reporting digest (alerting-service routes INFO via catch-all).
    """
    total_failed = sum(s.counts_by_status.get("failed", 0) for s in summaries)
    total_targets = sum(s.total for s in summaries)
    lines = " | ".join(_umbrella_line(s) for s in summaries)
    message = (
        f"[{channel}] DEPLOYMENT DIGEST — estate [{digest_date.isoformat()}]: "
        f"{total_targets} targets across {len(summaries)} umbrellas, "
        f"{total_failed} failed. {lines}."
    )
    return AlertEvent(
        alert_id=str(uuid.uuid4()),
        rule_id=_RULE_ID,
        triggered_at=datetime.now(UTC),
        severity="INFO",
        message=message,
        metric_value=float(total_failed),
        threshold=0.0,
        code=AlertCode.DEPLOYMENT_DIGEST,
    )


def post_deployment_digest(
    event: AlertEvent,
    *,
    alerting_service_url: str,
) -> AlertEvent | None:
    """POST the deployment digest ``AlertEvent`` to alerting-service over HTTP.

    Honest no-op (returns None, logs) when ``alerting_service_url`` is unset —
    never a silent failure, never a fabricated send.
    """
    if not alerting_service_url:
        logger.warning(
            "[deployment-digest] ALERTING_SERVICE_URL unset — digest NOT posted: %s",
            event.message,
        )
        return None
    url = f"{alerting_service_url.rstrip('/')}{_ALERTS_INGRESS_PATH}"
    resp = httpx.post(url, json=event.model_dump(mode="json"), timeout=_POST_TIMEOUT_SECONDS)
    resp.raise_for_status()
    logger.info("[deployment-digest] posted INFO digest to %s", url)
    return event


def build_estate_summaries(now: datetime) -> list[UmbrellaSummaryResponse]:
    """Load the inventory ONCE and roll it up for every digest umbrella.

    Reuses the same ``_load_inventory`` + ``build_umbrella_summary`` seam as the
    ``/api/deployments/umbrella/{umbrella}/summary`` endpoint, but loads the
    inventory a single time (not once per umbrella).
    """
    items = _load_inventory(now)
    return [build_umbrella_summary(umbrella, items) for umbrella in _DIGEST_UMBRELLAS]


def run_deployment_digest(*, dry_run: bool = False, channel: str = _DEFAULT_CHANNEL) -> dict[str, object]:
    """Build + POST the daily deployment-estate digest (the pure core).

    Called by BOTH the Cloud Run Job worker (deployment_digest_worker.main, the
    daily cron) and the ``/api/deployments/digest/run`` endpoint (on-demand / UI
    / dry-run preview). Loads the inventory once, rolls up LIVE / BATCH / PAPER,
    builds the INFO digest ``AlertEvent`` and POSTs it to alerting-service.
    Returns a status payload so the run is auditable (``posted`` | ``empty`` |
    ``no_url`` | ``dry_run``).
    """
    now = datetime.now(UTC)
    summaries = build_estate_summaries(now)
    total_targets = sum(s.total for s in summaries)

    if total_targets == 0:
        # Honest empty: no deployment targets classified for any umbrella. Emit
        # nothing (no fabricated zero digest); log so the cron run is auditable.
        logger.info("[deployment-digest] estate empty (0 targets) — nothing to digest")
        return {"status": "empty", "total_targets": 0}

    event = build_deployment_digest_event(summaries, digest_date=now.date(), channel=channel)

    if dry_run:
        logger.info("[deployment-digest] DRY-RUN — would post: %s", event.message)
        return {"status": "dry_run", "total_targets": total_targets, "message": event.message}

    posted = post_deployment_digest(event, alerting_service_url=settings.ALERTING_SERVICE_URL)
    return {
        "status": "posted" if posted is not None else "no_url",
        "total_targets": total_targets,
        "message": event.message,
    }


@router.post("/digest/run")
def run_deployment_digest_endpoint(
    dry_run: bool = Query(default=False, description="Build the digest but skip the alerting-service POST."),
    channel: str = Query(default=_DEFAULT_CHANNEL, description="Slack channel hint embedded in the digest message."),
) -> dict[str, object]:
    """On-demand / UI trigger for the deployment-estate digest.

    Thin HTTP wrapper over :func:`run_deployment_digest`. The DAILY cadence runs
    via the isolated Cloud Run Job worker (deployment_digest_worker), not this
    endpoint — this surface is for operator-initiated / dry-run previews.
    """
    return run_deployment_digest(dry_run=dry_run, channel=channel)


__all__ = [
    "build_deployment_digest_event",
    "build_estate_summaries",
    "post_deployment_digest",
    "router",
    "run_deployment_digest",
    "run_deployment_digest_endpoint",
]
