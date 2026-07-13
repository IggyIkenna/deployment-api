"""Daily deployment-estate digest → UTL log_event → Pub/Sub → ni-service → Slack.

Parity #5 of ``deployment_observability_parity_live_batch_paper_2026_06_22.md``:
a once-a-day Slack digest that rolls the deployment estate up per **umbrella** —
LIVE (targets running/up), BATCH (completions + failures), PAPER (run status) —
plus the most-recent failing target per umbrella. The operator gets ONE morning
glance at "is everything that should be running, running? what completed/failed
overnight?" instead of watching the real-time lifecycle stream all day.

Emit path — the SAME one the deployment lifecycle alerts (#3) already use and the
data-pipeline fleet monitors use: UTL ``log_event`` publishes a ``DEPLOYMENT_DIGEST``
event (severity INFO) to the ``lifecycle-events`` Pub/Sub topic; the ni-service
``alert_subscriber`` consumes it, ``deployment_rule_for`` matches it as an INFO
deployment event, and it mirrors to ``#data-pipeline-alerts`` (channel-only, never
pages). The digest text rides in ``details["message"]``. NO HTTP URL to configure —
the relay is the proven, already-wired path.

The data source is the deployment-api inventory itself (``_load_inventory`` +
``build_umbrella_summary``) — deployment-api already owns the per-umbrella
rollups (``GET /api/deployments/umbrella/{umbrella}/summary``), so the digest
adds no new data logic, only the scheduled fold + emit.

Cron: a daily Cloud Scheduler job runs the isolated Cloud Run Job worker
(``deployment_api.scripts.deployment_digest_worker``) — see
``deployment-service/terraform/gcp/deployment_digest_scheduler.tf``. Honest
no-op when the estate is empty (logs, emits nothing — never a fabricated digest).

SSOT: ``plans/active/deployment_observability_parity_live_batch_paper_2026_06_22.md`` #5
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime

from fastapi import APIRouter, Query
from unified_api_contracts import DeploymentUmbrella
from unified_trading_library import (
    DEPLOYMENT_DIGEST,
    PubSubEventSink,
    UnifiedCloudConfig,
    log_event,
    run_lifecycle,
    setup_events,
)

from deployment_api.routes.deployments_inventory import (
    UmbrellaSummaryResponse,
    _load_inventory,
    build_umbrella_summary,
)

logger = logging.getLogger(__name__)

router = APIRouter()

_SERVICE_NAME = "deployment-digest"
_LIFECYCLE_EVENTS_TOPIC = "lifecycle-events"

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


def build_deployment_digest_message(
    summaries: list[UmbrellaSummaryResponse],
    *,
    digest_date: date,
) -> str:
    """Fold the per-umbrella rollups into the digest Slack message (a plain str).

    Rides in ``details["message"]`` on the ``DEPLOYMENT_DIGEST`` event — the
    ni-service router renders ``[DEPLOYMENT_DIGEST] {message}`` to Slack.
    """
    total_failed = sum(s.counts_by_status.get("failed", 0) for s in summaries)
    total_targets = sum(s.total for s in summaries)
    lines = " | ".join(_umbrella_line(s) for s in summaries)
    return (
        f"DEPLOYMENT DIGEST — estate [{digest_date.isoformat()}]: "
        f"{total_targets} targets across {len(summaries)} umbrellas, "
        f"{total_failed} failed. {lines}."
    )


def build_estate_summaries(now: datetime) -> list[UmbrellaSummaryResponse]:
    """Load the inventory ONCE and roll it up for every digest umbrella.

    Reuses the same ``_load_inventory`` + ``build_umbrella_summary`` seam as the
    ``/api/deployments/umbrella/{umbrella}/summary`` endpoint, but loads the
    inventory a single time (not once per umbrella).
    """
    items = _load_inventory(now)
    return [build_umbrella_summary(umbrella, items) for umbrella in _DIGEST_UMBRELLAS]


def _ensure_live_events() -> bool:
    """Best-effort: wire ``log_event`` → the ``lifecycle-events`` Pub/Sub topic.

    Mirrors the data-pipeline fleet monitor's setup exactly (the confirmed path
    whose events reach ni-service → #data-pipeline-alerts). The deployment-api
    HTTP service does not initialise events at startup, and the digest worker is
    a standalone Cloud Run Job — so the digest sets up its own live-mode writer
    here. Returns True when set up; a setup failure is logged, not raised.
    """
    try:
        project_id = getattr(UnifiedCloudConfig(), "gcp_project_id", "") or ""
        if not project_id:
            logger.warning("[deployment-digest] no gcp_project_id — events not set up; digest not emitted")
            return False
        sink = PubSubEventSink(
            project_id=project_id,
            topic=_LIFECYCLE_EVENTS_TOPIC,
            service_name=_SERVICE_NAME,
        )
        setup_events(service_name=_SERVICE_NAME, mode="live", sink=sink)
        return True
    except Exception as exc:
        logger.warning("[deployment-digest] live-events setup failed (best-effort): %s", exc)
        return False


def run_deployment_digest(*, dry_run: bool = False) -> dict[str, object]:
    """Build + emit the daily deployment-estate digest (the pure core).

    Called by BOTH the Cloud Run Job worker (deployment_digest_worker.main, the
    daily cron) and the ``/api/deployments/digest/run`` endpoint (on-demand / UI
    / dry-run preview). Loads the inventory once, rolls up LIVE / BATCH / PAPER,
    builds the digest message and emits it as an INFO ``DEPLOYMENT_DIGEST`` event
    via UTL ``log_event`` → Pub/Sub → ni-service → Slack. Returns a status
    payload so the run is auditable (``emitted`` | ``empty`` | ``dry_run`` |
    ``not_configured``).
    """
    now = datetime.now(UTC)
    summaries = build_estate_summaries(now)
    total_targets = sum(s.total for s in summaries)

    if total_targets == 0:
        # Honest empty: no deployment targets classified for any umbrella. Emit
        # nothing (no fabricated zero digest); log so the cron run is auditable.
        logger.info("[deployment-digest] estate empty (0 targets) — nothing to digest")
        return {"status": "empty", "total_targets": 0}

    message = build_deployment_digest_message(summaries, digest_date=now.date())
    total_failed = sum(s.counts_by_status.get("failed", 0) for s in summaries)

    if dry_run:
        logger.info("[deployment-digest] DRY-RUN — would emit: %s", message)
        return {"status": "dry_run", "total_targets": total_targets, "message": message}

    if not _ensure_live_events():
        return {"status": "not_configured", "total_targets": total_targets, "message": message}

    # run_lifecycle wraps the emit so the digest run gets its own STARTED/STOPPED
    # lifecycle events (auto-correlated by run_id) — the ServiceBootstrap-parity
    # pattern the data-pipeline monitor jobs use. The writer is already live
    # (PubSub) from _ensure_live_events above.
    with run_lifecycle(service_name=_SERVICE_NAME):
        log_event(
            DEPLOYMENT_DIGEST,
            severity="INFO",
            details={
                "message": message,
                "source": _SERVICE_NAME,
                "total_targets": total_targets,
                "failed": total_failed,
            },
        )
    logger.info("[deployment-digest] emitted DEPLOYMENT_DIGEST (%d targets, %d failed)", total_targets, total_failed)
    return {"status": "emitted", "total_targets": total_targets, "message": message}


@router.post("/digest/run")
def run_deployment_digest_endpoint(
    dry_run: bool = Query(default=False, description="Build the digest but skip the log_event emit."),
) -> dict[str, object]:
    """On-demand / UI trigger for the deployment-estate digest.

    Thin HTTP wrapper over :func:`run_deployment_digest`. The DAILY cadence runs
    via the isolated Cloud Run Job worker (deployment_digest_worker), not this
    endpoint — this surface is for operator-initiated / dry-run previews.
    """
    return run_deployment_digest(dry_run=dry_run)


__all__ = [
    "build_deployment_digest_message",
    "build_estate_summaries",
    "router",
    "run_deployment_digest",
    "run_deployment_digest_endpoint",
]
