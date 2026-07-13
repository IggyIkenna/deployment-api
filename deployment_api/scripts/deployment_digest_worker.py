"""Daily deployment-estate digest worker — Cloud Run Job entrypoint.

Runs once a day via Cloud Scheduler cron (terraform:
``deployment-service/terraform/gcp/deployment_digest_scheduler.tf``). Builds the
per-umbrella deployment digest (LIVE up / BATCH completions+failures / PAPER
status) and emits it as an INFO ``DEPLOYMENT_DIGEST`` event via UTL ``log_event``
→ the ``lifecycle-events`` Pub/Sub topic → ni-service subscriber → Slack — the
SAME relay the deployment lifecycle alerts (#3) and the data-pipeline fleet
monitors already use. No URL to configure.

Thin wrapper over :func:`deployment_api.routes.deployment_digest.run_deployment_digest`
(the pure core, shared with the ``/api/deployments/digest/run`` endpoint), which
sets up the live-mode Pub/Sub writer itself. This worker exists so the DAILY
cadence runs in an ISOLATED Cloud Run Job — its own memory envelope, off the live
deployment-api service's request path — the same reason the data-pipeline fleet
monitors run as dedicated jobs.

Exit-0-always: a transient failure is logged and the next day's cron re-runs
(the digest is idempotent — a stateless read-and-emit). Mirrors the
``max_retries=0`` posture of the data-pipeline monitor jobs.

Image:
    Reuses the deployment-api image (the ``uts-shared-deployment-api`` image) —
    it has the inventory code + the UTL event relay natively.

Plan:
    ``unified-trading-pm/plans/active/deployment_observability_parity_live_batch_paper_2026_06_22.md`` #5

CLI:
    python -m deployment_api.scripts.deployment_digest_worker [--dry-run]
"""

from __future__ import annotations

import argparse
import logging
import sys

from deployment_api.routes.deployment_digest import run_deployment_digest

logger = logging.getLogger(__name__)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Emit the daily deployment-estate digest to Slack via log_event.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build the digest and log it, but skip the log_event emit.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Cloud Run Job entrypoint — build + emit the digest, exit 0 always."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = _parse_args(argv)
    try:
        result = run_deployment_digest(dry_run=bool(args.dry_run))
        logger.info("[deployment-digest] cron result: %s", result)
    except Exception:
        # exit-0-always; the next day's cron re-runs the idempotent digest.
        logger.exception("[deployment-digest] cron run failed — will retry on the next daily cron")
    return 0


if __name__ == "__main__":
    sys.exit(main())
