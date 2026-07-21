"""Internal endpoint: sports coverage-drift daily snapshot + comparator (Phase 8.B).

Mirrors ``_rollup.py``'s pattern — the Cloud Scheduler hits this in-service route (protected
by the same ``verify_any_auth`` applied to the shared data-status ``router``) rather than a
standalone Cloud Run Job, for consistency with this repo's established gen1-service-route
convention for scheduled compute.

Routes register on the package facade's shared ``router`` (mounted at ``/api/data-status``).
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import Query

from deployment_api.routes.data_status import router

logger = logging.getLogger(__name__)


@router.post("/sports/coverage-drift-run")
async def run_sports_coverage_drift(lookback_days: int = Query(default=7)) -> dict[str, object]:
    """Snapshot today's sports feature-calculator coverage + alert on drift vs N days back.

    Synchronous — the caller is the Cloud Scheduler with a long attempt-deadline, not a UI
    request. The actual compute (a bounded manifest-window read + a pure comparator) runs off
    the event loop via ``asyncio.to_thread`` so it never blocks other in-flight requests on
    this gen1 instance.
    """
    from deployment_api.scripts.coverage_drift_worker import run as run_coverage_drift

    logger.info("sports coverage-drift run: lookback_days=%d", lookback_days)
    result = await asyncio.to_thread(run_coverage_drift, lookback_days)
    return result
