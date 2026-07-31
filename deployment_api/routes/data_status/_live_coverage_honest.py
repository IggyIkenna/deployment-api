"""Cross-asset-group honest-coverage data-status route.

Split 2026-07-31 (``deployment_api_qg_size_gate_debt_2026_07_30.md``) out of
``_live_coverage.py`` (920L, over the 900L file-size cap) — pure code motion,
no logic changes. Patched module-level collaborators are resolved through the
facade module (``_ds``) at call time so the existing test patch surface keeps
intercepting.
"""

import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Final, cast

from fastapi import HTTPException, Query
from fastapi.responses import Response

import deployment_api.routes.data_status as _ds
from deployment_api.routes.data_status import router

logger = logging.getLogger(__name__)


def _honest_coverage_bucket() -> str:
    """Honest-coverage bucket derived from the configured project id.

    Mirrors the writer's derivation (instruments-service
    ``scripts/measure_honest_coverage.py``: ``f"{PROJECT_ID}-honest-coverage"``).
    Fails loud when ``GCP_PROJECT_ID`` is unset -- no hardcoded
    prod-project fallback (QG hardcoded-project-id ratchet).
    """
    return f"{_ds._cfg.require_gcp_project_id()}-honest-coverage"  # pyright: ignore[reportPrivateUsage]


# How many days back an UN-DATED honest-coverage request will look for the
# most recent measured coverage before giving up with a 404. The daily
# ``measure-honest-coverage`` cron has not run yet for the first several hours
# of each UTC day, so "today" is routinely absent; without this the card
# showed an empty "not yet computed" state for that whole window every morning.
_HONEST_COVERAGE_FALLBACK_LOOKBACK_DAYS: Final[int] = 14


@router.get("/honest-coverage")
async def get_honest_coverage(
    query_date: str | None = Query(
        None,
        alias="date",
        description="ISO date YYYY-MM-DD (default: latest available within the last 14 days)",
    ),
) -> Response:
    """Cross-asset-group honest coverage for a given date.

    Reads ``gs://{gcp_project_id}-honest-coverage/{date}/coverage.json``
    written daily by the ``measure-honest-coverage`` cron VM
    (``deployment-service/scripts/vm/launch-measure-honest-coverage-vm.sh``)
    and returns the JSON payload with two additive provenance fields. The
    payload carries its own ``date`` field, so the UI card always shows WHICH
    day it is rendering.

    **Fallback provenance (2026-07-17):** the response carries
    ``requested_date`` (the day the caller asked for — an explicit ``?date=``,
    else today UTC) and ``resolved_date`` (the day whose file was actually
    served). ``requested_date == resolved_date`` means "today's file";
    a difference means the walk-back below served an older measurement, which
    the card can then state exactly instead of inferring staleness from
    ``date``. Both are additive — every pre-existing field is passed through
    untouched. A payload that is not a JSON object (never written by the
    current writer) has nothing to enrich onto and is served verbatim.

    **Latest-available fallback (2026-07-15):** an un-dated request defaults to
    "today UTC", but the daily cron has not run yet for the first several hours
    of each UTC day, so a bare request 404'd and the card showed an empty
    "Coverage not yet computed" state for that whole window. To avoid that, an
    un-dated request walks BACK from today up to
    ``_HONEST_COVERAGE_FALLBACK_LOOKBACK_DAYS`` days and serves the most recent
    measured coverage (its real ``date`` travels in the payload, so nothing is
    mislabelled). An EXPLICIT ``date`` is honoured exactly (404 on miss) — no
    silent substitution of a date the caller did not ask for.

    Phase 2C per ``cross_asset_group_catalogue_audit_2026_05_10.md``.
    SSOT: ``codex/03-deployment/data-status-ui-surface.md``.

    Returns 404 when no coverage has been measured for the requested date
    (explicit date), or for any day in the lookback window (un-dated request).
    """
    from deployment_api.utils.storage_facade import read_object_text

    coverage_bucket = _honest_coverage_bucket()

    # Ordered candidate dates, most-recent first. Explicit date = exactly that
    # day (no fallback); un-dated = today then walk back through the window.
    if query_date is not None:
        candidate_dates = [query_date]
    else:
        today = datetime.now(UTC).date()
        candidate_dates = [
            (today - timedelta(days=offset)).isoformat()
            for offset in range(_HONEST_COVERAGE_FALLBACK_LOOKBACK_DAYS + 1)
        ]

    raw: str | None = None
    resolved_date: str | None = None
    for candidate in candidate_dates:
        try:
            raw = read_object_text(coverage_bucket, f"{candidate}/coverage.json")
            resolved_date = candidate
            break
        except Exception as exc:  # missing object -> try the next-oldest day
            logger.debug("honest-coverage: no data for %s: %s", candidate, exc)

    if raw is None or resolved_date is None:
        window_desc = (
            candidate_dates[0]
            if len(candidate_dates) == 1
            else f"any of the last {_HONEST_COVERAGE_FALLBACK_LOOKBACK_DAYS} days (through {candidate_dates[-1]})"
        )
        logger.info("honest-coverage: no data available for %s", window_desc)
        raise HTTPException(
            status_code=404,
            detail=f"honest-coverage data not available for {window_desc}",
        )

    if resolved_date != candidate_dates[0]:
        logger.info(
            "honest-coverage: %s absent; serving latest available measurement (%s)",
            candidate_dates[0],
            resolved_date,
        )

    try:
        parsed = cast("object", json.loads(raw))
    except Exception as exc:
        logger.warning("honest-coverage: malformed JSON for %s: %s", resolved_date, exc)
        raise HTTPException(
            status_code=500,
            detail=f"honest-coverage JSON is malformed for date={resolved_date}",
        ) from exc

    if not isinstance(parsed, dict):
        # Not an object -> no field to hang provenance on. Serve verbatim rather
        # than reshaping a payload we do not understand.
        return Response(content=raw, media_type="application/json")

    payload = cast("dict[str, object]", parsed)
    payload["requested_date"] = candidate_dates[0]
    payload["resolved_date"] = resolved_date
    return Response(content=json.dumps(payload), media_type="application/json")
