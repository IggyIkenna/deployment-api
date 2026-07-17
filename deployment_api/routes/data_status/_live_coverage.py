"""Live-pipeline status / honest-coverage / venue-year-coverage data-status routes.

Split from ``routes/data_status.py`` (pure code motion; plan:
``codex_violations_ratchet_to_five_2026_06_10.md`` Phase-1 P2). Routes
register on the package facade's shared ``router``; patched module-level
collaborators are resolved through the facade module (``_ds``) at call
time so the existing test patch surface keeps intercepting.
"""

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Final, Literal, cast

import httpx
import pandas as pd
from fastapi import HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel, Field

import deployment_api.routes.data_status as _ds
from deployment_api.routes.data_status import router
from deployment_api.routes.data_status._coverage_scope import (
    ConfigVersionTriple,
    CoverageScope,
    config_versions,
    filter_to_mvp,
)
from deployment_api.services.data_status_union import (
    has_provenance_columns,
    provenance_breakdown,
    union_reduce_to_cells,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Live-pipeline data-status endpoint — Phase 11.1 design-only stub.
#
# Plan: live_pipeline_mtds_mdps_features_2026_05_08.md Phase 11.
# Implementation is BLOCKED on Phase 5/6 (live features-asset-scoped +
# cross-cutting runners) shipping per features-consolidation Phase 7. This
# stub returns an empty list with the correct shape so the deployment-ui
# `LiveDataStatusTab` (Phase 11.3) can render against the contract before
# the live pipeline is producing real shards.
# ---------------------------------------------------------------------------


# Closed set of capture-status states per writegate Phase 3.D.5 4-state
# taxonomy (CLAUDE.md "Availability manifest v5 (honest-coverage)"). Lifted
# inline here to keep this Phase-11 stub self-contained until the UAC
# `CaptureStatus` Literal is unified across the dashboard tier; per
# CLAUDE.md Citadel Rule 7 (SSOT) we converge on UAC once Phase 5/6
# implementation makes the live producer publish live capture_status rows.
LiveCaptureStatus = Literal[
    "captured",
    "empty_confirmed",
    "attempted_failed",
    "expected_unattempted",
]


class LiveStatusRow(BaseModel):  # CORRECT-LOCAL: deployment-ui Live-tab response row; TS consumer only
    """One per-shard live-pipeline status row for the deployment-UI Live tab.

    Phase 11.1 endpoint contract per
    ``live_pipeline_mtds_mdps_features_2026_05_08.md`` Phase 11. The
    endpoint pivots the availability manifest by the live-pipeline-mode
    family (any ``pipeline_mode`` whose value begins with ``live`` —
    ``live_<source>`` such as ``live_binance``, plus the legacy alias)
    and joins per-shard health from the Health-API endpoints (Phase 8
    already shipped at UTL@54d658e8 + UTL@908b1647).

    Shard-key axes mirror the v5 manifest row key (per CLAUDE.md
    "Shard-granularity SSOT"). Per-shard health metrics are sourced from
    the consumer service's :func:`make_health_router`'s
    ``data_freshness`` callback (per CLAUDE.md "Service Infrastructure
    Requirements"):

    * ``staleness_seconds`` — wall-clock seconds since the last
      :class:`~unified_api_contracts.events.streaming.CandleComputedEvent`
      for the shard.
    * ``degraded_ratio_60s`` — fraction of the last-60s emissions where
      ``emission_outcome="PUBLISHED_DEGRADED"`` (per CLAUDE.md service-
      emission-policy SSOT). Higher means more WS reconnects + carry-
      forward LTP bars (stale-not-missing rule fires).
    * ``cluster_pct_skipped_60s`` — for bundled shards (options_chain /
      futures_chain / prediction canonical-question-group / sports per-
      fixture-bundle), fraction of expected_root_clusters that did NOT
      receive a CandleComputed in the last 60s. 0% = full cluster
      coverage; 100% = no-emit per Phase 4.3 Cat (A').
    * ``last_candle_emitted_at`` — most recent CandleComputedEvent
      ``available_at`` for the shard.

    DESIGN-ONLY: the live endpoint currently returns an empty list; the
    fields above land once the per-asset-group MDPS+features-asset-scoped
    triplets publish to ``streaming.{asset_group}.candle_computed`` per
    Phase 4 + 5 implementation.
    """

    asset_group: str
    venue: str
    chain: str | None = None
    data_type: str
    instrument_type: str | None = None
    instrument_id: str | None = None
    league_id: str | None = None
    timeframe: str
    feature_group: str | None = None
    capture_status: LiveCaptureStatus
    staleness_seconds: float = Field(
        ...,
        ge=0,
        description=(
            "Wall-clock seconds since the last CandleComputedEvent for this "
            "shard. NaN-equivalent (set to +inf in implementation) when no "
            "event has ever been seen."
        ),
    )
    degraded_ratio_60s: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description=("Fraction of last-60s emissions with emission_outcome=PUBLISHED_DEGRADED."),
    )
    cluster_pct_skipped_60s: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description=(
            "For bundled shards: fraction of expected_root_clusters that "
            "did NOT receive a CandleComputed in the last 60s. 0 for "
            "non-bundled shards."
        ),
    )
    last_candle_emitted_at: datetime | None = None


class LiveStatusResponse(BaseModel):  # CORRECT-LOCAL: deployment-ui Live-tab response envelope; TS consumer only
    """``GET /api/data-status/live`` response envelope."""

    status: Literal["ok"] = "ok"
    rows: list[LiveStatusRow] = Field(default_factory=list)
    asset_groups: list[str] = Field(
        default_factory=list,
        description=("List of asset_groups represented in `rows` (deduped)."),
    )
    refreshed_at: datetime


_LIVE_PIPELINE_MODE_PREFIX: Final[str] = "live"
"""Manifest ``pipeline_mode`` STRING-PREFIX tagging live-pipeline shards.

``pipeline_mode`` is SOURCE-AWARE (``{mode}_{source}`` — e.g.
``live_binance`` / ``live_databento``) per the G0 standardisation. The
manifest carries it as a STRING, and OLD live parquets still hold the
legacy ``live``-prefixed transitional alias string. So we MATCH ON THE
``live`` PREFIX (``pipeline_mode.startswith("live")``) to capture BOTH
that legacy alias AND every ``live_<source>`` value in one read — an
exact-equality filter would silently DROP all ``live_<source>`` rows.
"""


def _is_live_mode(pipeline_mode: object) -> bool:
    """True when a manifest ``pipeline_mode`` cell is a live-pipeline mode.

    String-prefix match (``startswith("live")``) so it captures every
    ``live_<source>`` value (``live_binance`` / ``live_databento`` / …)
    AND the legacy ``live`` alias carried by old parquets — never an
    exact-equality compare against a single literal (that would drop all
    ``live_<source>`` rows). Non-string / NaN cells are not live.
    """
    if not isinstance(pipeline_mode, str):
        return False
    return pipeline_mode.strip().lower().startswith(_LIVE_PIPELINE_MODE_PREFIX)


_ASSET_GROUPS: Final[tuple[str, ...]] = ("cefi", "defi", "tradfi", "sports", "prediction")
"""Closed set of asset_groups the live-status endpoint scans.

Mirrors CLAUDE.md "Asset-group vocabulary". The endpoint iterates one
MTDS bucket per asset_group when no filter is supplied.
"""

_LIVE_STATUS_SERVICE: Final[str] = "market-tick-data-service"
"""MTDS shares the bucket name template with MDPS per
``data_status_drilldown._BUCKET_TEMPLATES`` (both resolve to
``market-data-tick-{asset_group}-{pid}``). Reading the manifest via
``market-tick-data-service`` covers both raw-tick + MDPS-candle
live-pipeline (``live_<source>``) shards in one read.
"""


def _normalise_asset_groups(filter_values: list[str] | None) -> list[str]:
    """Resolve the asset_group filter argument to a concrete iteration list.

    Empty / ``None`` filter → all 5 asset_groups. Unknown asset_groups
    are silently dropped (per Phase 11.1 endpoint contract — the
    frontend tolerates partial responses).
    """
    if not filter_values:
        return list(_ASSET_GROUPS)
    requested = {ag.lower().strip() for ag in filter_values}
    return [ag for ag in _ASSET_GROUPS if ag in requested]


def _coerce_value(row: dict[str, object], column: str) -> object:
    """Return ``row[column]`` or ``None`` for missing/NaN cells."""
    value = row.get(column)
    if value is None:
        return None
    # pandas NaN check without importing numpy (truthy / equality patterns
    # both fail for NaN; the ``!= value`` trick is the canonical NaN test).
    try:
        if value != value:
            return None
    except (TypeError, ValueError):
        pass
    return value


def _string_or_none(row: dict[str, object], column: str) -> str | None:
    """Coerce a manifest cell to ``str | None`` (empty string → None)."""
    value = _coerce_value(row, column)
    if value is None:
        return None
    out = str(value)
    return out if out else None


def _capture_status_from(row: dict[str, object]) -> LiveCaptureStatus:
    """Coerce manifest ``capture_status`` to the closed-set Literal.

    Defaults unknown / missing values to ``captured`` per the manifest
    reader's legacy-back-compat convention (matches
    :data:`~deployment_api.services.data_status_drilldown._DEFAULT_CAPTURE_STATUS`).
    """
    raw = _string_or_none(row, "capture_status") or "captured"
    if raw in ("captured", "empty_confirmed", "attempted_failed", "expected_unattempted"):
        return raw  # pyright: ignore[reportReturnType]
    return "captured"


def _staleness_seconds_from(
    row: dict[str, object],
    *,
    now: datetime,
) -> float:
    """Derive per-shard ``staleness_seconds`` from the manifest ``attempted_at``.

    The manifest's ``attempted_at`` is the write-time stamp when MTDS /
    MDPS finalised the shard's parquet — a coarse proxy for
    last-event-age. The precise per-shard ``last_event_age_seconds``
    comes from the consumer service's Health-API
    :class:`~unified_trading_library.streaming.StreamingHealthSnapshot`;
    the deployment-api Health-API HTTP join lands once a per-service
    URL registry is wired in
    :class:`~deployment_api.deployment_api_config.DeploymentApiConfig`.
    Until then this manifest-derived signal is the best available data.

    Returns ``inf`` when ``attempted_at`` is missing (treats the shard
    as "never seen" → alerting tier-1 fires immediately).
    """
    raw = _coerce_value(row, "attempted_at")
    if raw is None:
        return float("inf")
    if isinstance(raw, datetime):
        attempted_at = raw
    else:
        try:
            attempted_at = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return float("inf")
    if attempted_at.tzinfo is None:
        # Treat naïve manifest timestamps as UTC (manifest writer's
        # convention).
        attempted_at = attempted_at.replace(tzinfo=UTC)
    # Normalise `now` to UTC for safe subtraction with the manifest's
    # tz-aware timestamps.
    now_utc = now if now.tzinfo is not None else now.replace(tzinfo=UTC)
    delta = now_utc - attempted_at
    return max(delta.total_seconds(), 0.0)


def _last_candle_emitted_at(row: dict[str, object]) -> datetime | None:
    """Manifest ``attempted_at`` as the surrogate for last-emitted-candle.

    Strict Phase 8 contract says the Health-API
    :class:`StreamingHealthSnapshot.last_event_age_seconds` is the SSOT;
    the manifest write-time is a coarse approximation. Refinement
    follows the Health-API HTTP join (deferred).
    """
    raw = _coerce_value(row, "attempted_at")
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _build_live_row(
    *,
    row: dict[str, object],
    asset_group: str,
    now: datetime,
) -> LiveStatusRow:
    """Build one :class:`LiveStatusRow` from a manifest row.

    Per-shard health metrics (``degraded_ratio_60s`` /
    ``cluster_pct_skipped_60s``) stay at 0.0 until the Health-API HTTP
    join lands — they require rolling-window emission stats that the
    manifest doesn't carry. ``staleness_seconds`` + ``last_candle_emitted_at``
    derive from the manifest's ``attempted_at`` (coarse proxy).
    """
    return LiveStatusRow(
        asset_group=asset_group,
        venue=str(_coerce_value(row, "venue") or ""),
        chain=_string_or_none(row, "chain"),
        data_type=str(_coerce_value(row, "data_type") or ""),
        instrument_type=_string_or_none(row, "instrument_type"),
        instrument_id=_string_or_none(row, "instrument_id"),
        league_id=_string_or_none(row, "league_id"),
        timeframe=str(_coerce_value(row, "timeframe") or ""),
        feature_group=_string_or_none(row, "feature_group"),
        capture_status=_capture_status_from(row),
        staleness_seconds=_staleness_seconds_from(row, now=now),
        degraded_ratio_60s=0.0,
        cluster_pct_skipped_60s=0.0,
        last_candle_emitted_at=_last_candle_emitted_at(row),
    )


async def _fetch_health_data_freshness(
    service_name: str,
    base_url: str,
    timeout_seconds: float,
) -> dict[str, object] | None:
    """Fetch ``data_freshness`` from one service's Health-API.

    Per Phase 8 contract: services expose ``GET /health`` returning JSON
    with a ``data_freshness`` field carrying the
    :class:`~unified_trading_library.streaming.StreamingHealthSnapshot`
    serialised dict (``last_event_age_seconds`` / ``consumer_lag_pending`` /
    ``zero_activity_bar_rate`` / etc.).

    Returns ``None`` on any failure (timeout / non-2xx / parse error /
    missing field) — the endpoint treats these as soft failures + falls
    back to the manifest-derived staleness for the affected shards.
    """
    url = base_url.rstrip("/") + "/health"
    try:
        async with httpx.AsyncClient(timeout=timeout_seconds) as client:
            response = await client.get(url, headers={"Accept": "application/json"})
        if response.status_code != 200:
            logger.warning(
                "Live data-status: %s /health returned %d (url=%s)",
                service_name,
                response.status_code,
                url,
            )
            return None
        # Health-API payload is a dynamic per-service dict (data_freshness shape
        # varies by service); validation happens structurally below, not via Pydantic.
        body = response.json()  # pyright: ignore[reportAny]  # noqa: qg-raw-json
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning(
            "Live data-status: %s /health call failed (url=%s): %s",
            service_name,
            url,
            exc,
        )
        return None
    if not isinstance(body, dict):
        return None
    freshness = body.get("data_freshness")  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
    if not isinstance(freshness, dict):
        return None
    return freshness  # pyright: ignore[reportUnknownVariableType]


async def _gather_health_data_freshness(
    service_urls: dict[str, str],
    timeout_seconds: float,
) -> dict[str, dict[str, object]]:
    """Fan-out per-service ``/health`` calls in parallel.

    Returns the per-service ``data_freshness`` dicts. Services whose
    /health call fails are omitted from the result; the endpoint
    serves the manifest-derived staleness for those shards.
    """
    if not service_urls:
        return {}
    services = list(service_urls.items())
    tasks = [
        _fetch_health_data_freshness(service_name, base_url, timeout_seconds) for service_name, base_url in services
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    out: dict[str, dict[str, object]] = {}
    for (service_name, _base_url), result in zip(services, results, strict=False):
        if isinstance(result, BaseException):
            logger.warning(
                "Live data-status: %s /health gather failed: %r",
                service_name,
                result,
            )
            continue
        if result is not None:
            out[service_name] = result
    return out


def _staleness_seconds_from_health(
    freshness: dict[str, object],
) -> float | None:
    """Extract ``last_event_age_seconds`` from one Health-API snapshot.

    Returns ``None`` when the field is missing or non-numeric — caller
    falls back to the manifest-derived staleness.
    """
    raw = freshness.get("last_event_age_seconds")
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    try:
        return float(raw)  # pyright: ignore[reportArgumentType]
    except (TypeError, ValueError):
        return None


def _read_live_manifest_rows(asset_group: str) -> list[object]:
    """Read MTDS manifest for one asset_group, filtered to live-pipeline (``live_<source>``) rows.

    Returns an empty list when the manifest is unreachable, missing the
    ``pipeline_mode`` column (pre-v8 manifest), or contains no live
    rows. Mirrors the resilient-read shape used by
    :func:`deployment_api.services.data_status_drilldown.build_mtds_shard_csv_export`
    so the endpoint stays responsive even when one asset_group's bucket
    is missing.
    """
    # Call-time patch surface: tests/unit/test_data_status_live.py patches
    # unified_trading_library.read_availability_index with `with patch(...)` at
    # call time -- a module-top binding would not be intercepted. Stays lazy.
    from unified_trading_library import read_availability_index  # noqa: imports-inside-functions

    from deployment_api.services.data_status_drilldown import build_bucket_name

    try:
        bucket = build_bucket_name(_LIVE_STATUS_SERVICE, asset_group)
    except ValueError:
        return []
    try:
        df = read_availability_index(bucket)
    except (OSError, RuntimeError, ValueError) as exc:
        logger.warning(
            "Live data-status: manifest read failed for %s/%s: %s",
            _LIVE_STATUS_SERVICE,
            asset_group,
            exc,
        )
        return []
    if len(df) == 0:  # pyright: ignore[reportUnnecessaryComparison]
        return []
    if "pipeline_mode" not in df.columns:
        # Pre-v8 manifest (no pipeline_mode column) → no live shards by
        # definition; return empty.
        return []
    # STRING-PREFIX match (not exact-equality) so we capture every
    # ``live_<source>`` value AND the legacy ``live``-prefixed alias
    # string in old parquets — an exact filter would drop all
    # ``live_<source>`` rows.
    live_df = df[df["pipeline_mode"].map(_is_live_mode)]  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
    if len(live_df) == 0:
        return []
    return list(live_df.to_dict(orient="records"))


@router.get("/live", response_model=LiveStatusResponse)
async def get_live_data_status(
    asset_group: list[str] | None = Query(
        None,
        description=(
            "Filter to specific asset_groups (closed set: cefi / defi / tradfi / sports / prediction). Default: all."
        ),
    ),
) -> LiveStatusResponse:
    """Live-pipeline data-status pivoted by the ``live_<source>`` mode family.

    Phase 11.1 endpoint per
    ``live_pipeline_mtds_mdps_features_2026_05_08.md`` Phase 11. Reads
    the availability manifest for each requested asset_group, filters to
    rows whose ``pipeline_mode`` STRING begins with ``live`` (every
    ``live_<source>`` value plus the legacy alias, via a prefix match —
    never an exact-equality compare that would drop ``live_<source>``
    rows), and returns one :class:`LiveStatusRow` per shard.

    Sources:

    * **Shard-key axes** (asset_group / venue / chain / data_type /
      instrument_type / instrument_id / league_id / timeframe /
      feature_group) — from the v8 manifest columns directly.
    * **``capture_status``** — from the writegate Phase 3.D.5 4-state
      taxonomy column (defaults to ``captured`` for legacy rows).
    * **``staleness_seconds`` + ``last_candle_emitted_at``** — derived
      from the manifest's ``attempted_at`` (write-time stamp). A coarse
      proxy for last-event-age; precise per-shard
      :class:`~unified_trading_library.streaming.StreamingHealthSnapshot.last_event_age_seconds`
      requires an HTTP join against each consumer service's
      :func:`~unified_trading_library.feature_service_base.build_health_router`
      ``data_freshness`` callback. **The HTTP join is DEFERRED** until
      a per-service URL registry lands in
      :class:`~deployment_api.deployment_api_config.DeploymentApiConfig`
      — tracked under ``live_pipeline_mtds_mdps_features_2026_05_08.md``
      Phase 11.1 follow-ups.
    * **``degraded_ratio_60s`` + ``cluster_pct_skipped_60s``** — stay
      at ``0.0`` until the Health-API HTTP join ships (these require
      rolling-window emission stats not carried in the manifest).

    Returns an empty ``rows`` list when:

    * No requested asset_group's MTDS bucket has the v8 ``pipeline_mode``
      column (pre-2026-05-08 manifests).
    * No live producers have published yet (Phase 5/6 implementation
      via Harsh slot 5 has not landed per-service consumer wire-in).
    * Manifest reads fail (logged + treated as empty).

    The endpoint stays responsive even when some asset_group buckets
    are unreachable — failures are logged + the row-list aggregates
    across the reachable buckets.
    """

    requested_asset_groups = _normalise_asset_groups(asset_group)
    now = datetime.now(UTC)

    # Phase 11.1 Health-API HTTP join — fan-out one /health call per
    # configured service in parallel. Empty URL registry → manifest-only
    # fallback (no override; staleness derived from `attempted_at`).
    per_service_freshness = await _gather_health_data_freshness(
        service_urls=_ds._cfg.live_pipeline_service_urls,  # pyright: ignore[reportPrivateUsage]
        timeout_seconds=_ds._cfg.live_pipeline_health_timeout_seconds,  # pyright: ignore[reportPrivateUsage]
    )
    # When any service supplied a precise `last_event_age_seconds`, prefer
    # it over the coarse manifest-derived value. Across-shard for now —
    # per-shard Health-API snapshots require a follow-up shape change.
    precise_staleness_seconds: float | None = None
    for freshness in per_service_freshness.values():
        candidate = _staleness_seconds_from_health(freshness)
        if candidate is None:
            continue
        if precise_staleness_seconds is None or candidate < precise_staleness_seconds:
            precise_staleness_seconds = candidate

    all_rows: list[LiveStatusRow] = []
    seen_asset_groups: list[str] = []
    for ag in requested_asset_groups:
        manifest_rows = _read_live_manifest_rows(ag)
        if not manifest_rows:
            continue
        seen_asset_groups.append(ag)
        for row in manifest_rows:
            live_row = _build_live_row(row=cast(dict[str, object], row), asset_group=ag, now=now)  # pyright: ignore[reportArgumentType]
            if precise_staleness_seconds is not None:
                live_row = live_row.model_copy(
                    update={"staleness_seconds": precise_staleness_seconds},
                )
            all_rows.append(live_row)

    return LiveStatusResponse(
        rows=all_rows,
        asset_groups=seen_asset_groups,
        refreshed_at=now,
    )


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


# ---------------------------------------------------------------------------
# §4 venue x year coverage breakdown (deployment_ui plan §4 P1)
# ---------------------------------------------------------------------------

_VENUE_YEAR_COVERAGE_ASSET_GROUPS = ("cefi", "tradfi", "defi", "sports", "prediction")
_BLOCKED_CREDENTIALS_REASON = "blocked_credentials"


class VenueYearRow(BaseModel):  # CORRECT-LOCAL: deployment-ui venue-year drilldown row; TS consumer only
    venue: str
    asset_group: str
    year: int
    captured: int = 0
    empty_confirmed: int = 0
    expected_unattempted: int = 0
    pending_paid_key: int = 0
    attempted_failed: int = 0
    total: int = 0

    @property
    def remaining(self) -> int:
        return self.total - self.captured - self.empty_confirmed - self.expected_unattempted


class VenueYearCoverageResponse(
    BaseModel
):  # CORRECT-LOCAL: deployment-ui venue-year response envelope; TS consumer only
    rows: list[VenueYearRow]
    asset_groups_loaded: list[str]
    asset_groups_failed: list[str]
    scope: CoverageScope = "could_exist"
    config_versions: dict[str, ConfigVersionTriple] = Field(default_factory=config_versions)
    source_breakdown: list[dict[str, object]] = Field(
        default_factory=list,
        description=(
            "FLAG-1 per-(pipeline_mode, source) capture-status CELL breakdown across the "
            "loaded asset_groups (CeFi multi-source UNION provenance). Empty on a v8 "
            "manifest carrying no source/pipeline_mode columns."
        ),
    )


@router.get("/venue-year-coverage", response_model=VenueYearCoverageResponse)
async def get_venue_year_coverage(
    asset_groups: str = Query(
        "cefi,tradfi,defi",
        description="Comma-separated asset groups (cefi/tradfi/defi/sports/prediction)",
    ),
    scope: CoverageScope = Query(
        "could_exist",
        description=(
            "Coverage scope (denominator filter). 'could_exist' (DEFAULT — current "
            "behaviour) = the full 4-state could-exist denominator. 'all' = the full "
            "universe (== could_exist at this endpoint). 'mvp' = restrict to cells where "
            "UAC is_mvp(...) is True (MVP-readiness board)."
        ),
    ),
) -> VenueYearCoverageResponse:
    """Per-venue x year capture-status breakdown from the MTDS availability manifest.

    Reads ``_index/availability_index.parquet`` for each requested asset group
    and aggregates by (venue, year, capture_status).  The UI uses this to show:

    * **captured** — shards successfully downloaded.
    * **empty_confirmed** — genuinely absent (pre-listing, out-of-window, venue gap).
    * **expected_unattempted** — known-missing, not yet attempted.
    * **pending_paid_key** — ``attempted_failed`` + ``error_reason=blocked_credentials``
      (401 / expired Tardis key).  A cell with this reason must read
      "downloadable once key active", NOT "complete/empty".
    * **attempted_failed** — other failures (network, parse error, etc.).
    * **remaining** (derived) = total - captured - empty_confirmed - expected_unattempted.

    Source: ``_index/availability_index.parquet`` per MTDS bucket.

    Stale-tolerant read (item 5a): the manifest is read via the stale-tolerant
    monitoring reader — on an EMPTY live result it falls back to the consolidated
    ``_index`` blob DIRECTLY (no live-freshness gate), so a paused/stale
    consolidator does not blank the board (migration-safe, read-only).

    FLAG-1 multi-source UNION (item 5b): when the manifest carries provenance
    columns (``source`` / ``pipeline_mode`` — CeFi multi-source), the rows are
    UNION-reduced to ONE honest cell first (≥1 source ``captured`` ⇒ the cell is
    ``captured``), and a per-(pipeline_mode, source) ``source_breakdown`` is
    surfaced alongside the counts.

    Scope toggle (Item 4): ``scope=mvp`` restricts the denominator + numerator to
    UAC ``is_mvp(...)`` cells; ``could_exist`` (default) / ``all`` keep the full
    universe. The per-config ``config_versions`` triples are returned so a
    coverage delta attributes to a scope-change vs a data-change.
    """
    requested_ags = [ag.strip().lower() for ag in asset_groups.split(",") if ag.strip()]
    requested_ags = [ag for ag in requested_ags if ag in _VENUE_YEAR_COVERAGE_ASSET_GROUPS]

    rows: list[VenueYearRow] = []
    loaded: list[str] = []
    failed: list[str] = []
    source_breakdown: list[dict[str, object]] = []

    for ag in requested_ags:
        try:
            bucket = _ds.build_bucket_name("market-tick-data-service", ag)
            # Stale-tolerant monitoring read (item 5a): empty live → consolidated
            # ``_index`` blob directly. Resolved through the facade so the test
            # patch surface intercepts.
            df: pd.DataFrame = _ds._read_manifest_index(bucket)  # pyright: ignore[reportPrivateUsage]
        except Exception as exc:
            logger.warning("venue-year-coverage: failed to read manifest for %s: %s", ag, exc)
            failed.append(ag)
            continue

        if df.empty or "date" not in df.columns:
            loaded.append(ag)
            continue

        # FLAG-1 (item 5b): per-(pipeline_mode, source) breakdown BEFORE the union
        # collapse, so a multi-source CeFi cell surfaces each source's answer.
        if has_provenance_columns(df):
            for entry in provenance_breakdown(df):
                entry["asset_group"] = ag.upper()
                source_breakdown.append(entry)
            # UNION-reduce multi-(source, pipeline_mode) rows to ONE honest cell so
            # the venue x year counts are cell-grain (≥1 source captured ⇒ captured),
            # never row-grain double-counted across provenance rows.
            df = union_reduce_to_cells(df)
            if df.empty or "date" not in df.columns:
                loaded.append(ag)
                continue

        # Scope filter (Item 4): mvp → keep only UAC is_mvp cells. could_exist / all
        # keep the full enumerated universe (the manifest IS that universe).
        if scope == "mvp":
            df = filter_to_mvp(df, ag)
            if df.empty:
                loaded.append(ag)
                continue

        # Derive year from date column (YYYY-MM-DD or datetime).
        df = df.copy()
        df["_year"] = pd.to_datetime(df["date"], errors="coerce").dt.year
        df = df.dropna(subset=["_year", "venue"])
        if df.empty:
            loaded.append(ag)
            continue

        df["_year"] = df["_year"].astype(int)
        df["venue"] = df["venue"].astype(str).str.upper()

        # Normalise capture_status: default missing to "captured" (legacy rows).
        if "capture_status" not in df.columns:
            df["capture_status"] = "captured"
        else:
            df["capture_status"] = df["capture_status"].fillna("captured").astype(str)

        error_col = df["error_reason"].astype(str) if "error_reason" in df.columns else pd.Series("", index=df.index)

        # Classify: attempted_failed + blocked_credentials → pending_paid_key.
        def _classify(row: "pd.Series[object]") -> str:  # type: ignore[type-arg]
            status = str(row["capture_status"]).lower()
            if status == "attempted_failed":
                reason = str(row.get("_error_reason", "") or "").lower()  # noqa: qg-empty-fallback — optional manifest column
                if _BLOCKED_CREDENTIALS_REASON in reason:
                    return "pending_paid_key"
            return status

        df["_error_reason"] = error_col
        df["_status_key"] = df.apply(_classify, axis=1)

        # Aggregate per (venue, year, status_key).
        agg = df.groupby(["venue", "_year", "_status_key"]).size().reset_index(name="count")

        # Also compute total per (venue, year).
        totals = df.groupby(["venue", "_year"]).size().reset_index(name="total")
        totals_map: dict[tuple[str, int], int] = {
            (str(r["venue"]), int(r["_year"])): int(r["total"]) for _, r in totals.iterrows()
        }

        # Build result rows — one VenueYearRow per (venue, year).
        bucket_map: dict[tuple[str, int], dict[str, int]] = {}
        for _, agg_row in agg.iterrows():
            key = (str(agg_row["venue"]), int(agg_row["_year"]))
            if key not in bucket_map:
                bucket_map[key] = {}
            bucket_map[key][str(agg_row["_status_key"])] = int(agg_row["count"])

        for (venue, year), counts in sorted(bucket_map.items()):
            rows.append(
                VenueYearRow(
                    venue=venue,
                    asset_group=ag.upper(),
                    year=year,
                    captured=counts.get("captured", 0),
                    empty_confirmed=counts.get("empty_confirmed", 0),
                    expected_unattempted=counts.get("expected_unattempted", 0),
                    pending_paid_key=counts.get("pending_paid_key", 0),
                    attempted_failed=counts.get("attempted_failed", 0),
                    total=totals_map.get((venue, year), 0),
                )
            )

        loaded.append(ag)

    return VenueYearCoverageResponse(
        rows=rows,
        asset_groups_loaded=loaded,
        asset_groups_failed=failed,
        scope=scope,
        config_versions=config_versions(),
        source_breakdown=source_breakdown,
    )
