"""Live-pipeline status data-status routes.

Split from ``routes/data_status.py`` (pure code motion; plan:
``codex_violations_ratchet_to_five_2026_06_10.md`` Phase-1 P2). Routes
register on the package facade's shared ``router``; patched module-level
collaborators are resolved through the facade module (``_ds``) at call
time so the existing test patch surface keeps intercepting.

Further split 2026-07-31 (``deployment_api_qg_size_gate_debt_2026_07_30.md``)
from the original combined ``_live_coverage.py`` (920L, over the 900L file-
size cap) into three sibling modules by endpoint: this file keeps the
``/live`` endpoint; ``/honest-coverage`` moved to ``_live_coverage_honest.py``;
``/venue-year-coverage`` moved to ``_live_coverage_venue_year.py``. Pure code
motion — no logic changes.
"""

import asyncio
import logging
from datetime import UTC, datetime
from typing import Final, Literal, cast

import httpx
from fastapi import Query
from pydantic import BaseModel, Field

import deployment_api.routes.data_status as _ds
from deployment_api.routes.data_status import router

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
    from deployment_api.services.manifest_source import DRILLDOWN_COLUMNS

    try:
        bucket = build_bucket_name(_LIVE_STATUS_SERVICE, asset_group)
    except ValueError:
        return []
    try:
        # read_availability_index_bare_defi_callers_2026_07_27.md: this ran
        # unprojected against every asset_group's (incl. defi's up-to-1.58 GB)
        # availability index on every /live poll. DRILLDOWN_COLUMNS covers every
        # field _build_live_row reads (venue/chain/data_type/instrument_type/
        # instrument_id/league_id/timeframe/feature_group/capture_status/
        # attempted_at) plus pipeline_mode (the live-shard filter column below).
        df = read_availability_index(bucket, columns=DRILLDOWN_COLUMNS)
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
