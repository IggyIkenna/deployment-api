"""Drill-down / deploy-missing / live-cluster / turbo data-status routes.

Split from ``routes/data_status.py`` (pure code motion; plan:
``codex_violations_ratchet_to_five_2026_06_10.md`` Phase-1 P2). Routes
register on the package facade's shared ``router``; patched module-level
collaborators are resolved through the facade module (``_ds``) at call
time so the existing test patch surface keeps intercepting.
"""

import asyncio
import logging
from typing import Literal

from fastapi import HTTPException, Query, Request

import deployment_api.routes.data_status as _ds
from deployment_api.routes.data_status import router
from deployment_api.services import DataStatusService
from deployment_api.services.data_status_mock import build_mock_turbo_response
from deployment_api.services.deploy_missing import (
    DeployMissingError,
    assert_tarball_not_blocked,
)

logger = logging.getLogger(__name__)


@router.get("/drilldown/{service}/{asset_group}")
async def get_data_status_drilldown(
    service: str,
    asset_group: str,
    start_date: str = Query(..., description="ISO YYYY-MM-DD window lower bound"),
    end_date: str = Query(..., description="ISO YYYY-MM-DD window upper bound"),
    chain: str | None = Query(None, description="DEFI chain filter"),
    venue: str | None = Query(None, description="Venue filter"),
    data_type: str | None = Query(None, description="Data type filter"),
    instrument_type: str | None = Query(None, description="Instrument type filter"),
    instrument_id: str | None = Query(None, description="Instrument id filter"),
    league_id: str | None = Query(None, description="Sports league id filter"),
    feature_group: str | None = Query(None, description="Features feature_group filter"),
    feature_family: str | None = Query(
        None,
        description=(
            "Features feature_family filter (parent classification of "
            "feature_group — closed-set UAC FeatureFamily enum: calendar / "
            "commodity / cross_instrument / delta_one / multi_timeframe / "
            "onchain / sports / volatility). When omitted, all families are "
            "aggregated. Plan: features-repo consolidation Phase 8B."
        ),
    ),
    timeframe: str | None = Query(None, description="Timeframe filter"),
    canonical_question_group: str | None = Query(None, description="Prediction canonical_question_group filter"),
    pipeline_mode: str | None = Query(
        None,
        description=(
            "v9 provenance filter (G3/M5): narrow the tree to a single "
            "pipeline_mode (e.g. ``batch_databento`` / ``live_databento`` / "
            "``replay_databento``). The data-status totals always UNION across "
            "modes; this filter scopes the per-mode view."
        ),
    ),
    source: str | None = Query(
        None,
        description="v9 provenance filter (G3/M5): narrow the tree to a single source (e.g. ``databento``).",
    ),
    group_by: list[str] | None = Query(
        None,
        description=(
            "v9 provenance group-by axes (G3/M5): ``pipeline_mode`` and/or "
            "``source``, inserted at the TOP of the tree so batch-vs-live (or "
            "per-source) coverage can be compared. Repeat the param to group by "
            "both."
        ),
    ),
    expand_to_depth: int = Query(2, ge=0, le=10, description="Levels to materialise eagerly"),
    child_offset: int = Query(
        0,
        ge=0,
        description=(
            "Pagination offset into the top-level children list. "
            "Combine with ``child_limit`` to fetch the next page when a "
            "venue has thousands of instruments."
        ),
    ),
    child_limit: int | None = Query(
        None,
        ge=1,
        le=10_000,
        description=(
            "Max top-level children returned. ``None`` = no slice "
            "(return all up to the per-node cap); positive int paginates."
        ),
    ),
) -> dict[str, object]:  # pyright: ignore[reportUnknownParameterType]
    """Hierarchical shard-atom drill-down per the codex per-asset_group matrix.

    Plan: ``data_status_drilldown_shard_atom_alignment_2026_05_07`` Phase 1.
    Replaces the flat ``venue -> instrument_type -> day`` panel with a tree
    shaped per ``unified_api_contracts.registry.data_status_axis_matrix``.
    Lazy-load via filter query params: each progressively-deeper request
    returns only the matched subtree.
    """
    if _ds._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
        return {  # pyright: ignore[reportUnknownVariableType]
            "service": service,
            "asset_group": asset_group,
            "axes": [],
            "tree": [],
            "totals": {
                "captured": 0,
                "empty_confirmed": 0,
                "attempted_failed": 0,
                "total": 0,
                "completion_pct": 0.0,
            },
            "filtered_by": {},
            "mock": True,
        }
    raw_filters: dict[str, str | None] = {
        "chain": chain,
        "venue": venue,
        "data_type": data_type,
        "instrument_type": instrument_type,
        "instrument_id": instrument_id,
        "league_id": league_id,
        "feature_group": feature_group,
        "feature_family": feature_family,
        "timeframe": timeframe,
        "canonical_question_group": canonical_question_group,
        "pipeline_mode": pipeline_mode,
        "source": source,
    }
    filters: dict[str, str] = {k: v for k, v in raw_filters.items() if v is not None}
    try:
        return _ds.get_hierarchical_drilldown(
            service=service,
            asset_group=asset_group,
            window_start=start_date,
            window_end=end_date,
            filters=filters,
            expand_to_depth=expand_to_depth,
            child_offset=child_offset,
            child_limit=child_limit,
            group_by=group_by,
        )
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("drilldown(service=%s, asset_group=%s) failed", service, asset_group)
        raise HTTPException(status_code=500, detail=f"hierarchical drilldown failed: {e!s}") from e


@router.post("/deploy-missing-preview")
async def post_deploy_missing_preview(
    request: dict[str, object],
):
    """Build the surgical-recovery shell command for ONE leaf shard.

    Phase 3 ships preview-mode (operator copies + runs the command from
    their authenticated terminal); auto-launch is a follow-up plan
    pending security review of the deployment-api -> gcloud perm
    boundary. Body shape::

        {
            "service": "market-tick-data-service",
            "asset_group": "defi",
            "row_key": {"venue": "AAVE_V3-ARBITRUM",
                        "data_type": "lending_indices",
                        "instrument_id": "USDC",
                        "day": "2024-03-04"}
        }
    """
    service = str(request.get("service", ""))  # noqa: qg-empty-fallback — validated below
    asset_group = str(request.get("asset_group", ""))  # noqa: qg-empty-fallback — validated below
    raw_row_key = request.get("row_key", {})  # noqa: qg-empty-fallback — validated below
    mode = str(request.get("mode", "preview"))
    override_tarball_block = bool(request.get("override_tarball_block", False))
    if not isinstance(raw_row_key, dict):
        raise HTTPException(status_code=400, detail="row_key must be an object")
    row_key: dict[str, str] = {str(k): str(v) for k, v in raw_row_key.items()}  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
    if not service or not asset_group:
        raise HTTPException(status_code=400, detail="service and asset_group are required")
    try:
        if mode == "tarball-from-local":
            assert_tarball_not_blocked(_ds._cfg.deployment_env, override=override_tarball_block)  # pyright: ignore[reportPrivateUsage]
        preview = _ds.build_deploy_missing_preview(
            service=service,
            asset_group=asset_group,
            row_key=row_key,
            mode=mode,
        )
    except DeployMissingError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return preview.to_dict()


@router.post("/deploy-missing-launch")
async def post_deploy_missing_launch(
    request: dict[str, object],
) -> dict[str, object]:
    """Auto-launch a backfill VM for ONE leaf shard (Phase 2 auto-launch).

    Phase 0 Decision 3 rate limits: 30/op/hr · 200/op/day · 100/proj/hr.
    Per-shard idempotency: returns existing running VM if shard already in-flight.
    Blocks up to 90s waiting for the VM to emit STARTED.

    Body shape::

        {
            "service":    "market-tick-data-service",
            "asset_group": "defi",
            "row_key":    {"venue": "AAVE_V3-ARBITRUM",
                           "data_type": "lending_indices",
                           "instrument_id": "USDC",
                           "day": "2024-03-04"},
            "operator_id":         "operator@example.com",  // required
            "dry_run":             false,                   // optional
            "skip_tarball_check":  false                    // optional
        }
    """
    from deployment_api.services.deploy_missing_launch import (
        DeployMissingLaunchResult,
        DeployMissingRateLimitError,
        launch_deploy_missing_vm,
    )

    service = str(request.get("service", ""))  # noqa: qg-empty-fallback — validated below
    asset_group = str(request.get("asset_group", ""))  # noqa: qg-empty-fallback — validated below
    operator_id = str(request.get("operator_id", "unknown"))
    raw_row_key = request.get("row_key", {})  # noqa: qg-empty-fallback — validated below
    dry_run = bool(request.get("dry_run", False))
    skip_tarball_check = bool(request.get("skip_tarball_check", False))

    if not isinstance(raw_row_key, dict):
        raise HTTPException(status_code=400, detail="row_key must be an object")
    row_key: dict[str, str] = {str(k): str(v) for k, v in raw_row_key.items()}  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
    if not service or not asset_group:
        raise HTTPException(status_code=400, detail="service and asset_group are required")

    try:
        result: DeployMissingLaunchResult = launch_deploy_missing_vm(
            service=service,
            asset_group=asset_group,
            row_key=row_key,
            operator_id=operator_id,
            dry_run=dry_run,
            skip_tarball_check=skip_tarball_check,
        )
    except DeployMissingRateLimitError as e:
        raise HTTPException(status_code=429, detail=str(e)) from e
    except DeployMissingError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return result.to_dict()


@router.get("/deploy-missing-services")
async def get_deploy_missing_services():
    """List services with a registered launcher script for Deploy-Missing.

    UI consumers call this to decide which leaves render the
    Deploy-Missing button vs a "manual recovery" placeholder.
    """
    return {"services": _ds.deploy_missing_supported_services()}


@router.post("/deploy-live-cluster-preview")
async def post_deploy_live_cluster_preview(
    request: dict[str, object],
):
    """Build the launcher command for ONE live-cluster VM type (Phase 11.4).

    Live-cluster VMs deploy as a fixed topology per
    ``unified-trading-pm/codex/05-infrastructure/runtime-tiers-and-deployment.md``
    § "Live-pipeline VM topology (2026-05-08 cutover)":

    * 5 per-asset_group MTDS producers (``role="mtds-live"``)
    * 5 per-asset_group MDPS+features consumers (``role="mdps-features-live"``)
    * 1 singleton features-cross-cutting (``role="features-cross-cutting"``)
    * 1 singleton replay-cascade (``role="replay-cascade"`` — window-parameterised)

    Body shape::

        {
            "role": "mtds-live",
            "asset_group": "cefi",     # null for singleton roles
            "deployment_env": "prod",  # prod | staging | dev
            "replay_start": "...",     # required iff role=replay-cascade
            "replay_end": "...",
            "replay_shard_key": "..."
        }
    """
    role = str(request.get("role", ""))  # noqa: qg-empty-fallback — validated below
    raw_asset_group = request.get("asset_group")
    asset_group = str(raw_asset_group) if raw_asset_group not in (None, "") else None
    deployment_env = str(request.get("deployment_env", "prod"))
    replay_start_raw = request.get("replay_start")
    replay_end_raw = request.get("replay_end")
    replay_shard_key_raw = request.get("replay_shard_key")
    replay_start = str(replay_start_raw) if replay_start_raw not in (None, "") else None
    replay_end = str(replay_end_raw) if replay_end_raw not in (None, "") else None
    replay_shard_key = str(replay_shard_key_raw) if replay_shard_key_raw not in (None, "") else None
    if not role:
        raise HTTPException(status_code=400, detail="role is required")
    try:
        preview = _ds.build_live_cluster_launch_preview(
            role=role,
            asset_group=asset_group,
            deployment_env=deployment_env,
            replay_start=replay_start,
            replay_end=replay_end,
            replay_shard_key=replay_shard_key,
        )
    except DeployMissingError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return preview.to_dict()


@router.get("/deploy-live-cluster-roles")
async def get_deploy_live_cluster_roles():
    """List live-cluster roles registered in ``_LIVE_CLUSTER_LAUNCHER_SCRIPTS``.

    UI consumers call this to render the "Deploy live cluster" role-picker
    (per Phase 11.4 of live_pipeline_mtds_mdps_features_2026_05_08.md).
    """
    return {"roles": _ds.deploy_missing_supported_live_cluster_roles()}


@router.get("/drilldown-pairs")
async def get_drilldown_supported_pairs():
    """List all ``(service, asset_group)`` pairs the drill-down endpoint supports.

    UI consumers introspect this to decide which services render the
    hierarchical drill-down vs the legacy flat panel during the Phase 2
    migration.
    """
    return {"pairs": _ds.list_supported_pairs()}


@router.get("/turbo")
async def get_data_status_turbo(
    request: Request,
    service: str = Query(..., description="Service name"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    asset_group: list[str] | None = Query(None, description="Filter by asset group"),
    venue: list[str] | None = Query(None, description="Filter by venue"),
    pipeline_mode: list[str] | None = Query(
        None,
        description=(
            "Filter by pipeline_mode (OR semantics). Accepts canonical PipelineMode values "
            "e.g. 'batch_tardis', 'batch_databento', 'live_websocket'. "
            "Bypasses rollup cache; forces on-demand manifest read."
        ),
    ),
    include_sub_dimensions: bool = Query(False, description="Include sub-dimension breakdown"),
    include_instrument_types: bool = Query(False, description="Include instrument type breakdown"),
    include_file_counts: bool = Query(False, description="Include per-date file counts"),
    include_dates_list: bool = Query(False, description="Include sorted list of dates found"),
    cloud: Literal["gcp", "aws"] = Query("gcp", description="Cloud provider for bucket reads"),
):
    """Get data status with turbo mode caching (5-minute cache TTL)."""
    if _ds._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
        # Same v5-shaped mock as /manifest — keeps the Phase-C UI surfaces
        # (capture_status_counts, failure_rate_by_dimension, attempt vs
        # capture split) identically reachable whether the UI prefers
        # /turbo or /manifest. Ignores venue/include_* filters in mock —
        # the seed data is enough to drive every UI flow.
        _ = (include_sub_dimensions, include_instrument_types, include_file_counts)
        _ = (include_dates_list, venue, pipeline_mode)
        return build_mock_turbo_response(
            service=service,
            start_date=start_date,
            end_date=end_date,
            asset_groups=asset_group,
        )
    try:
        # Use manifest reader directly (faster, no CLI subprocess,
        # returns league breakdowns for sports venues).
        _pipeline_modes = pipeline_mode or None

        async def _manifest_source(
            service: str,
            start_date: str,
            end_date: str,
            asset_groups: list[str] | None = None,
            **_kw: object,
        ) -> dict[str, object]:
            return await _ds.data_status_service.get_manifest_status(
                service=service,
                start_date=start_date,
                end_date=end_date,
                asset_groups=asset_groups,
                cloud=cloud,
                pipeline_modes=_pipeline_modes,
            )

        result = await _ds.data_analytics_service.get_data_status_turbo(
            service=service,
            start_date=start_date,
            end_date=end_date,
            from_data_status_service=_manifest_source,
            asset_groups=asset_group,
            venues=venue,
            pipeline_modes=_pipeline_modes,
        )

        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])

        return result

    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in get_data_status_turbo")
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e


@router.get("/coverage-summary")
async def get_data_coverage_summary(
    service: str = Query("instruments-service", description="Service name"),
    categories: str | None = Query(None, description="Comma-separated categories (default: all)"),
):
    """Return instrument coverage summary: manifest totals + latest-day unique counts.

    Fast endpoint for the deployment UI data status overview card.
    Reads manifest parquets (totals) + latest-day parquets (unique instruments by type).
    """
    if _ds._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
        return {
            "service": service,
            "categories": {
                "CEFI": {
                    "total_shards": 25000,
                    "total_instrument_rows": 12_500_000,
                    "unique_dates": 2200,
                    "unique_venues": 18,
                    "date_range": {"start": "2019-01-01", "end": "2026-04-03"},
                    "latest_day": "2026-04-03",
                    "latest_day_instruments": {
                        "SPOT": 1200,
                        "PERPETUAL": 800,
                        "FUTURE": 600,
                        "OPTION": 1271,
                    },
                    "latest_day_total": 3871,
                },
                "TRADFI": {
                    "total_shards": 40000,
                    "total_instrument_rows": 22_000_000,
                    "unique_dates": 1800,
                    "unique_venues": 6,
                    "date_range": {"start": "2019-01-01", "end": "2026-04-03"},
                    "latest_day": "2026-04-03",
                    "latest_day_instruments": {
                        "EQUITY": 8000,
                        "ETF": 3500,
                        "INDEX": 200,
                        "OPTION": 2500,
                        "FUTURE": 514,
                    },
                    "latest_day_total": 14714,
                },
                "DEFI": {
                    "total_shards": 10000,
                    "total_instrument_rows": 3_500_000,
                    "unique_dates": 250,
                    "unique_venues": 35,
                    "date_range": {"start": "2020-01-20", "end": "2026-04-03"},
                    "latest_day": "2026-04-03",
                    "latest_day_instruments": {"LP_POOL": 1200, "LENDING_POOL": 622},
                    "latest_day_total": 1822,
                },
                "SPORTS": {
                    "total_shards": 3800,
                    "total_instrument_rows": 1_700_000,
                    "unique_dates": 36,
                    "unique_venues": 3,
                    "date_range": {"start": "2026-03-01", "end": "2026-04-03"},
                    "latest_day": "2026-04-03",
                    "latest_day_instruments": {"FIXTURE": 450},
                    "latest_day_total": 450,
                },
            },
            "totals": {
                "shards": 78800,
                "instrument_rows": 39_700_000,
                "dates_across_categories": 4286,
                "latest_day_instruments": 20857,
            },
            "mock": True,
        }

    try:
        # deployment-service boundary import kept call-time (runtime-only module, QUALITY_GATE_BYPASS_AUDIT.md § 2.5C)
        from deployment_service.cli.utils.manifest_reader import ManifestReader  # noqa: imports-inside-functions

        reader = ManifestReader()
        cat_list = [c.strip().upper() for c in categories.split(",")] if categories else None
        result = await asyncio.to_thread(
            reader.get_coverage_summary,
            service=service,
            categories=cat_list,  # pyright: ignore[reportCallIssue]
        )

        return result

    except ImportError as exc:
        logger.warning("deployment_service not available for coverage summary")
        raise HTTPException(
            status_code=501,
            detail="Coverage summary requires deployment-service package",
        ) from exc
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in get_data_coverage_summary")
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(status_code=500, detail=f"Coverage summary failed: {e}") from e


@router.get("/turbo/stats")
async def get_turbo_cache_stats():
    """Get turbo mode cache statistics."""
    try:
        return await _ds.data_analytics_service.get_cache_stats()
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in get_turbo_cache_stats")
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e


@router.post("/turbo/clear")
async def clear_turbo_cache():
    """Clear ALL data-status caches.

    Four cache layers exist; UI staleness occurs when any are skipped:
      1. data_analytics_service._turbo_cache — turbo response cache
      2. data_status_service._INDEX_CACHE — manifest parquet cache (5-min TTL)
      3. data_status_drilldown._cache — per-(date, venue, data_type) drilldown cache (5-min TTL)
      4. DataStatusService._REF_DATA_CACHE — upstream-expected-dates cache (5-min TTL, class-level)

    Reference incident 2026-05-05: PREDICTION fanout completed and canonical
    showed 100% by date, but /turbo kept returning stale 89.05% because layers
    3 and 4 were not cleared. clear_drilldown_cache was imported at module top
    but never invoked from this endpoint.
    """
    try:
        from deployment_api.services.data_status_service import (
            clear_index_cache,
            clear_rollup_cache,
        )

        clear_index_cache()
        clear_rollup_cache()
        _ds.clear_drilldown_cache()
        DataStatusService._REF_DATA_CACHE.clear()  # pyright: ignore[reportPrivateUsage]
        return await _ds.data_analytics_service.clear_cache()
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in clear_turbo_cache")
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e
