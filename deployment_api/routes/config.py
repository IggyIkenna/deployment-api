"""
Configuration API Routes

Endpoints for venues, expected start dates, dependencies, and region config.
"""

import asyncio
import logging
from pathlib import Path
from typing import cast

import yaml
from fastapi import APIRouter, FastAPI, HTTPException, Query, Request

from deployment_api import settings
from deployment_api.config_loader import ConfigLoader
from deployment_api.utils.cache import TTL_SERVICE_CONFIG, cache

logger = logging.getLogger(__name__)

router = APIRouter()

# Module-level cached ConfigLoader (avoids re-creating per request)
_config_loader: ConfigLoader | None = None


def get_config_loader(request: Request) -> ConfigLoader:
    """Get shared ConfigLoader instance using app's config directory."""
    global _config_loader
    if _config_loader is None:
        config_dir = cast(Path, cast(FastAPI, request.app).state.config_dir)
        _config_loader = ConfigLoader(str(config_dir))
    return _config_loader


@router.get("/region")
async def get_region_config():
    """
    Return the configured GCS region for UI validation.

    Used by DeployForm and DataStatusTab to warn when user selects a different region
    (cross-region egress costs).
    """
    return {
        "gcs_region": settings.GCS_REGION,
        "zones": [f"{settings.GCS_REGION}-{s}" for s in ["a", "b", "c"]],
        "enforce_single_region": settings.ENFORCE_SINGLE_REGION,
        "deployment_env": settings.DEPLOYMENT_ENV,
    }


@router.get("/validate-region")
async def validate_region(
    requested_region: str = Query(..., description="Region user wants to deploy to"),
):
    """
    Validate requested deployment region against GCS region.

    Returns cross_region flag and warning message when deploying to a different
    region than GCS (incurs egress costs).
    """
    gcs_region = settings.GCS_REGION
    cross_region = requested_region != gcs_region
    warning = None
    if cross_region and settings.WARN_CROSS_REGION_EGRESS:
        warning = (
            f"Deploying to {requested_region} will incur cross-region egress costs "
            f"as GCS is in {gcs_region}. Use {gcs_region} to avoid egress charges. "
            "Zone failover (1a → 1b → 1c) provides high availability within the region."
        )
    return {
        "requested_region": requested_region,
        "gcs_region": gcs_region,
        "cross_region": cross_region,
        "warning": warning,
    }


@router.get("/venues")
async def get_venues(request: Request):
    """
    Get all venues organized by asset group.

    Returns:
        Venues grouped by asset group (CEFI, TRADFI, DEFI, …) with their data types.
    """

    def _load_venues_sync():
        loader = get_config_loader(request)
        return loader.load_venues_config()

    try:
        venues_config = await asyncio.to_thread(_load_venues_sync)
        if isinstance(venues_config, dict):
            out = dict(venues_config)
            # YAML on disk may still use ``categories``; public API is ``asset_groups``.
            if "categories" in out and "asset_groups" not in out:
                out["asset_groups"] = out.pop("categories")
            return out
        return venues_config
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Failed to load venues config")
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e


@router.get("/venues/{asset_group}")
async def get_venues_by_asset_group(asset_group: str, request: Request):
    """
    Get venues for a specific asset group.

    Args:
        asset_group: Asset group (CEFI, TRADFI, DEFI, …)

    Returns:
        List of venues and their data types for the asset group.
    """

    def _load_asset_group_sync() -> dict[str, object] | None:
        from typing import cast

        loader = get_config_loader(request)
        venues_config = loader.load_venues_config()
        groups_raw: object = venues_config.get("asset_groups") or venues_config.get("categories") or {}
        groups = cast(dict[str, object], groups_raw) if isinstance(groups_raw, dict) else {}

        # Case-insensitive lookup; YAML may use ``categories`` or ``asset_groups``.
        for ag_name, ag_data_raw in groups.items():
            if ag_name.upper() == asset_group.upper():
                ag_data = cast(dict[str, object], ag_data_raw) if isinstance(ag_data_raw, dict) else {}
                return {
                    "asset_group": ag_name,
                    "venues": ag_data.get("venues") or [],
                    "data_types": ag_data.get("data_types") or [],
                }
        return None

    try:
        result = await asyncio.to_thread(_load_asset_group_sync)
        if result is None:
            raise HTTPException(status_code=404, detail=f"Asset group '{asset_group}' not found")
        return result
    except HTTPException:
        raise
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Failed to load venues by asset group")
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e


@router.get("/expected-start-dates")
async def get_expected_start_dates(request: Request):
    """
    Get expected data start dates for all services.

    Returns:
        Start dates by service, category, and venue.
    """
    config_dir = cast(Path, cast(FastAPI, request.app).state.config_dir)
    start_dates_path: Path = config_dir / "expected_start_dates.yaml"

    if not start_dates_path.exists():
        raise HTTPException(status_code=404, detail="expected_start_dates.yaml not found")

    def _load_yaml_sync() -> dict[str, object]:
        with open(start_dates_path) as f:
            return cast(dict[str, object], yaml.safe_load(f))

    try:
        data = await asyncio.to_thread(_load_yaml_sync)
        return data
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Failed to load expected start dates")
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e


@router.get("/expected-start-dates/{service_name}")
async def get_service_start_dates(service_name: str, request: Request):
    """
    Get expected start dates for a specific service.

    Args:
        service_name: Name of the service

    Returns:
        Start dates by category and venue for the service.
    """
    config_dir = cast(Path, cast(FastAPI, request.app).state.config_dir)
    start_dates_path: Path = config_dir / "expected_start_dates.yaml"

    if not start_dates_path.exists():
        raise HTTPException(status_code=404, detail="expected_start_dates.yaml not found")

    def _load_service_dates_sync() -> dict[str, object] | None:
        with open(start_dates_path) as f:
            data = cast(dict[str, object], yaml.safe_load(f))

        if service_name not in data:
            return None

        return {
            "service": service_name,
            "start_dates": data[service_name],
        }

    try:
        result = await asyncio.to_thread(_load_service_dates_sync)
        if result is None:
            raise HTTPException(
                status_code=404,
                detail=f"Service '{service_name}' not found in expected_start_dates.yaml",
            )
        return result
    except HTTPException:
        raise
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Failed to load service start dates for %s", service_name)
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e


@router.get("/dependencies")
async def get_dependencies(request: Request):
    """
    Get service dependency graph.

    Returns:
        Full dependencies configuration including execution order.
    """
    config_dir = cast(Path, cast(FastAPI, request.app).state.config_dir)
    deps_path: Path = config_dir / "dependencies.yaml"

    if not deps_path.exists():
        raise HTTPException(status_code=404, detail="dependencies.yaml not found")

    def _load_deps_sync() -> dict[str, object]:
        with open(deps_path) as f:
            return cast(dict[str, object], yaml.safe_load(f))

    try:
        data = await asyncio.to_thread(_load_deps_sync)
        return data
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Failed to load dependencies")
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e


def _load_service_deps_from_file(deps_path: Path, service_name: str) -> dict[str, object] | None:
    """Load and build service dependency info from a dependencies.yaml file."""
    with open(deps_path) as f:
        data = cast(dict[str, object], yaml.safe_load(f))
    services = cast(dict[str, dict[str, object]], data.get("services") or {})
    execution_order = cast(list[str], data.get("execution_order") or [])
    if service_name not in services:
        return None
    service_data = services[service_name]
    dependents: list[str] = []
    for svc_name, svc_data in services.items():
        if svc_name == service_name:
            continue
        for upstream in cast(list[dict[str, object]], svc_data.get("upstream") or []):
            if upstream.get("service") == service_name:
                dependents.append(svc_name)
                break
    dag_edges: list[dict[str, object]] = []
    for svc_name, svc_data in services.items():
        for upstream in cast(list[dict[str, object]], svc_data.get("upstream") or []):
            dag_edges.append(
                {
                    "from": upstream.get("service"),
                    "to": svc_name,
                    "required": upstream.get("required", False),
                }
            )
    return {
        "service": service_name,
        "description": service_data.get("description"),
        "upstream": service_data.get("upstream") or [],
        "outputs": service_data.get("outputs") or [],
        "external_dependencies": service_data.get("external_dependencies") or [],
        "downstream_dependents": dependents,
        "dag": {
            "nodes": list(services.keys()),
            "edges": dag_edges,
            "execution_order": execution_order,
        },
    }


@router.get("/dependencies/{service_name}")
async def get_service_dependencies(service_name: str, request: Request):
    """
    Get dependencies for a specific service.

    Args:
        service_name: Name of the service

    Returns:
        Upstream dependencies, downstream dependents, and full DAG context.
    """
    cache_key = f"config:dependencies:{service_name}"

    async def _fetch():
        config_dir = cast(Path, cast(FastAPI, request.app).state.config_dir)
        deps_path: Path = config_dir / "dependencies.yaml"
        if not deps_path.exists():
            raise HTTPException(status_code=404, detail="dependencies.yaml not found")
        result = await asyncio.to_thread(_load_service_deps_from_file, deps_path, service_name)
        if result is None:
            raise HTTPException(
                status_code=404,
                detail=f"Service '{service_name}' not found in dependencies.yaml",
            )
        return result

    try:
        return await cache.get_or_fetch(cache_key, _fetch, TTL_SERVICE_CONFIG)
    except HTTPException:
        raise
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Failed to load service dependencies for %s", service_name)
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e


@router.get("/shard-axis-matrix")
async def get_shard_axis_matrix(
    service: str | None = Query(None, description="Optional: filter to one service"),
):
    """Return the per-(service, asset_group) shard / display / primary axis SSOT.

    Plan: ``data_status_multi_axis_shard_propagation_2026_05_06.md`` Phase 2.

    SSOT lives in
    ``unified_api_contracts.registry.data_status_axis_matrix``. The
    deployment-ui DataStatusTab reads this endpoint to drive the per-service
    axis selector (DEFI → chain dropdown, sports → league_id dropdown,
    strategy/execution → job_id picker, ml → model_family + training_period,
    multi-timeframe → timeframe dropdown).

    Empty axis tuples for ``(service, asset_group)`` pairs the service does
    NOT cover (e.g. ``features-onchain-service`` only ships ``defi``)
    deliberately surface as missing keys in the response — the UI shows
    "asset_group not in scope" rather than rendering an empty panel.

    Optional ``service`` query filters the matrix to one service for
    smaller payloads.
    """
    # Lazy import keeps the route importable in mock mode where UAC
    # may not be installed (Tier 0 emulator-only setup).
    from unified_api_contracts.registry.data_status_axis_matrix import (
        BREAKDOWN_AXES,
        DISPLAY_AXES,
        PRIMARY_AXIS,
        SHARD_AXIS_MATRIX,
    )

    target = service if isinstance(service, str) and service else None

    def _filter(items: dict[tuple[str, str], object]) -> dict[str, dict[str, object]]:
        out: dict[str, dict[str, object]] = {}
        for (svc, asset_group), value in items.items():
            if target is not None and svc != target:
                continue
            out.setdefault(svc, {})[asset_group] = value
        return out

    return {
        "shard_axes": _filter(SHARD_AXIS_MATRIX),
        "display_axes": _filter(DISPLAY_AXES),
        "primary_axis": _filter(PRIMARY_AXIS),
        "breakdown_axes": _filter(BREAKDOWN_AXES),
    }
