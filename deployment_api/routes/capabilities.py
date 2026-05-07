"""API capabilities and runtime info for UI display."""

from typing import cast

import yaml
from fastapi import APIRouter, HTTPException

from deployment_api.utils.service_utils import get_config_dir
from deployment_api.utils.storage_facade import get_gcs_fuse_status

router = APIRouter()


@router.get("")
async def get_capabilities():
    """
    Get API capabilities for UI display.

    Includes GCS Fuse status: whether the API is using FUSE mounts for GCS
    reads (faster) or falling back to GCS API.
    """
    return {"gcs_fuse": get_gcs_fuse_status()}


def _service_asset_groups_from_sharding(service: str) -> dict[str, object]:
    """
    Read ``sharding.{service}.yaml`` and return the ``asset_group`` (or legacy
    ``category``) axis values for UI filters and deploy dimensions.
    """
    config_path = get_config_dir() / f"sharding.{service}.yaml"

    if not config_path.exists():
        raise HTTPException(
            status_code=404, detail=f"Sharding config not found for service: {service}"
        )

    with open(config_path) as f:
        config = cast(dict[str, object], yaml.safe_load(f))

    dimensions = cast(list[dict[str, object]], config.get("dimensions") or [])
    for dim in dimensions:
        if dim.get("name") in ("asset_group", "category"):
            values_raw: object = dim.get("values") or []
            values: list[object] = (
                cast(list[object], values_raw) if isinstance(values_raw, list) else []
            )
            return {"service": service, "asset_groups": values}

    no_ags: list[object] = []
    return {"service": service, "asset_groups": no_ags}


@router.get("/service-asset-groups/{service}")
async def get_service_asset_groups(service: str) -> dict[str, object]:
    """
    Get supported asset groups for a service from its sharding config.

    Preferred path; replaces ``/service-categories/{service}``.
    """
    try:
        return _service_asset_groups_from_sharding(service)
    except HTTPException:
        raise
    except (OSError, ValueError, RuntimeError) as e:
        raise HTTPException(status_code=500, detail=f"Failed to load sharding config: {e}") from e


@router.get("/service-categories/{service}")
async def get_service_categories(service: str) -> dict[str, object]:
    """
    Legacy alias for :func:`get_service_asset_groups` — same response shape
    (``asset_groups`` list).
    """
    try:
        return _service_asset_groups_from_sharding(service)
    except HTTPException:
        raise
    except (OSError, ValueError, RuntimeError) as e:
        raise HTTPException(status_code=500, detail=f"Failed to load sharding config: {e}") from e
