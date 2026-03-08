"""
Checklist API Routes

Endpoints for service production readiness checklists.
Reads from configs/checklist.{service}.yaml files.
"""

import asyncio
import logging
import re
from pathlib import Path
from typing import cast

import yaml
from fastapi import APIRouter, FastAPI, HTTPException, Request

logger = logging.getLogger(__name__)

router = APIRouter()


def _get_checklist_path(config_dir: Path, service_name: str) -> Path:
    """Get the path to a service's checklist file."""
    return config_dir / f"checklist.{service_name}.yaml"


def _load_checklist_yaml(config_dir: Path, service_name: str) -> dict[str, object]:
    """Load and parse a checklist YAML file."""
    checklist_path = _get_checklist_path(config_dir, service_name)

    if not checklist_path.exists():
        raise FileNotFoundError(f"Checklist not found for service: {service_name}")

    with open(checklist_path) as f:
        return cast(dict[str, object], yaml.safe_load(f))


def _format_phase_name(phase_key: str) -> str:
    """Convert phase key to display name.

    Example: phase_1_repository_foundation -> Repository Foundation
    """
    # Remove 'phase_N_' prefix
    name = re.sub(r"^phase_\d+_", "", phase_key)
    # Replace underscores with spaces and title case
    return name.replace("_", " ").title()


def _parse_checklist(checklist_data: dict[str, object]) -> dict[str, object]:  # noqa: C901
    """Parse checklist YAML into structured response.

    Returns dict with:
    - service: service name
    - last_updated: date string
    - readiness_percent: overall percentage
    - total_items: total count
    - completed_items: done count
    - partial_items: partial count
    - pending_items: pending count
    - categories: list of phase objects with items
    - blocking_items: list of items with blocking: true and status != done
    """
    service = checklist_data.get("service_name", "unknown")
    last_updated = checklist_data.get("last_updated") or ""

    categories = []
    all_items = []
    blocking_items = []

    # Find all phase_ keys
    phase_keys = [k for k in checklist_data if k.startswith("phase_")]
    phase_keys.sort()  # Ensure order

    for phase_key in phase_keys:
        phase_data = checklist_data.get(phase_key, {})
        if not isinstance(phase_data, dict):
            continue

        phase_items = []
        phase_done = 0
        phase_total = 0

        for item_key, item_data in phase_data.items():
            if not isinstance(item_data, dict):
                continue

            status = item_data.get("status", "pending")
            description = item_data.get("description", item_key)
            notes = item_data.get("notes") or ""
            verified_date = item_data.get("verified_date")
            is_blocking = item_data.get("blocking", False)

            item = {
                "id": item_key,
                "description": description,
                "status": status,
                "notes": notes,
                "verified_date": verified_date,
                "blocking": is_blocking,
            }

            phase_items.append(item)
            all_items.append(item)
            phase_total += 1

            if status == "done":
                phase_done += 1
            elif status == "partial":
                phase_done += 0.5  # Count partial as half

            # Check for blocking items
            if is_blocking and status in ("pending", "not_started"):
                blocking_items.append(
                    {
                        "id": item_key,
                        "description": description,
                        "category": _format_phase_name(phase_key),
                        "notes": notes,
                    }
                )

        phase_percent = int(phase_done / phase_total * 100) if phase_total > 0 else 100

        categories.append(
            {
                "name": phase_key,
                "display_name": _format_phase_name(phase_key),
                "percent": phase_percent,
                "total_items": phase_total,
                "completed_items": int(phase_done),
                "items": phase_items,
            }
        )

    # Calculate overall stats
    total_items = len(all_items)
    done_count = sum(1 for i in all_items if i["status"] == "done")
    partial_count = sum(1 for i in all_items if i["status"] == "partial")
    pending_count = sum(1 for i in all_items if i["status"] in ("pending", "not_started"))
    na_count = sum(1 for i in all_items if i["status"] == "n/a")

    # Calculate percentage (exclude n/a from total)
    applicable_total = total_items - na_count
    if applicable_total > 0:
        # Partial counts as half
        effective_done = done_count + (partial_count * 0.5)
        readiness_percent = int((effective_done / applicable_total) * 100)
    else:
        readiness_percent = 100

    return {
        "service": service,
        "last_updated": last_updated,
        "readiness_percent": readiness_percent,
        "total_items": total_items,
        "completed_items": done_count,
        "partial_items": partial_count,
        "pending_items": pending_count,
        "not_applicable_items": na_count,
        "categories": categories,
        "blocking_items": blocking_items,
    }


def _get_warnings(checklist: dict[str, object]) -> list[str]:
    """Extract warnings from checklist - items that are pending but not blocking."""
    warnings = []

    categories = cast(list[dict[str, object]], checklist.get("categories") or [])
    for category in categories:
        items = cast(list[dict[str, object]], category.get("items") or [])
        for item in items:
            if item["status"] in ("pending", "partial") and not item.get("blocking", False):
                warnings.append(f"{item['description']} ({item['id']})")

    return warnings[:10]  # Limit to 10 warnings


@router.get("/{service_name}/checklist")
async def get_service_checklist(service_name: str, request: Request):
    """
    Get checklist status for a service.

    Returns the production readiness checklist with progress by category.

    Args:
        service_name: Name of the service (e.g., 'instruments-service')

    Returns:
        Checklist with readiness percentage, categories, items, and blocking issues.
    """
    config_dir = cast(Path, cast(FastAPI, request.app).state.config_dir)

    def _load_sync():
        checklist_data = _load_checklist_yaml(config_dir, service_name)
        return _parse_checklist(checklist_data)

    try:
        result = await asyncio.to_thread(_load_sync)
        return result
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"Checklist not found for service '{service_name}'. "
            f"Expected file: configs/checklist.{service_name}.yaml",
        ) from None
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Internal error in get_service_checklist")
        raise HTTPException(status_code=500, detail="Internal server error") from e


@router.get("/{service_name}/checklist/validate")
async def validate_service_checklist(service_name: str, request: Request):
    """
    Validate service readiness for deployment.

    Returns a pre-deploy validation result indicating if the service
    is ready for deployment or has blocking issues.

    Args:
        service_name: Name of the service

    Returns:
        Validation result with ready status, blocking items, and warnings.
    """
    config_dir = cast(Path, cast(FastAPI, request.app).state.config_dir)

    def _validate_sync():
        checklist_data = _load_checklist_yaml(config_dir, service_name)
        checklist = _parse_checklist(checklist_data)

        blocking_items = cast(list[object], checklist.get("blocking_items") or [])
        readiness_percent = checklist.get("readiness_percent", 0)
        warnings = _get_warnings(checklist)

        # Determine if ready for deployment
        # Ready if no blocking items AND readiness >= 95%
        # 95% threshold allows AWS multi-cloud items to remain partial (GCP-first deployment)
        is_ready = len(blocking_items) == 0 and readiness_percent >= 95

        # Can proceed with acknowledgment if no blocking items (even at lower readiness)
        # Allows deployment when non-critical items (AWS, etc.) are partial
        can_proceed = len(blocking_items) == 0

        return {
            "service": service_name,
            "ready": is_ready,
            "readiness_percent": readiness_percent,
            "total_items": checklist.get("total_items", 0),
            "completed_items": checklist.get("completed_items", 0),
            "blocking_items": blocking_items,
            "warnings": warnings,
            "can_proceed_with_acknowledgment": can_proceed,
        }

    try:
        result = await asyncio.to_thread(_validate_sync)
        return result
    except FileNotFoundError:
        # If no checklist, assume service is ready but with warning
        return {
            "service": service_name,
            "ready": True,
            "readiness_percent": 0,
            "total_items": 0,
            "completed_items": 0,
            "blocking_items": [],
            "warnings": [f"No checklist found for {service_name}"],
            "can_proceed_with_acknowledgment": True,
        }
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Internal error in validate_service_checklist")
        raise HTTPException(status_code=500, detail="Internal server error") from e


@router.get("")
async def list_checklists(request: Request):
    """
    List all available service checklists with summary status.

    Returns:
        List of services with their readiness percentages.
    """
    config_dir = cast(Path, cast(FastAPI, request.app).state.config_dir)

    def _list_sync():
        checklists = []

        # Find all checklist files
        for checklist_file in config_dir.glob("checklist.*.yaml"):
            # Skip template and special files
            filename: str = checklist_file.name
            if filename in (
                "checklist.template.yaml",
                "checklist.prerequisites.yaml",
                "checklist.unified-trading-library.yaml",
            ):
                continue

            # Extract service name
            service_name = filename.replace("checklist.", "").replace(".yaml", "")

            try:
                checklist_data = _load_checklist_yaml(config_dir, service_name)
                parsed = _parse_checklist(checklist_data)

                checklists.append(
                    {
                        "service": service_name,
                        "readiness_percent": parsed["readiness_percent"],
                        "total_items": parsed["total_items"],
                        "completed_items": parsed["completed_items"],
                        "pending_items": parsed["pending_items"],
                        "blocking_count": len(cast(list[object], parsed["blocking_items"])),
                        "last_updated": parsed["last_updated"],
                    }
                )
            except (OSError, ValueError, RuntimeError) as e:
                checklists.append(
                    {
                        "service": service_name,
                        "readiness_percent": 0,
                        "error": str(e),
                    }
                )

        # Sort by readiness (highest first)
        checklists.sort(key=lambda x: x.get("readiness_percent", 0), reverse=True)

        return checklists

    result = await asyncio.to_thread(_list_sync)
    return {"checklists": result, "count": len(result)}
