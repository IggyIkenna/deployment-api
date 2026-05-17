"""
Repo coverage endpoint (Phase 8.E.2).

GET /api/repos/coverage  — per-repo coverage-snapshot summary.

Reads parquets written by coverage_snapshot.sh from:
  gs://{project_id}-deployment-events/coverage_snapshot/repo=<R>/coverage_snapshot_*.parquet

Each parquet row schema (from coverage_snapshot_emit.py):
  repo, surface, target_pct, actual_pct, files_matched,
  lines_covered, lines_valid, snapshot_at

Response per repo: most-recent snapshot_at, all_above_target flag, worst surface details.

Plan: deployment_and_qg_strategy_implementation_2026_05_13.md Phase 8.E.2.
"""

from __future__ import annotations

import io
import logging

from fastapi import APIRouter

logger = logging.getLogger(__name__)
router = APIRouter()

_COVERAGE_PREFIX = "coverage_snapshot/"


def _evaluate_repo_coverage(
    rows: list[dict[str, object]],
) -> dict[str, object]:
    """Aggregate surface rows for one repo into a single status dict."""
    if not rows:
        return {
            "all_above_target": False,
            "snapshot_at": None,
            "worst_surface": None,
            "worst_actual_pct": None,
            "worst_target_pct": None,
        }

    snapshot_at: object = rows[0].get("snapshot_at")
    below = [r for r in rows if (r.get("actual_pct") or 0.0) < (r.get("target_pct") or 0)]
    if not below:
        return {
            "all_above_target": True,
            "snapshot_at": snapshot_at,
            "worst_surface": None,
            "worst_actual_pct": None,
            "worst_target_pct": None,
        }

    worst = min(below, key=lambda r: float(r.get("actual_pct") or 0.0))
    return {
        "all_above_target": False,
        "snapshot_at": snapshot_at,
        "worst_surface": str(worst.get("surface") or "unknown"),
        "worst_actual_pct": float(worst.get("actual_pct") or 0.0),
        "worst_target_pct": int(worst.get("target_pct") or 0),
    }


def _load_coverage_from_gcs(
    project_id: str,
) -> dict[str, list[dict[str, object]]]:
    """Load latest coverage snapshot rows per repo from GCS."""
    from unified_trading_library import download_from_storage, get_storage_client

    bucket_name = f"{project_id}-deployment-events"
    rows_by_repo: dict[str, list[dict[str, object]]] = {}
    storage_client = get_storage_client(project_id)
    blobs = list(storage_client.list_blobs(bucket_name, prefix=_COVERAGE_PREFIX))

    seen_dates: dict[str, str] = {}
    for blob in sorted(blobs, key=lambda b: b.name, reverse=True):
        parts = blob.name.split("/")
        repo_part = next((p for p in parts if p.startswith("repo=")), None)
        if repo_part is None:
            continue
        repo_name = repo_part.removeprefix("repo=")
        # Only load the most-recent snapshot date per repo (first blob in reverse sort)
        if repo_name in seen_dates and not blob.name.startswith(
            f"{_COVERAGE_PREFIX}repo={repo_name}/{seen_dates[repo_name]}"
        ):
            continue
        try:
            import pyarrow.parquet as pq

            raw = download_from_storage(bucket_name, blob.name)
            table = pq.read_table(io.BytesIO(raw))
            pydict = table.to_pydict()
            n_rows = len(pydict.get("repo") or [])
            for i in range(n_rows):
                row: dict[str, object] = {col: (vals[i] if vals else None) for col, vals in pydict.items()}
                if repo_name not in rows_by_repo:
                    rows_by_repo[repo_name] = []
                    # Track which snapshot date we're reading for this repo
                    date_part = next(
                        (p for p in parts if p.startswith("coverage_snapshot_")),
                        "",
                    )
                    seen_dates[repo_name] = date_part
                rows_by_repo[repo_name].append(row)
        except Exception as blob_exc:
            logger.warning("[REPO-COVERAGE] Failed to read parquet %s: %s", blob.name, blob_exc)
    return rows_by_repo


@router.get("/coverage")
async def get_repo_coverage() -> list[dict[str, object]]:
    """
    Return per-repo coverage status from the most-recent coverage snapshot.

    all_above_target=true iff all tracked surfaces are at or above their target_pct.
    """
    from deployment_api.deployment_api_config import DeploymentApiConfig

    _api_cfg = DeploymentApiConfig()
    if _api_cfg.is_mock_mode():
        return [
            {
                "repo": "deployment-api",
                "all_above_target": True,
                "snapshot_at": "2026-05-17T06:00:00Z",
                "worst_surface": None,
                "worst_actual_pct": None,
                "worst_target_pct": None,
            },
            {
                "repo": "unified-trading-library",
                "all_above_target": True,
                "snapshot_at": "2026-05-17T06:00:00Z",
                "worst_surface": None,
                "worst_actual_pct": None,
                "worst_target_pct": None,
            },
            {
                "repo": "features-service",
                "all_above_target": False,
                "snapshot_at": "2026-05-17T06:00:00Z",
                "worst_surface": "per_archetype_calculators",
                "worst_actual_pct": 79.5,
                "worst_target_pct": 90,
            },
            {
                "repo": "market-data-processing-service",
                "all_above_target": False,
                "snapshot_at": "2026-05-16T06:00:00Z",
                "worst_surface": "service_startup",
                "worst_actual_pct": 36.1,
                "worst_target_pct": 100,
            },
        ]

    try:
        project_id: str = _api_cfg.gcp_project_id or ""
        rows_by_repo: dict[str, list[dict[str, object]]] = {}
        try:
            rows_by_repo = _load_coverage_from_gcs(project_id)
        except Exception as gcs_exc:
            logger.warning("[REPO-COVERAGE] GCS read failed: %s", gcs_exc)

        if not rows_by_repo:
            return [
                {
                    "repo": "no_data",
                    "all_above_target": False,
                    "snapshot_at": None,
                    "worst_surface": None,
                    "worst_actual_pct": None,
                    "worst_target_pct": None,
                }
            ]

        results: list[dict[str, object]] = []
        for repo, rows in rows_by_repo.items():
            status = _evaluate_repo_coverage(rows)
            results.append({"repo": repo, **status})
        return sorted(results, key=lambda r: str(r["repo"]))

    except Exception as exc:
        logger.error("[REPO-COVERAGE] Unexpected failure: %s", exc)
        return [
            {
                "repo": "error",
                "all_above_target": False,
                "snapshot_at": None,
                "worst_surface": str(exc),
                "worst_actual_pct": None,
                "worst_target_pct": None,
            }
        ]
