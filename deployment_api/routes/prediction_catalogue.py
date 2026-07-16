"""Prediction-market catalogue browser API — human-readable browsing by
category / canonical_question_group.

Reads the prediction lifecycle catalogue (``prod/catalog.parquet``) read-only.
Mirrors ``routes/catalogue_lifecycle.py`` (mock-mode aware, thin route over the
service). Plan data_status_page_ux_and_canonicalisation_2026_07_16 P3.
"""

from __future__ import annotations

from fastapi import APIRouter, Query

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.services.prediction_catalogue import read_prediction_catalogue

_cfg = DeploymentApiConfig()

router = APIRouter(prefix="/data-status", tags=["Data Status"])


@router.get("/prediction-catalogue")
async def get_prediction_catalogue(
    category: str | None = Query(None, description="Optional coarse category filter (crypto/financial/sports/…)"),
    canonical_question_group: str | None = Query(None, description="Optional canonical_question_group sub-filter"),
    venue: str | None = Query(None, description="Optional venue filter (KALSHI/POLYMARKET)"),
    search: str | None = Query(None, description="Optional substring search over label/instrument_id/cqg"),
    limit: int = Query(50, ge=1, le=500, description="Page size"),
    offset: int = Query(0, ge=0, description="Page offset"),
) -> dict[str, object]:
    """Paginated, faceted browse of the live prediction-market catalogue."""
    if _cfg.is_mock_mode():
        return {"rows": [], "total": 0, "category_counts": {}, "cqg_counts": {}, "mock": True}
    result = read_prediction_catalogue(
        category=category,
        canonical_question_group=canonical_question_group,
        venue=venue,
        search=search,
        limit=limit,
        offset=offset,
    )
    return {
        "rows": list(result["rows"]),
        "total": result["total"],
        "category_counts": dict(result["category_counts"]),
        "cqg_counts": dict(result["cqg_counts"]),
    }
