"""Cross-service E2E pipeline trace — GAP G-TRACE.

Threads one (instrument, date) through every pipeline stage (IS -> MTDS -> MDPS ->
features-* -> strategy -> execution) and reports each hop's manifest ``capture_status``,
so an operator can answer "where did this instrument/date get stuck" in one call
instead of checking each service's data-status panel separately.

Reuses the existing per-shard capture-status lookup
(:func:`lookup_capture_status_for_shard`, ``services/data_status_drilldown/_core.py``)
rather than re-implementing manifest reads — this endpoint is pure orchestration over
that primitive, called once per stage.

Stage order + upstream service names mirror the authoritative
``unified_trading_library.dependency_check.PIPELINE_DEPENDENCIES`` graph (also documented
in ``/codex/04-architecture/e2e-pipeline-manifest-wiring.md``). The features stage fans
out to every family ``PIPELINE_DEPENDENCIES`` actually wires (onchain/delta_one/
volatility) rather than picking one — the simplified doc-level chain draws it as a
single "features" box, but the real graph is per-family, and collapsing to one would
silently hide a stuck family.

Split as a new submodule (mirrors ``_distinct_values.py`` / ``_axis_census.py`` —
attaches to the shared package ``router``; see ``routes/data_status/__init__.py``).

Plan: infra_satellite_ao_dispatch_batch1_2026_07_26.md (GAP G-TRACE).
"""

from __future__ import annotations

import logging

from fastapi import Query

import deployment_api.routes.data_status as _ds
from deployment_api.routes.data_status import router

logger = logging.getLogger(__name__)

# (stage index, service_name) — mirrors PIPELINE_DEPENDENCIES' consumer chain.
# Stage 4 (features) fans out to every family the graph wires as a strategy-service
# upstream, rather than picking one arbitrarily.
_TRACE_STAGES: tuple[tuple[int, str], ...] = (
    (1, "instruments-service"),
    (2, "market-tick-data-service"),
    (3, "market-data-processing-service"),
    (4, "features-onchain-service"),
    (4, "features-delta-one-service"),
    (4, "features-volatility-service"),
    (5, "strategy-service"),
    (6, "execution-service"),
)


@router.get("/pipeline-trace")
async def get_pipeline_trace(
    instrument: str = Query(..., description="Instrument id to trace (e.g. BTC-USDT)"),
    date: str = Query(..., description="Date to trace (YYYY-MM-DD)"),
    asset_group: str = Query(..., description="Asset group (cefi/tradfi/defi/sports/prediction)"),
    instrument_type: str | None = Query(None, description="Optional instrument_type narrowing"),
    venue: str | None = Query(None, description="Optional venue narrowing"),
    chain: str | None = Query(None, description="Optional DeFi chain narrowing"),
) -> dict[str, object]:
    """Cross-service E2E trace for one (instrument, date) — per-hop ``capture_status``
    across the full IS->MTDS->MDPS->features->strategy->execution chain.

    Read-only; each hop is an independent :func:`lookup_capture_status_for_shard` call
    against that stage's own manifest — a slow/absent upstream never blocks a
    downstream hop from reporting its own status. ``stuck_at`` names the first hop (in
    pipeline order) whose status is not ``captured``, or ``None`` if every hop captured.
    """
    axes: dict[str, str | None] = {
        "instrument_id": instrument,
        "instrument_type": instrument_type,
        "venue": venue,
        "chain": chain,
    }
    hops: list[dict[str, object]] = []
    for stage, service in _TRACE_STAGES:
        try:
            result = _ds.lookup_capture_status_for_shard(
                service=service,
                asset_group=asset_group,
                day=date,
                **axes,
            )
        except (OSError, ValueError, RuntimeError) as exc:
            logger.warning("pipeline-trace hop failed for %s: %s", service, exc)
            result = {"status": "never_attempted", "error_reason": "", "attempted_at": "", "written_at": ""}
        hops.append({"stage": stage, "service": service, **result})

    stuck_at = next((hop["service"] for hop in hops if hop["status"] != "captured"), None)

    return {
        "instrument": instrument,
        "date": date,
        "asset_group": asset_group.lower(),
        "hops": hops,
        "stuck_at": stuck_at,
    }
