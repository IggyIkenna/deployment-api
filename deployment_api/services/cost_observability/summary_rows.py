"""Per-cloud aggregation + row assembly for ``CostObservabilityService.summarize``.

Split out of ``cost_observability/service.py`` (1055L, over the 900L file-size gate) +
``CostObservabilityService.summarize`` (88L, over the 50L method-size gate) per
``plans/active/issues/deployment_api_qg_size_gate_debt_2026_07_30.md``. Both functions are pure
over their explicit arguments (a window ``Table`` / already-aggregated dicts) — neither touches
``CostObservabilityService`` state.
"""

from __future__ import annotations

import pyarrow as pa

from deployment_api.services.cost_observability.models import CLOUD_AWS, CLOUD_GCP, CLOUD_GITHUB, CloudSummary
from deployment_api.services.cost_observability.row_builders import (
    _f,  # pyright: ignore[reportPrivateUsage]
    _s,  # pyright: ignore[reportPrivateUsage]
)
from deployment_api.services.cost_observability.snapshot import aggregate_arrow

CLOUD_ORDER = [CLOUD_GCP, CLOUD_AWS, CLOUD_GITHUB]


def _cloud_current_aggregates(
    cur: pa.Table, dates: list[str]
) -> tuple[dict[str, tuple[object, ...]], dict[str, list[float]]]:
    """Per-cloud current aggregates + per-(cloud,day) net for the sparkline."""
    cur_by_cloud = {
        _s(r[0]): r
        for r in aggregate_arrow(
            cur,
            "SELECT cloud, SUM(cost) gross, SUM(credit) credit, SUM(cost_native) gross_n, "
            "SUM(credit_native) credit_n, ANY_VALUE(currency) ccy, BOOL_OR(is_placeholder) ph "
            "FROM cost_records GROUP BY cloud",
        )
    }
    day_index = {d: i for i, d in enumerate(dates)}
    daily_by_cloud: dict[str, list[float]] = {c: [0.0] * len(dates) for c in CLOUD_ORDER}
    day_rows = aggregate_arrow(cur, "SELECT cloud, day, SUM(cost + credit) FROM cost_records GROUP BY cloud, day")
    for c, day, net in day_rows:
        idx = day_index.get(_s(day))
        if idx is not None and _s(c) in daily_by_cloud:
            daily_by_cloud[_s(c)][idx] = _f(net)
    return cur_by_cloud, daily_by_cloud


def _build_cloud_summaries(
    cur_by_cloud: dict[str, tuple[object, ...]],
    daily_by_cloud: dict[str, list[float]],
    prior_net: dict[str, float],
) -> tuple[list[CloudSummary], float, float, float]:
    """Per-cloud ``CloudSummary`` rows + the grand (net/gross/credit) totals across CLOUD_ORDER."""
    clouds: list[CloudSummary] = []
    grand = grand_gross = grand_credit = 0.0
    for cloud in CLOUD_ORDER:
        row = cur_by_cloud.get(cloud)
        gross = round(_f(row[1]) if row else 0.0, 2)
        credit = round(_f(row[2]) if row else 0.0, 2)
        total = round(gross + credit, 2)  # net — what actually gets invoiced
        gross_native = round(_f(row[3]) if row else 0.0, 2)
        credit_native = round(_f(row[4]) if row else 0.0, 2)
        native_currency = (_s(row[5]) if row else "") or "USD"
        grand += total
        grand_gross += gross
        grand_credit += credit
        prior_total = prior_net.get(cloud, 0.0)
        delta = round(((total - prior_total) / prior_total) * 100, 1) if prior_total else None
        clouds.append(
            CloudSummary(
                cloud=cloud,
                total=total,
                gross=gross,
                credit=credit,
                delta_pct=delta,
                daily=[round(v, 4) for v in daily_by_cloud[cloud]],
                is_placeholder=bool(row[6]) if row else False,
                currency=native_currency,
                total_native=round(gross_native + credit_native, 2),
                gross_native=gross_native,
                credit_native=credit_native,
            )
        )
    return clouds, grand, grand_gross, grand_credit
