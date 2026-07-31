"""Shared DuckDB-cell coercion + generic breakdown-row builders for cost observability.

Split out of ``cost_observability/service.py`` (1055L, over the 900L file-size gate) per
``plans/active/issues/deployment_api_qg_size_gate_debt_2026_07_30.md``. These helpers don't
touch ``CostObservabilityService`` state — every one is a pure function over its explicit
arguments (a window ``Table``, a WHERE fragment, params) — so they moved here verbatim rather
than staying as instance methods. ``service.py`` re-imports the coercion helpers (``_s``/``_f``/
``_fn``/``_i``) rather than redefining them, since ``resource_rows.py`` and
``breakdown_dimensions.py`` need the exact same coercions and a duplicate copy would drift.
"""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa

from deployment_api.services.cost_observability.models import CLOUD_AWS, CLOUD_GCP, CLOUD_GITHUB, BreakdownRow
from deployment_api.services.cost_observability.snapshot import aggregate_arrow

# Return up to this many real groups so the UI can PAGINATE through all of them (e.g. ~565
# buckets); anything beyond still folds into the honest "Other" roll-up so the header total
# stays exact. Single source of truth — ``service.py`` re-imports this rather than redeclaring it.
_BREAKDOWN_LIMIT = 1000


# --- typed coercion of DuckDB result cells (fetchall returns untyped tuples) --
def _s(v: object) -> str:
    return v if isinstance(v, str) else ("" if v is None else str(v))


def _f(v: object) -> float:
    return float(v) if isinstance(v, (int, float)) else 0.0


def _fn(v: object) -> float | None:
    return float(v) if isinstance(v, (int, float)) else None


def _i(v: object) -> int | None:
    return int(v) if isinstance(v, int) else None


def _agg_cols(cutoff_iso: str) -> str:
    """Shared SELECT fragment: net/gross/credit (+ native), single-or-USD currency, provisional
    OR-fold, and the spot>on-demand>other purchase fold — the in-SQL equivalents of the old
    per-group Python accumulators. Rounded to 6dp (like the source query); the response builder
    rounds to 2dp, mirroring the pre-Increment-2 math. Grouped rows only, never raw rows."""
    return (
        "SUM(cost + credit) AS net, SUM(cost) AS gross, SUM(credit) AS credit, "
        "SUM(cost_native + credit_native) AS net_n, SUM(cost_native) AS gross_n, "
        "SUM(credit_native) AS credit_n, "
        "CASE WHEN COUNT(DISTINCT currency) = 1 THEN ANY_VALUE(currency) ELSE 'USD' END AS ccy, "
        f"BOOL_OR(day >= '{cutoff_iso}') AS provisional, "
        "CASE WHEN BOOL_OR(purchase_option = 'spot') THEN 'spot' "
        "WHEN BOOL_OR(purchase_option = 'on-demand') THEN 'on-demand' ELSE 'other' END AS purchase"
    )  # nosec B608 — cutoff_iso is a server-derived ISO date (see _provisional_cutoff_iso), no user input


def _agg_row(
    t: tuple[object, ...],
    off: int,
    *,
    label: str,
    cloud: str | None,
    detail: str,
    purchase: bool = False,
    provisional: bool = True,
    **extra: object,
) -> BreakdownRow:
    """Build a BreakdownRow from an ``_agg_cols`` tuple slice ``t[off:off+9]``:
    net, gross, credit, net_n, gross_n, credit_n, ccy, provisional, purchase. Rounds to 2dp
    (mirroring the pre-Increment-2 per-group Python rounding). ``purchase=True`` uses the SQL
    purchase fold; ``provisional=False`` forces is_provisional off (the resource/bucket view
    deliberately never marked it) — both preserve the exact pre-Increment-2 per-view shape."""
    return BreakdownRow(
        label=label,
        cloud=cloud,
        cost=round(_f(t[off]), 2),
        gross=round(_f(t[off + 1]), 2),
        credit=round(_f(t[off + 2]), 2),
        cost_native=round(_f(t[off + 3]), 2),
        gross_native=round(_f(t[off + 4]), 2),
        credit_native=round(_f(t[off + 5]), 2),
        currency=_s(t[off + 6]) or "USD",
        is_provisional=(bool(t[off + 7]) if provisional else False),
        purchase_option=(_s(t[off + 8]) if purchase else ""),
        detail=detail,
        **extra,  # pyright: ignore[reportArgumentType]
    )


def _grouped(table: pa.Table, cwhere: str, cparams: Sequence[object], cutoff: str, key_expr: str) -> list[BreakdownRow]:
    rows = aggregate_arrow(
        table,
        f"SELECT cloud, {key_expr} AS label, {_agg_cols(cutoff)} FROM cost_records "  # nosec B608 — key_expr/cwhere are code-internal SQL fragments; user params are bound
        f"WHERE {cwhere} GROUP BY cloud, label ORDER BY net DESC, label",
        list(cparams),
    )
    return [
        _agg_row(r, 2, label=_s(r[1]), cloud=_s(r[0]), detail=_CLOUD_LABEL.get(_s(r[0]), _s(r[0])), purchase=True)
        for r in rows
    ]


def _by_sku(table: pa.Table, cwhere: str, cparams: Sequence[object], cutoff: str) -> list[BreakdownRow]:
    """SKU (GCP) / usage_type (AWS) breakdown — the "why is this service expensive" axis,
    e.g. Regional Coldline Class A Operations hidden inside "Cloud Storage"."""
    rows = aggregate_arrow(
        table,
        f"SELECT cloud, service, COALESCE(NULLIF(sku, ''), 'Unknown') AS sku, {_agg_cols(cutoff)} "  # nosec B608 — cwhere is code-internal; user params bound
        f"FROM cost_records WHERE {cwhere} GROUP BY cloud, service, sku ORDER BY net DESC, sku",
        list(cparams),
    )
    return [_agg_row(r, 3, label=_s(r[2]), cloud=_s(r[0]), detail=_s(r[1])) for r in rows]


def _by_day(
    table: pa.Table, cwhere: str, cparams: Sequence[object], cutoff: str, dates: list[str]
) -> list[BreakdownRow]:
    by_day = {
        _s(r[0]): r
        for r in aggregate_arrow(
            table,
            f"SELECT day, {_agg_cols(cutoff)} FROM cost_records WHERE {cwhere} GROUP BY day",  # nosec B608 — cwhere code-internal; params bound
            list(cparams),
        )
    }
    rows: list[BreakdownRow] = []
    for d in reversed(dates):
        r = by_day.get(d)
        if r is None:  # a window day with zero spend (dict.fromkeys behaviour) — honest 0 row
            rows.append(BreakdownRow(label=d, cloud=None, cost=0.0, detail=""))
        else:
            rows.append(_agg_row(r, 1, label=d, cloud=None, detail=""))
    return rows


def _finalize_rows(
    rows_all: list[BreakdownRow],
    *,
    totals: tuple[float, float, float],
    native_totals: tuple[float, float, float],
    scope_currency: str,
    extra_aggregates: tuple[BreakdownRow, ...] = (),
) -> tuple[list[BreakdownRow], int]:
    """Cap a cost-sorted (descending) row list to the top ``_BREAKDOWN_LIMIT``, folding the rest
    into ONE ``Other (N more)`` row whose cost/gross/credit are the RESIDUAL vs the true `totals`
    — so the shown rows sum to the header total EXACTLY (to the cent), absorbing per-group
    rounding. Idle/orphaned rows below the cap stay visible (never folded, so the
    waste-surfacing survives). `extra_aggregates` (e.g. an
    ``Unattributed`` row) are appended after Other and counted against the residual. Returns
    (rows_to_show, total_group_count)."""
    total_net, total_gross, total_credit = totals
    total_net_n, total_gross_n, total_credit_n = native_totals
    total_groups = len(rows_all)
    if total_groups <= _BREAKDOWN_LIMIT:
        return list(rows_all) + list(extra_aggregates), total_groups
    shown = rows_all[:_BREAKDOWN_LIMIT]
    tail = rows_all[_BREAKDOWN_LIMIT:]
    waste_extras = [r for r in tail if r.is_idle]
    kept = shown + waste_extras
    remaining_count = len(tail) - len(waste_extras)
    aggregates: list[BreakdownRow] = []
    if remaining_count > 0:
        shown_net = sum(r.cost for r in kept) + sum(a.cost for a in extra_aggregates)
        shown_gross = sum(r.gross for r in kept) + sum(a.gross for a in extra_aggregates)
        shown_credit = sum(r.credit for r in kept) + sum(a.credit for a in extra_aggregates)
        shown_net_n = sum(r.cost_native for r in kept) + sum(a.cost_native for a in extra_aggregates)
        shown_gross_n = sum(r.gross_native for r in kept) + sum(a.gross_native for a in extra_aggregates)
        shown_credit_n = sum(r.credit_native for r in kept) + sum(a.credit_native for a in extra_aggregates)
        aggregates.append(
            BreakdownRow(
                label=f"Other ({remaining_count:,} more)",
                cloud=None,
                cost=round(total_net - shown_net, 2),
                gross=round(total_gross - shown_gross, 2),
                credit=round(total_credit - shown_credit, 2),
                currency=scope_currency,
                cost_native=round(total_net_n - shown_net_n, 2),
                gross_native=round(total_gross_n - shown_gross_n, 2),
                credit_native=round(total_credit_n - shown_credit_n, 2),
                detail=f"rows beyond the top {_BREAKDOWN_LIMIT}",
                is_aggregate=True,
            )
        )
    aggregates.extend(extra_aggregates)
    return kept + aggregates, total_groups


_CLOUD_LABEL = {CLOUD_GCP: "GCP", CLOUD_AWS: "AWS", CLOUD_GITHUB: "GitHub"}
