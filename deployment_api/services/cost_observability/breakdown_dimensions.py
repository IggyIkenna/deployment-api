"""Per-dimension row dispatch for ``CostObservabilityService.breakdown`` (the "By X" tabs).

Split out of ``cost_observability/service.py`` (1055L, over the 900L file-size gate) +
``CostObservabilityService.breakdown`` (148L, the largest method, over the 50L method-size gate)
per ``plans/active/issues/deployment_api_qg_size_gate_debt_2026_07_30.md``. ``breakdown()`` itself
still resolves the window, handles the "day" dimension (chronological, uncapped — never goes
through the top-N dispatch below), and finalizes/returns the response; everything about picking
a dimension's raw (uncapped) row set — plus the two small pieces duplicated across both the
"day" and top-N return paths (``share_pct`` + the ``BreakdownResponse`` shape) — lives here.
``by_resource``/``stopped_vm_disk_rows`` are passed in as bound methods (not imported) because
they need ``self._waste_cross_refs()``'s live GCP cross-reference — every other dimension's row
source is a pure function of its arguments.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence

import pyarrow as pa

from deployment_api.services.cost_observability.models import (
    BUSINESS_LABEL_KEYS,
    KIND_BUCKET,
    BreakdownResponse,
    BreakdownRow,
)
from deployment_api.services.cost_observability.row_builders import (
    _by_sku,  # pyright: ignore[reportPrivateUsage]
    _f,  # pyright: ignore[reportPrivateUsage]
    _grouped,  # pyright: ignore[reportPrivateUsage]
    _s,  # pyright: ignore[reportPrivateUsage]
)
from deployment_api.services.cost_observability.snapshot import aggregate_arrow

_ByResourceFn = Callable[..., list[BreakdownRow]]
_StoppedVmDiskFn = Callable[[pa.Table, str, Sequence[object]], list[BreakdownRow]]


def _breakdown_scope_totals(
    table: pa.Table, cwhere: str, cparams: Sequence[object], dimension: str
) -> tuple[tuple[float, float, float], tuple[float, float, float], str, float]:
    """True totals for this dimension's SCOPE, summed from RAW rows in DuckDB (not from rounded
    per-group rows) so every tab's header total equals the KPI/summary to the cent — the cap +
    per-group rounding residual are absorbed by the "Other" row. Bucket scope is buckets only;
    every other dimension covers all rows (resource incl. the unattributed tail, surfaced by the
    caller). One currency only when the scope is a single cloud (the tally case: cloud=gcp ->
    GBP); mixed -> USD. Returns (totals, native_totals, scope_currency, total)."""
    scope_where = cwhere + (" AND resource_kind = 'bucket'" if dimension == "bucket" else "")
    tr = aggregate_arrow(
        table,
        "SELECT SUM(cost + credit), SUM(cost), SUM(credit), SUM(cost_native + credit_native), "
        "SUM(cost_native), SUM(credit_native), "
        "CASE WHEN COUNT(DISTINCT currency) = 1 THEN ANY_VALUE(currency) ELSE 'USD' END "  # nosec B608
        f"FROM cost_records WHERE {scope_where}",
        list(cparams),
    )[0]
    totals = (round(_f(tr[0]), 2), round(_f(tr[1]), 2), round(_f(tr[2]), 2))
    native_totals = (round(_f(tr[3]), 2), round(_f(tr[4]), 2), round(_f(tr[5]), 2))
    return totals, native_totals, _s(tr[6]) or "USD", totals[0]


def _unattributed_row(
    table: pa.Table, cwhere: str, cparams: Sequence[object], scope_currency: str
) -> tuple[BreakdownRow, ...]:
    """Cost the provider tags to NO resource (Cloud Run, networking, …) is dropped by the
    per-resource grouping; surfaced as one row so the resource total reconciles to the cloud
    total instead of silently sitting ~$365 low. Empty tuple when the residual rounds to 0."""
    ur = aggregate_arrow(
        table,
        "SELECT SUM(cost + credit), SUM(cost_native + credit_native) FROM cost_records "
        f"WHERE resource_id = '' AND {cwhere}",  # nosec B608 — cwhere is 'cloud = ?' or 'TRUE'
        list(cparams),
    )[0]
    unattributed, unattributed_native = round(_f(ur[0]), 2), round(_f(ur[1]), 2)
    if abs(unattributed) < 0.01:
        return ()
    return (
        BreakdownRow(
            label="Unattributed (no resource id)",
            cloud=None,
            cost=unattributed,
            currency=scope_currency,
            cost_native=unattributed_native,
            detail="cost the provider doesn't tag to a resource (Cloud Run, networking, …)",
            is_aggregate=True,
        ),
    )


def _waste_dimension_rows(
    table: pa.Table,
    cwhere: str,
    cparams: Sequence[object],
    cutoff: str,
    dates: list[str],
    by_resource: _ByResourceFn,
    stopped_vm_disk_rows: _StoppedVmDiskFn,
) -> tuple[list[BreakdownRow], tuple[float, float, float], tuple[float, float, float], float]:
    """Idle static IPs / orphaned disks / orphaned images+machine-images+snapshots / idle elastic
    IPs (the SAME per-resource waste classification the "resource" dimension already flags) plus
    `stopped_vm_disk` rows (a different shape — a disk's post-compute-stop cost sub-window, not a
    resource's whole-window cost), merged + re-sorted. The pre-computed scope totals cover ALL
    resource spend (waste + non-waste alike); re-derive true totals for this waste-only scope from
    the filtered rows themselves so the header total / "Other" residual / share_pct stay
    internally consistent (mirrors why the "bucket" dimension narrows its scope instead — that
    narrowing isn't SQL-expressible here, the waste predicate needs a live GCP cross-ref)."""
    rows_all = [r for r in by_resource(table, cwhere, cparams, cutoff, None, window_days=len(dates)) if r.is_idle]
    rows_all.extend(stopped_vm_disk_rows(table, cwhere, cparams))
    rows_all.sort(key=lambda r: r.cost, reverse=True)
    totals = (
        round(sum(r.cost for r in rows_all), 2),
        round(sum(r.gross for r in rows_all), 2),
        round(sum(r.credit for r in rows_all), 2),
    )
    native_totals = (
        round(sum(r.cost_native for r in rows_all), 2),
        round(sum(r.gross_native for r in rows_all), 2),
        round(sum(r.credit_native for r in rows_all), 2),
    )
    return rows_all, totals, native_totals, totals[0]


def _dispatch_dimension_rows(
    dimension: str,
    table: pa.Table,
    cwhere: str,
    cparams: Sequence[object],
    cutoff: str,
    dates: list[str],
    label_key: str,
    scope_currency: str,
    totals: tuple[float, float, float],
    native_totals: tuple[float, float, float],
    total: float,
    *,
    by_resource: _ByResourceFn,
    stopped_vm_disk_rows: _StoppedVmDiskFn,
) -> tuple[list[BreakdownRow], tuple[BreakdownRow, ...], tuple[float, float, float], tuple[float, float, float], float]:
    """Dispatch a non-"day" breakdown ``dimension`` to its raw (uncapped) row set. Returns
    ``(rows_all, extra_aggregates, totals, native_totals, total)`` — ``totals``/``native_totals``/
    ``total`` pass through unchanged for every dimension except "waste", which re-derives them
    (see :func:`_waste_dimension_rows`). The caller applies the top-N cap + fold afterwards."""
    if dimension == "bucket":
        rows_all = by_resource(table, cwhere, cparams, cutoff, KIND_BUCKET, window_days=len(dates))
        return rows_all, (), totals, native_totals, total
    if dimension == "resource":
        # window_days so bucket-kind rows in the resource view also get storage detail — the
        # "Top storage buckets" leaf table is fed by this dimension.
        rows_all = by_resource(table, cwhere, cparams, cutoff, None, window_days=len(dates))
        extra = _unattributed_row(table, cwhere, cparams, scope_currency)
        return rows_all, extra, totals, native_totals, total
    if dimension == "region":
        rows_all = _grouped(table, cwhere, cparams, cutoff, "COALESCE(NULLIF(region, ''), 'global')")
        return rows_all, (), totals, native_totals, total
    if dimension == "zone":
        rows_all = _grouped(table, cwhere, cparams, cutoff, "COALESCE(NULLIF(zone, ''), 'unknown')")
        return rows_all, (), totals, native_totals, total
    if dimension == "sku":
        return _by_sku(table, cwhere, cparams, cutoff), (), totals, native_totals, total
    if dimension == "label":
        # Spend by a resource-level GCP business label (purpose/category/venue/asset_group). GCP-only —
        # AWS/GitHub carry no labels, so their spend groups under "(unlabeled)". NULLIF(labels,'') so an
        # empty-labels row (stored as "") isn't fed to json_extract_string as malformed JSON; a null/
        # absent key then falls back to "(unlabeled)".
        key = label_key if label_key in BUSINESS_LABEL_KEYS else "purpose"
        expr = f"COALESCE(NULLIF(json_extract_string(NULLIF(labels, ''), '$.{key}'), ''), '(unlabeled)')"
        return _grouped(table, cwhere, cparams, cutoff, expr), (), totals, native_totals, total
    if dimension == "waste":
        rows_all, totals, native_totals, total = _waste_dimension_rows(
            table, cwhere, cparams, cutoff, dates, by_resource, stopped_vm_disk_rows
        )
        return rows_all, (), totals, native_totals, total
    return _grouped(table, cwhere, cparams, cutoff, "service"), (), totals, native_totals, total  # service (default)


def _apply_share_pct(rows: list[BreakdownRow], total: float) -> None:
    for r in rows:
        r.share_pct = round((r.cost / total) * 100, 1) if total else 0.0


def _build_breakdown_response(
    dimension: str, cloud: str, dates: list[str], total: float, total_groups: int, rows: list[BreakdownRow]
) -> BreakdownResponse:
    return BreakdownResponse(
        dimension=dimension,
        cloud=cloud,
        days=len(dates),
        start_date=dates[0] if dates else "",
        end_date=dates[-1] if dates else "",
        total=total,
        total_groups=total_groups,
        rows=rows,
    )
