"""Per-resource breakdown query + row assembly (the "resource"/"bucket"/"waste" dimensions).

Split out of ``cost_observability/service.py`` (1055L, over the 900L file-size gate) +
``CostObservabilityService._by_resource`` (110L, over the 50L method-size gate) per
``plans/active/issues/deployment_api_qg_size_gate_debt_2026_07_30.md``. The three functions here
are the pure-data half of ``_by_resource`` — the SQL fetch, the storage-class/cost-component
aggregation, and the row assembly. The live-GCP waste cross-refs (unattached disks / orphaned
images / stale machine images / stale snapshots) stay instance methods on
``CostObservabilityService`` (they need ``self._cfg`` + a `ThreadPoolExecutor` fan-out, and
they're each well under the size gate already) — ``_by_resource`` still calls
``self._waste_cross_refs()`` and passes the four resulting frozensets in here.
"""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa

from deployment_api.services.cost_observability.models import CLOUD_AWS, CLOUD_GCP, BreakdownRow, ResourceDailyCost
from deployment_api.services.cost_observability.row_builders import (
    _agg_cols,  # pyright: ignore[reportPrivateUsage]
    _agg_row,  # pyright: ignore[reportPrivateUsage]
    _f,  # pyright: ignore[reportPrivateUsage]
    _fn,  # pyright: ignore[reportPrivateUsage]
    _i,  # pyright: ignore[reportPrivateUsage]
    _s,  # pyright: ignore[reportPrivateUsage]
)
from deployment_api.services.cost_observability.snapshot import aggregate_arrow
from deployment_api.services.cost_observability.waste import (
    WASTE_IDLE_ELASTIC_IP,
    WASTE_IDLE_STATIC_IP,
    WASTE_ORPHANED_DISK,
    WASTE_ORPHANED_IMAGE,
    WASTE_ORPHANED_MACHINE_IMAGE,
    WASTE_ORPHANED_SNAPSHOT,
)

# The 4 live-GCP waste cross-ref sets (unattached disks / orphaned images / stale machine images /
# stale snapshots), in that fixed order — see CostObservabilityService._waste_cross_refs.
_WasteXRefs = tuple[frozenset[str], frozenset[str], frozenset[str], frozenset[str]]

# GCP bills storage as GiB-months; a day's usage_amount is that day's fraction of a
# calendar month, so summing across the window and rescaling by the average days/month
# recovers the average GB actually stored (365.25 / 12, not a fixed 30 — months vary 28-31).
_AVG_DAYS_PER_MONTH = 30.44
_GCP_STORAGE_CLASSES = ("Archive", "Coldline", "Nearline", "Standard")


def _gcp_storage_class(sku: str) -> str | None:
    """Storage-class label from a GCP storage SKU description, or None if not a volume SKU.

    Only meaningful once the caller has already confirmed `usage_unit == "gibibyte month"` —
    Operations/retrieval SKUs (billed in count/bytes-retrieved) can share these same class
    words (e.g. "Regional Standard Class A Operations") without being a storage-volume charge.
    """
    low = sku.lower()
    for cls in _GCP_STORAGE_CLASSES:
        if cls.lower() in low:
            return cls
    return None


def _aws_storage_class(usage_type: str) -> str | None:
    """Storage-class label from an AWS S3 usage_type, or None if not a `TimedStorage-*` volume type.

    Mapped onto the same 4 labels the UI already uses for GCP so bucket rows render one
    consistent class axis cross-cloud: Glacier Deep Archive -> Archive, Glacier (flexible
    retrieval) -> Coldline, *-IA (Standard/One Zone infrequent access) -> Nearline, else -> Standard.
    """
    if "TimedStorage" not in usage_type:
        return None
    if "GDA" in usage_type:
        return "Archive"
    if "Glacier" in usage_type:
        return "Coldline"
    if "IA" in usage_type:
        return "Nearline"
    return "Standard"


# Cost-composition of a storage resource's SKUs — what a bucket's spend is actually made of.
# A bucket's total is often operations-dominated (an event-log bucket bills millions of Class-A
# writes on a few GB stored, verified live 2026-07-09: the events bucket = 99.8% ops,
# $0.58 of storage), so splitting the net cost into storage / operations / egress makes the total
# legible. Text-pattern over the SKU (GCP `sku.description`) / usage_type (AWS `line_item_usage_type`),
# same approach as `_storage_class`; every storage SKU falls into exactly one bucket so the parts
# sum to the row's net cost. "egress" folds retrieval/download/transfer-out (all data-access charges).
_COMPONENT_STORAGE = "storage"
_COMPONENT_OPERATIONS = "operations"
_COMPONENT_EGRESS = "egress"
_COMPONENT_OTHER = "other"


def _cost_component(cloud: str, sku: str) -> str:
    low = sku.lower()
    if cloud == CLOUD_GCP:
        # Order matters: "Regional Coldline Class A Operations" contains a class word but is an
        # OPERATIONS charge, so match operations before storage.
        if "operations" in low:
            return _COMPONENT_OPERATIONS
        if "storage" in low:
            return _COMPONENT_STORAGE
        if "download" in low or "data transfer" in low or "network" in low or "retrieval" in low:
            return _COMPONENT_EGRESS
        return _COMPONENT_OTHER
    if cloud == CLOUD_AWS:
        # AWS usage_type: "APN1-Requests-Tier1/2" (ops), "APN1-TimedStorage-ByteHrs" (storage),
        # "*-DataTransfer-Out-Bytes" (egress). Verified live 2026-07-09.
        if "requests" in low:
            return _COMPONENT_OPERATIONS
        if "timedstorage" in low:
            return _COMPONENT_STORAGE
        if "datatransfer" in low or "-out-" in low or "retrieval" in low:
            return _COMPONENT_EGRESS
        return _COMPONENT_OTHER
    return _COMPONENT_OTHER


def _resource_base_rows(
    table: pa.Table, cwhere: str, cparams: Sequence[object], cutoff: str, kind_filter: str
) -> list[tuple[object, ...]]:
    """Heavy money grouping in DuckDB (168K rows -> ~13K groups). agg_cols occupy t[2:11]; then
    service(11), kind(12), machine(13-15), waste flags(16-21)."""
    return aggregate_arrow(
        table,
        f"SELECT cloud, resource_id, {_agg_cols(cutoff)}, "  # nosec B608 — cwhere/kind_filter are code-internal; user params bound
        "ANY_VALUE(service) AS service, "
        "COALESCE(ANY_VALUE(resource_kind) FILTER (WHERE resource_kind <> 'other'), 'other') AS kind, "
        "arg_max(machine_type, day) FILTER (WHERE machine_type <> '') AS mtype, "
        "arg_max(vcpu, day) FILTER (WHERE machine_type <> '') AS mvcpu, "
        "arg_max(memory_gb, day) FILTER (WHERE machine_type <> '') AS mmem, "
        "BOOL_OR(sku LIKE '%Static Ip Charge%') AS w_ip, "
        "BOOL_OR(sku LIKE '%PD Capacity%') AS w_pd, "
        "BOOL_OR(sku LIKE '%ElasticIP:IdleAddress%') AS w_eip, "
        "BOOL_OR(sku LIKE '%Storage Machine Image%') AS w_mimg, "
        "BOOL_OR(sku LIKE '%Storage Image%' AND sku NOT LIKE '%Storage Machine Image%') AS w_img, "
        "BOOL_OR(sku LIKE '%Storage PD Snapshot%') AS w_snap "
        f"FROM cost_records WHERE {cwhere} AND resource_id <> ''{kind_filter} "
        "GROUP BY cloud, resource_id ORDER BY net DESC, resource_id",
        list(cparams),
    )


def _resource_component_class_maps(
    table: pa.Table, cwhere: str, cparams: Sequence[object]
) -> tuple[dict[tuple[str, str], dict[str, float]], dict[tuple[str, str], dict[str, float]]]:
    """Storage detail (bucket-kind rows only) — a small subset (~350 buckets x their SKUs), so
    reuse the tested Python component/class classifiers rather than re-deriving them in SQL.
    Returns (cost-by-component, stored-amount-by-class), each keyed by (cloud, resource_id)."""
    comp_by_res: dict[tuple[str, str], dict[str, float]] = {}
    class_by_res: dict[tuple[str, str], dict[str, float]] = {}
    for c, rid, sku, uunit, uamt, net in aggregate_arrow(
        table,
        "SELECT cloud, resource_id, sku, usage_unit, SUM(usage_amount), SUM(cost + credit) "  # nosec B608 — cwhere code-internal; params bound
        f"FROM cost_records WHERE {cwhere} AND resource_id <> '' AND resource_kind = 'bucket' "
        "GROUP BY cloud, resource_id, sku, usage_unit",
        list(cparams),
    ):
        c_s, rid_s, sku_s, uunit_s = _s(c), _s(rid), _s(sku), _s(uunit)
        k = (c_s, rid_s)
        comp = _cost_component(c_s, sku_s)
        comp_by_res.setdefault(k, {})[comp] = comp_by_res.setdefault(k, {}).get(comp, 0.0) + _f(net)
        cls = (
            _gcp_storage_class(sku_s)
            if (c_s == CLOUD_GCP and uunit_s == "gibibyte month")
            else (_aws_storage_class(sku_s) if c_s == CLOUD_AWS else None)
        )
        if cls is not None:
            class_by_res.setdefault(k, {})[cls] = class_by_res.setdefault(k, {}).get(cls, 0.0) + _f(uamt)
    return comp_by_res, class_by_res


def _resource_row_waste_kind(c: str, t: tuple[object, ...], rid: str, xrefs: _WasteXRefs) -> str:
    """The one SKU-flagged (or live-cross-ref-confirmed) waste kind for a base row, or "" —
    bucket-dimension callers pass an all-empty `xrefs` (that dimension never surfaces idle-IP/
    orphaned-disk rows), matching the original inline `kind is None` gate."""
    unattached, orphaned_images, stale_machine_images, stale_snapshots = xrefs
    if c == CLOUD_GCP and bool(t[16]):
        return WASTE_IDLE_STATIC_IP
    if c == CLOUD_GCP and bool(t[17]) and rid in unattached:
        return WASTE_ORPHANED_DISK
    if c == CLOUD_GCP and bool(t[19]) and rid in stale_machine_images:
        return WASTE_ORPHANED_MACHINE_IMAGE
    if c == CLOUD_GCP and bool(t[20]) and rid in orphaned_images:
        return WASTE_ORPHANED_IMAGE
    if c == CLOUD_GCP and bool(t[21]) and rid in stale_snapshots:
        return WASTE_ORPHANED_SNAPSHOT
    if c == CLOUD_AWS and bool(t[18]):
        return WASTE_IDLE_ELASTIC_IP
    return ""


def _build_resource_rows(
    base: list[tuple[object, ...]],
    kind: str | None,
    xrefs: _WasteXRefs,
    comp_by_res: dict[tuple[str, str], dict[str, float]],
    class_by_res: dict[tuple[str, str], dict[str, float]],
    window_days: int | None,
) -> list[BreakdownRow]:
    """Assemble one ``BreakdownRow`` per ``_resource_base_rows`` tuple, threading in the waste
    flag (bucket dimension never flags idle-IP/orphaned-disk — see ``_resource_row_waste_kind``)
    and, when ``window_days`` is given, the storage-class GB breakdown + effective $/GB. SQL
    already sorted by net DESC; the caller applies the top-N cap afterwards."""
    rows: list[BreakdownRow] = []
    for t in base:
        c, rid = _s(t[0]), _s(t[1])
        waste = _resource_row_waste_kind(c, t, rid, xrefs) if kind is None else ""
        row = _agg_row(
            t,
            2,
            label=rid,
            cloud=c,
            detail=_s(t[11]),
            purchase=True,
            provisional=False,  # the resource/bucket view never set is_provisional (pre-Inc2 parity)
            resource_kind=_s(t[12]),
            is_idle=bool(waste),
            waste_kind=waste,
            machine_type=_s(t[13]),
            vcpu=_i(t[14]),
            memory_gb=_fn(t[15]),
        )
        components = comp_by_res.get((c, rid), {})
        if window_days:
            classes = class_by_res.get((c, rid))
            if classes:
                gb_by_class = {cls: round(amt * _AVG_DAYS_PER_MONTH / window_days, 2) for cls, amt in classes.items()}
                total_gb = round(sum(gb_by_class.values()), 2)
                row.storage_gb = total_gb
                row.storage_class_gb = gb_by_class
                if total_gb > 0:
                    # $/GB is the effective STORAGE rate — the storage COMPONENT cost over stored
                    # GB, NOT the row's total (operations-dominated) cost.
                    row.cost_per_gb = round(components.get("storage", 0.0) / total_gb, 4)
        kept = {comp: round(x, 2) for comp, x in components.items() if round(x, 2) != 0.0}
        if kept:
            row.cost_by_component = kept
        rows.append(row)
    return rows


def _resource_daily_from_grouped_rows(
    rows: list[tuple[object, ...]], today: str, hours_billed: float
) -> dict[str, ResourceDailyCost]:
    """Fold ``(resource_id, day, net)`` rows into per-resource ``ResourceDailyCost`` figures —
    see :meth:`CostObservabilityService.per_resource_daily`'s docstring for the exact semantics
    of ``actual_usd``/``avg_7d_usd``/``projected_24h_usd``/``cost_basis``. ``today`` and
    ``hours_billed`` are pre-derived by the caller so this stays a pure fold."""
    by_res_day: dict[str, dict[str, float]] = {}
    for rid, day, net in rows:
        by_res_day.setdefault(_s(rid), {})[_s(day)] = _f(net)
    out: dict[str, ResourceDailyCost] = {}
    for resource_id, day_net in by_res_day.items():
        daily = list(day_net.values())
        complete_days = [d for d in day_net if d < today]
        latest = max(complete_days) if complete_days else max(day_net)
        projected_24h = day_net[max(complete_days)] if complete_days else day_net[latest] / hours_billed * 24
        out[resource_id] = ResourceDailyCost(
            actual_usd=round(day_net[latest], 2),
            avg_7d_usd=round(sum(daily) / len(daily), 2),
            projected_24h_usd=round(projected_24h, 2),
            cost_basis="complete" if complete_days else "partial",
        )
    return out
