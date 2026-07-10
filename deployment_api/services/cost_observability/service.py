"""Cost observability service — fetch, cache, and aggregate normalized cost facts.

One cached primitive (`_fetch_window`) pulls all three clouds for a date window; every
API view (summary / breakdown / timeseries) is derived from that in memory. Per-cloud
failure is isolated — if Athena is down, GCP + GitHub still render (shard-level isolation).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.services.cost_observability.cache import CostWindowCache
from deployment_api.services.cost_observability.models import (
    CLOUD_AWS,
    CLOUD_GCP,
    CLOUD_GITHUB,
    KIND_BUCKET,
    KIND_OTHER,
    KIND_VM,
    PURCHASE_ON_DEMAND,
    PURCHASE_OTHER,
    PURCHASE_SPOT,
    BreakdownResponse,
    BreakdownRow,
    CloudSummary,
    CostRecord,
    SummaryResponse,
    TimeseriesPoint,
    TimeseriesResponse,
)
from deployment_api.services.cost_observability.providers import (
    aws_facts,
    gcp_facts,
    github_dummy_facts,
    github_facts,
)
from deployment_api.services.cost_observability.waste import classify_waste
from deployment_api.vm_utils import list_unattached_disk_names

logger = logging.getLogger(__name__)

CLOUD_ORDER = [CLOUD_GCP, CLOUD_AWS, CLOUD_GITHUB]
_PROVISIONAL_TRAILING_DAYS = 2
_MAX_DAYS = 366
_DEFAULT_DAYS = 30
_BREAKDOWN_LIMIT = 100

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


def _storage_class(r: CostRecord) -> str | None:
    if r.cloud == CLOUD_GCP:
        return _gcp_storage_class(r.sku) if r.usage_unit == "gibibyte month" else None
    if r.cloud == CLOUD_AWS:
        return _aws_storage_class(r.sku)
    return None


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


# Fold order for a group's purchase_option: "any spot line present" outranks "any on-demand
# line present" outranks "other-only" — surfaces "this resource/service had SOME spot cost"
# rather than an arbitrary last-seen value across its (usually many) underlying SKU lines.
_PURCHASE_RANK = {PURCHASE_SPOT: 2, PURCHASE_ON_DEMAND: 1, PURCHASE_OTHER: 0}


def _net(r: CostRecord) -> float:
    """What you actually pay for a row: usage cost plus its credits (credits are ≤ 0).

    GCP populates `credit` (promotions, CUD/SUD, free-tier); AWS/GitHub carry 0, so net == gross
    there. Every aggregation below sums net so the page reports real invoiced spend, not the
    pre-credit list cost; the summary additionally exposes gross + credit for the
    "you pay = gross - credits" headline. SSOT: codex/05-infrastructure/billing-cost-observability.md.
    """
    return r.cost + r.credit


def _net_native(r: CostRecord) -> float:
    """`_net` in the record's native currency (for the GBP-tally view; == `_net` for USD-native clouds)."""
    return r.cost_native + r.credit_native


class _NativeAcc:
    """Per-key native-currency accumulator (net/gross/credit + currency) so every breakdown builder
    threads the identical GBP-tally figures. `add(key, r)` in the loop; `apply(row, key)` stamps the
    four native fields onto a freshly-built BreakdownRow. Keeps the builders DRY and one-currency-safe."""

    def __init__(self) -> None:
        self._net: dict[object, float] = {}
        self._gross: dict[object, float] = {}
        self._credit: dict[object, float] = {}
        self._ccy: dict[object, str] = {}

    def add(self, key: object, r: CostRecord) -> None:
        self._net[key] = self._net.get(key, 0.0) + _net_native(r)
        self._gross[key] = self._gross.get(key, 0.0) + r.cost_native
        self._credit[key] = self._credit.get(key, 0.0) + r.credit_native
        # A key that mixes currencies (a cross-cloud "by day" row) has no single native currency →
        # mark USD so the UI never renders a £ symbol on a summed GBP+USD figure.
        prev = self._ccy.get(key)
        self._ccy[key] = r.currency if prev is None or prev == r.currency else "USD"

    def apply(self, row: BreakdownRow, key: object) -> BreakdownRow:
        row.currency = self._ccy.get(key, "USD")
        row.cost_native = round(self._net.get(key, 0.0), 2)
        row.gross_native = round(self._gross.get(key, 0.0), 2)
        row.credit_native = round(self._credit.get(key, 0.0), 2)
        return row


class CostObservabilityService:
    def __init__(self, config: DeploymentApiConfig | None = None) -> None:
        self._cfg = config or DeploymentApiConfig()
        self._cache = CostWindowCache()

    # -- config-derived source identifiers ------------------------------------
    def _gcp_table(self) -> str:
        project = self._cfg.require_gcp_project_id()
        return f"{project}.{self._cfg.gcp_billing_dataset}.{self._cfg.gcp_billing_resource_table}"

    # -- window fetch (cached) ------------------------------------------------
    def _fetch_window(self, start: date, end: date, *, force: bool = False) -> list[CostRecord]:
        key = f"{start.isoformat()}:{end.isoformat()}"
        return self._cache.get_or_load(key, lambda: self._load_window(start, end), force=force)

    def _load_window(self, start: date, end: date) -> list[CostRecord]:
        if self._cfg.is_mock_mode():
            return _mock_facts(start, end)
        cutoff = datetime.now(UTC).date() - timedelta(days=_PROVISIONAL_TRAILING_DAYS - 1)
        records: list[CostRecord] = []
        # Per-cloud isolation: a failure in one provider must not blank the whole page.
        records += _safe(lambda: gcp_facts(self._gcp_table(), start, end, cutoff), CLOUD_GCP)
        records += _safe(
            lambda: aws_facts(
                self._cfg.aws_cur_database,
                self._cfg.aws_cur_table,
                self._cfg.aws_cur_region,
                self._cfg.aws_athena_output_bucket,
                start,
                end,
                cutoff,
            ),
            CLOUD_AWS,
        )
        records += _safe(lambda: github_facts(start, end), CLOUD_GITHUB)
        return records

    # -- window math helper ---------------------------------------------------
    def _window(self, days: int) -> tuple[date, date, list[str]]:
        days = max(1, min(_MAX_DAYS, days))
        end = datetime.now(UTC).date() + timedelta(days=1)  # exclusive; includes today (partial)
        start = end - timedelta(days=days)
        dates = [(start + timedelta(days=i)).isoformat() for i in range(days)]
        return start, end, dates

    # -- summary --------------------------------------------------------------
    def summarize(self, days: int = _DEFAULT_DAYS, *, force: bool = False) -> SummaryResponse:
        start, end, dates = self._window(days)
        cur = self._fetch_window(start, end, force=force)
        prior = self._fetch_window(start - (end - start), start, force=force)

        day_index = {d: i for i, d in enumerate(dates)}
        clouds: list[CloudSummary] = []
        grand = 0.0
        grand_gross = 0.0
        grand_credit = 0.0
        for cloud in CLOUD_ORDER:
            cur_recs = [r for r in cur if r.cloud == cloud]
            gross = round(sum(r.cost for r in cur_recs), 2)
            credit = round(sum(r.credit for r in cur_recs), 2)
            total = round(gross + credit, 2)  # net — what actually gets invoiced
            # Native-currency KPI figures for the GBP-tally toggle (GCP=GBP; USD clouds mirror the USD).
            gross_native = round(sum(r.cost_native for r in cur_recs), 2)
            credit_native = round(sum(r.credit_native for r in cur_recs), 2)
            native_currency = next((r.currency for r in cur_recs), "USD")
            grand += total
            grand_gross += gross
            grand_credit += credit
            daily = [0.0] * len(dates)
            for r in cur_recs:
                idx = day_index.get(r.day)
                if idx is not None:
                    daily[idx] += _net(r)
            prior_total = sum(_net(r) for r in prior if r.cloud == cloud)
            delta = round(((total - prior_total) / prior_total) * 100, 1) if prior_total else None
            clouds.append(
                CloudSummary(
                    cloud=cloud,
                    total=total,
                    gross=gross,
                    credit=credit,
                    delta_pct=delta,
                    daily=[round(v, 4) for v in daily],
                    is_placeholder=any(r.is_placeholder for r in cur_recs),
                    currency=native_currency,
                    total_native=round(gross_native + credit_native, 2),
                    gross_native=gross_native,
                    credit_native=credit_native,
                )
            )
        grand = round(grand, 2)
        prior_grand = sum(_net(r) for r in prior)
        grand_delta = round(((grand - prior_grand) / prior_grand) * 100, 1) if prior_grand else None
        provisional = len({r.day for r in cur if r.is_provisional})
        return SummaryResponse(
            days=len(dates),
            total=grand,
            gross=round(grand_gross, 2),
            credit=round(grand_credit, 2),
            run_rate_daily=round(grand / len(dates), 2) if dates else 0.0,
            delta_pct=grand_delta,
            dates=dates,
            clouds=clouds,
            provisional_days=provisional,
            generated_at=datetime.now(UTC).isoformat(),
        )

    # -- breakdown ------------------------------------------------------------
    def breakdown(
        self, dimension: str, cloud: str = "all", days: int = _DEFAULT_DAYS, *, force: bool = False
    ) -> BreakdownResponse:
        start, end, dates = self._window(days)
        recs = self._fetch_window(start, end, force=force)
        if cloud != "all":
            recs = [r for r in recs if r.cloud == cloud]

        # True totals for this dimension's SCOPE, summed from RAW records (not from rounded per-group
        # rows) so every tab's header total equals the KPI/summary to the cent — the cap + per-group
        # rounding residual are absorbed by the "Other" row, never left as a cross-tab mismatch.
        # Bucket scope is buckets only; every other dimension covers all records (resource incl. the
        # unattributed tail, which is surfaced as its own row below).
        covered = [r for r in recs if r.resource_kind == KIND_BUCKET] if dimension == "bucket" else recs
        t_net = t_gross = t_credit = 0.0
        t_net_native = t_gross_native = t_credit_native = 0.0
        for r in covered:
            t_net += _net(r)
            t_gross += r.cost
            t_credit += r.credit
            t_net_native += _net_native(r)
            t_gross_native += r.cost_native
            t_credit_native += r.credit_native
        totals = (round(t_net, 2), round(t_gross, 2), round(t_credit, 2))
        native_totals = (round(t_net_native, 2), round(t_gross_native, 2), round(t_credit_native, 2))
        # One currency only when the scope is a single cloud (the tally case: cloud=gcp → GBP); mixed → USD.
        _ccy = {r.currency for r in covered}
        scope_currency = _ccy.pop() if len(_ccy) == 1 else "USD"
        total = totals[0]

        # "By day" stays chronological + uncapped (days are inherently bounded and the operator wants
        # every one) — every other dimension caps to the top-N with an honest "Other" roll-up.
        if dimension == "day":
            rows = self._by_day(recs, dates)
            for r in rows:
                r.share_pct = round((r.cost / total) * 100, 1) if total else 0.0
            return BreakdownResponse(
                dimension=dimension, cloud=cloud, days=len(dates), total=total, total_groups=len(rows), rows=rows
            )

        extra_aggregates: tuple[BreakdownRow, ...] = ()
        if dimension == "bucket":
            rows_all = self._by_resource(recs, KIND_BUCKET, window_days=len(dates))
        elif dimension == "resource":
            # window_days so bucket-kind rows in the resource view also get storage detail —
            # the "Top storage buckets" leaf table is fed by this dimension.
            rows_all = self._by_resource(recs, None, window_days=len(dates))
            # Cost the provider tags to NO resource (Cloud Run, networking, …) is dropped by the
            # per-resource grouping; surface it as one row so the resource total reconciles to the
            # cloud total instead of silently sitting ~$365 low.
            unattributed = round(sum(_net(r) for r in recs if not r.resource_id), 2)
            unattributed_native = round(sum(_net_native(r) for r in recs if not r.resource_id), 2)
            if abs(unattributed) >= 0.01:
                extra_aggregates = (
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
        elif dimension == "region":
            rows_all = self._grouped(recs, lambda r: r.region or "global")
        elif dimension == "zone":
            rows_all = self._grouped(recs, lambda r: r.zone or "unknown")
        elif dimension == "sku":
            rows_all = self._by_sku(recs)
        else:  # service (default)
            rows_all = self._grouped(recs, lambda r: r.service)

        rows, total_groups = self._finalize_rows(
            rows_all,
            totals=totals,
            native_totals=native_totals,
            scope_currency=scope_currency,
            extra_aggregates=extra_aggregates,
        )
        for r in rows:
            r.share_pct = round((r.cost / total) * 100, 1) if total else 0.0
        return BreakdownResponse(
            dimension=dimension, cloud=cloud, days=len(dates), total=total, total_groups=total_groups, rows=rows
        )

    def _finalize_rows(
        self,
        rows_all: list[BreakdownRow],
        *,
        totals: tuple[float, float, float],
        native_totals: tuple[float, float, float],
        scope_currency: str,
        extra_aggregates: tuple[BreakdownRow, ...] = (),
    ) -> tuple[list[BreakdownRow], int]:
        """Cap a cost-sorted (descending) row list to the top `_BREAKDOWN_LIMIT`, folding the rest into
        ONE ``Other (N more)`` row whose cost/gross/credit are the RESIDUAL vs the true `totals` — so the
        shown rows sum to the header total EXACTLY (to the cent), absorbing per-group rounding. Idle/
        orphaned rows below the cap stay visible (never folded, so the waste-surfacing survives).
        `extra_aggregates` (e.g. an ``Unattributed`` row) are appended after Other and counted against
        the residual. Returns (rows_to_show, total_group_count).
        """
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

    def _grouped(self, recs: list[CostRecord], key: Callable[[CostRecord], str]) -> list[BreakdownRow]:
        net: dict[tuple[str, str], float] = {}
        gross: dict[tuple[str, str], float] = {}
        credit: dict[tuple[str, str], float] = {}
        acc = _NativeAcc()
        prov: dict[tuple[str, str], bool] = {}
        purchase: dict[tuple[str, str], str] = {}
        for r in recs:
            k = (r.cloud, key(r))
            net[k] = net.get(k, 0.0) + _net(r)
            gross[k] = gross.get(k, 0.0) + r.cost
            credit[k] = credit.get(k, 0.0) + r.credit
            acc.add(k, r)
            prov[k] = prov.get(k, False) or r.is_provisional
            if _PURCHASE_RANK.get(r.purchase_option, 0) > _PURCHASE_RANK.get(purchase.get(k, PURCHASE_OTHER), 0):
                purchase[k] = r.purchase_option
        rows = [
            acc.apply(
                BreakdownRow(
                    label=label,
                    cloud=cloud,
                    cost=round(v, 2),
                    gross=round(gross[(cloud, label)], 2),
                    credit=round(credit[(cloud, label)], 2),
                    detail=_CLOUD_LABEL.get(cloud, cloud),
                    is_provisional=prov[(cloud, label)],
                    purchase_option=purchase.get((cloud, label), PURCHASE_OTHER),
                ),
                (cloud, label),
            )
            for (cloud, label), v in net.items()
        ]
        rows.sort(key=lambda x: x.cost, reverse=True)
        return rows

    def _by_sku(self, recs: list[CostRecord]) -> list[BreakdownRow]:
        """SKU (GCP) / usage_type (AWS) breakdown — the "why is this service expensive" axis,
        e.g. Regional Coldline Class A Operations hidden inside "Cloud Storage"."""
        net: dict[tuple[str, str, str], float] = {}
        gross: dict[tuple[str, str, str], float] = {}
        credit: dict[tuple[str, str, str], float] = {}
        prov: dict[tuple[str, str, str], bool] = {}
        acc = _NativeAcc()
        for r in recs:
            k = (r.cloud, r.service, r.sku or "Unknown")
            net[k] = net.get(k, 0.0) + _net(r)
            gross[k] = gross.get(k, 0.0) + r.cost
            credit[k] = credit.get(k, 0.0) + r.credit
            acc.add(k, r)
            prov[k] = prov.get(k, False) or r.is_provisional
        rows = [
            acc.apply(
                BreakdownRow(
                    label=sku,
                    cloud=cloud,
                    cost=round(v, 2),
                    gross=round(gross[(cloud, service, sku)], 2),
                    credit=round(credit[(cloud, service, sku)], 2),
                    detail=service,
                    is_provisional=prov[(cloud, service, sku)],
                ),
                (cloud, service, sku),
            )
            for (cloud, service, sku), v in net.items()
        ]
        rows.sort(key=lambda x: x.cost, reverse=True)
        return rows

    def _by_resource(
        self, recs: list[CostRecord], kind: str | None, *, window_days: int | None = None
    ) -> list[BreakdownRow]:
        net: dict[tuple[str, str], float] = {}
        gross: dict[tuple[str, str], float] = {}
        credit: dict[tuple[str, str], float] = {}
        detail: dict[tuple[str, str], str] = {}
        kind_of: dict[tuple[str, str], str] = {}
        waste_of: dict[tuple[str, str], str] = {}
        # Only the unfiltered "resource" dimension (kind=None) surfaces waste flags — the
        # bucket dimension never contains idle-IP/orphaned-disk rows, so skip the fleet lookup.
        unattached_disks = self._unattached_disk_names() if kind is None else frozenset()
        storage_amt: dict[tuple[str, str], dict[str, float]] = {}
        # Per-bucket net cost split by SKU component (storage / operations / egress / other). Its
        # "storage" entry doubles as the $/GB numerator so the rate and the Storage column agree.
        component_cost: dict[tuple[str, str], dict[str, float]] = {}
        purchase: dict[tuple[str, str], str] = {}
        machine_of: dict[tuple[str, str], tuple[str, str, int | None, float | None]] = {}
        acc = _NativeAcc()
        for r in recs:
            if not r.resource_id:
                continue
            if kind is not None and r.resource_kind != kind:
                continue
            k = (r.cloud, r.resource_id)
            net[k] = net.get(k, 0.0) + _net(r)
            gross[k] = gross.get(k, 0.0) + r.cost
            credit[k] = credit.get(k, 0.0) + r.credit
            acc.add(k, r)
            detail[k] = r.service
            # A resource keeps one kind; the VM/bucket split lets the UI drive its leaf tables.
            if r.resource_kind != KIND_OTHER or k not in kind_of:
                kind_of[k] = r.resource_kind
            if not waste_of.get(k):
                waste = classify_waste(
                    cloud=r.cloud, sku=r.sku, resource_id=r.resource_id, unattached_disk_names=unattached_disks
                )
                if waste:
                    waste_of[k] = waste
            # Storage detail attaches to any BUCKET-kind row (its own kind, not the query
            # dimension) so the resource dimension's bucket rows — which feed the "Top storage
            # buckets" leaf table — also carry it, not just the By-bucket view.
            if r.resource_kind == KIND_BUCKET:
                by_comp = component_cost.setdefault(k, {})
                comp = _cost_component(r.cloud, r.sku)
                by_comp[comp] = by_comp.get(comp, 0.0) + _net(r)
                cls = _storage_class(r)
                if cls is not None:
                    by_class = storage_amt.setdefault(k, {})
                    by_class[cls] = by_class.get(cls, 0.0) + r.usage_amount
            if _PURCHASE_RANK.get(r.purchase_option, 0) > _PURCHASE_RANK.get(purchase.get(k, PURCHASE_OTHER), 0):
                purchase[k] = r.purchase_option
            # A VM's disk/IP SKU rows carry no machine spec (only its Core/Ram rows do) — keep
            # the latest-day non-empty spec seen across the resource's rows.
            if r.machine_type:
                prev_day = machine_of.get(k, ("", "", None, None))[0]
                if r.day >= prev_day:
                    machine_of[k] = (r.day, r.machine_type, r.vcpu, r.memory_gb)
        rows = []
        for (cloud, rid), v in net.items():
            _, machine_type, vcpu, memory_gb = machine_of.get((cloud, rid), ("", "", None, None))
            components = component_cost.get((cloud, rid), {})
            row = BreakdownRow(
                label=rid,
                cloud=cloud,
                cost=round(v, 2),
                gross=round(gross[(cloud, rid)], 2),
                credit=round(credit[(cloud, rid)], 2),
                detail=detail[(cloud, rid)],
                resource_kind=kind_of[(cloud, rid)],
                is_idle=(cloud, rid) in waste_of,
                waste_kind=waste_of.get((cloud, rid), ""),
                purchase_option=purchase.get((cloud, rid), PURCHASE_OTHER),
                machine_type=machine_type,
                vcpu=vcpu,
                memory_gb=memory_gb,
            )
            if window_days:
                classes = storage_amt.get((cloud, rid))
                if classes:
                    gb_by_class = {
                        cls: round(amt * _AVG_DAYS_PER_MONTH / window_days, 2) for cls, amt in classes.items()
                    }
                    total_gb = round(sum(gb_by_class.values()), 2)
                    row.storage_gb = total_gb
                    row.storage_class_gb = gb_by_class
                    if total_gb > 0:
                        # $/GB is the effective STORAGE rate — the storage COMPONENT cost over stored
                        # GB, NOT the row's total (operations-dominated) cost. An events bucket bills
                        # ~$2.5k in Class-A writes on ~95 GB stored; total/GB reads a nonsense
                        # ~$25/GB, storage-cost/GB reads the real ~$0.013/GB.
                        row.cost_per_gb = round(components.get("storage", 0.0) / total_gb, 4)
            # Cost composition (bucket rows only carry component_cost) — drop components that round
            # to $0.00 so the UI shows only real drivers; those kept sum to ~`cost` (net).
            kept = {c: round(x, 2) for c, x in components.items() if round(x, 2) != 0.0}
            if kept:
                row.cost_by_component = kept
            acc.apply(row, (cloud, rid))
            rows.append(row)
        rows.sort(key=lambda x: x.cost, reverse=True)
        # Uncapped + cost-sorted; breakdown() → _finalize_rows applies the top-N cap and keeps idle/
        # orphaned rows visible below it (so the waste-surfacing survives the cap).
        return rows

    def _unattached_disk_names(self) -> frozenset[str]:
        """Live UNATTACHED persistent-disk names for GCP orphaned-disk detection.

        A disk with an empty Compute `users` field is attached to nothing — orphaned, still
        billing `PD Capacity`. Degrades to an empty set (never flags a false-positive orphan)
        when no project is configured — `list_unattached_disk_names` itself already degrades to
        empty on API failure.
        """
        project_id = self._cfg.gcp_project_id
        if not project_id:
            return frozenset()
        return frozenset(list_unattached_disk_names(project_id))

    def _by_day(self, recs: list[CostRecord], dates: list[str]) -> list[BreakdownRow]:
        net: dict[str, float] = dict.fromkeys(dates, 0.0)
        gross: dict[str, float] = dict.fromkeys(dates, 0.0)
        credit: dict[str, float] = dict.fromkeys(dates, 0.0)
        prov: dict[str, bool] = dict.fromkeys(dates, False)
        acc = _NativeAcc()
        for r in recs:
            if r.day in net:
                net[r.day] += _net(r)
                gross[r.day] += r.cost
                credit[r.day] += r.credit
                acc.add(r.day, r)
                prov[r.day] = prov[r.day] or r.is_provisional
        return [
            acc.apply(
                BreakdownRow(
                    label=d,
                    cloud=None,
                    cost=round(net[d], 2),
                    gross=round(gross[d], 2),
                    credit=round(credit[d], 2),
                    detail="",
                    is_provisional=prov[d],
                ),
                d,
            )
            for d in reversed(dates)
        ]

    # -- timeseries -----------------------------------------------------------
    def timeseries(self, days: int = _DEFAULT_DAYS, cloud: str = "all", *, force: bool = False) -> TimeseriesResponse:
        start, end, dates = self._window(days)
        recs = self._fetch_window(start, end, force=force)
        clouds = CLOUD_ORDER if cloud == "all" else [cloud]
        by_day: dict[str, dict[str, float]] = {d: dict.fromkeys(clouds, 0.0) for d in dates}
        for r in recs:
            if r.cloud in clouds and r.day in by_day:
                by_day[r.day][r.cloud] += _net(r)
        points = [TimeseriesPoint(date=d, values={c: round(by_day[d][c], 4) for c in clouds}) for d in dates]
        return TimeseriesResponse(days=len(dates), clouds=clouds, points=points)


_CLOUD_LABEL = {CLOUD_GCP: "GCP", CLOUD_AWS: "AWS", CLOUD_GITHUB: "GitHub"}


def _safe(loader: Callable[[], list[CostRecord]], cloud: str) -> list[CostRecord]:
    try:
        return loader()
    except Exception as exc:
        logger.warning("Cost provider %s failed: %s", cloud, exc)
        return []


# --- mock data (mock mode / tests, no cloud creds) ---------------------------
def _mock_facts(start: date, end: date) -> list[CostRecord]:
    gcp_mix = [
        ("Cloud Run", "svc/deployment-api", "other", "asia-northeast1", 46.0),
        ("Compute Engine", "mtds-perp-funding-backfill", KIND_VM, "asia-northeast1", 12.6),
        ("Cloud Storage", "mock-events-bucket", KIND_BUCKET, "asia-northeast1", 9.5),
    ]
    aws_mix = [
        ("Amazon Elastic Compute Cloud", "i-0c9b283b31d6b5ca7", KIND_VM, "ap-northeast-1", 5.4),
        ("Amazon Simple Storage Service", "mock-instruments-bucket", KIND_BUCKET, "ap-northeast-1", 0.07),
    ]
    out: list[CostRecord] = []
    day = start
    while day < end:
        iso = day.isoformat()
        prov = day >= datetime.now(UTC).date() - timedelta(days=1)
        for service, rid, kind, region, base in gcp_mix:
            out.append(
                CostRecord(
                    cloud=CLOUD_GCP,
                    day=iso,
                    service=service,
                    resource_id=rid,
                    resource_kind=kind,
                    region=region,
                    cost=base,
                    is_provisional=prov,
                )
            )
        for service, rid, kind, region, base in aws_mix:
            out.append(
                CostRecord(
                    cloud=CLOUD_AWS,
                    day=iso,
                    service=service,
                    resource_id=rid,
                    resource_kind=kind,
                    region=region,
                    cost=base,
                    is_provisional=prov,
                )
            )
        day = day.fromordinal(day.toordinal() + 1)
    out += github_dummy_facts(start, end)  # mock mode: never touch the network
    return out
