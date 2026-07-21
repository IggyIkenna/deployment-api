"""Cost observability service — fetch, cache, and aggregate normalized cost facts.

One cached primitive (`_window_table`) returns a compact Arrow window (from the GCS parquet
snapshot, or the live providers on fallback); every API view (summary / breakdown / timeseries)
is a DuckDB GROUP BY over that table (`aggregate_arrow`), so the raw fact rows never materialize
in Python. Per-cloud failure is isolated — if Athena is down, GCP + GitHub still render.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime, timedelta

import pyarrow as pa

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.services.cost_observability.cache import CostWindowCache
from deployment_api.services.cost_observability.models import (
    BUSINESS_LABEL_KEYS,
    CLOUD_AWS,
    CLOUD_GCP,
    CLOUD_GITHUB,
    KIND_BUCKET,
    KIND_VM,
    BreakdownResponse,
    BreakdownRow,
    CloudSummary,
    CostRecord,
    ResourceDailyCost,
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
from deployment_api.services.cost_observability.snapshot import (
    aggregate_arrow,
    get_cost_snapshot_store,
    records_to_table,
)
from deployment_api.services.cost_observability.waste import (
    WASTE_IDLE_ELASTIC_IP,
    WASTE_IDLE_STATIC_IP,
    WASTE_ORPHANED_DISK,
)
from deployment_api.vm_utils import list_unattached_disk_names

logger = logging.getLogger(__name__)

CLOUD_ORDER = [CLOUD_GCP, CLOUD_AWS, CLOUD_GITHUB]
_PROVISIONAL_TRAILING_DAYS = 2
_MAX_DAYS = 366
_DEFAULT_DAYS = 30
# Return up to this many real groups so the UI can PAGINATE through all of them (e.g. ~565 buckets);
# anything beyond still folds into the honest "Other" roll-up so the header total stays exact.
_BREAKDOWN_LIMIT = 1000

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


class CostObservabilityService:
    def __init__(self, config: DeploymentApiConfig | None = None) -> None:
        self._cfg = config or DeploymentApiConfig()
        # Cache the compact Arrow window table (~15 MB/window), not fat CostRecords (~300 MB) — every
        # view GROUP-BYs it in DuckDB (aggregate_arrow), so the raw rows never materialize in Python.
        self._cache: CostWindowCache[pa.Table] = CostWindowCache()

    # -- config-derived source identifiers ------------------------------------
    def _gcp_table(self) -> str:
        project = self._cfg.require_gcp_project_id()
        return f"{project}.{self._cfg.gcp_billing_dataset}.{self._cfg.gcp_billing_resource_table}"

    def _provisional_cutoff_iso(self) -> str:
        return (datetime.now(UTC).date() - timedelta(days=_PROVISIONAL_TRAILING_DAYS - 1)).isoformat()

    # -- window fetch (cached Arrow table) ------------------------------------
    def _window_table(self, start: date, end: date, *, force: bool = False) -> pa.Table:
        key = f"{start.isoformat()}:{end.isoformat()}"
        return self._cache.get_or_load(key, lambda: self._load_window_table(start, end), force=force)

    def _load_window_table(self, start: date, end: date) -> pa.Table:
        if self._cfg.is_mock_mode():
            return records_to_table(_mock_facts(start, end))
        # Fast path: the periodic GCS parquet snapshot (scripts/cost_snapshot_worker.py) sliced to
        # this window via DuckDB — no per-request BigQuery/Athena scan (bounded to ~2 scans/day/
        # cloud) and a ~3 MB local read instead of a 55-64s query. Falls through to the live
        # providers when no snapshot is present yet (bucket unprovisioned / worker not yet run), so
        # this is a safe no-op until the first snapshot lands. SSOT: billing-cost-observability.md.
        snap_t = self._snapshot_table(start, end)
        if snap_t is not None:
            return snap_t
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
                self._cfg.aws_athena_reader_role_arn,
            ),
            CLOUD_AWS,
        )
        records += _safe(lambda: github_facts(start, end), CLOUD_GITHUB)
        return records_to_table(records)

    def _snapshot_table(self, start: date, end: date) -> pa.Table | None:
        """Arrow window table from the GCS parquet snapshot, or None if none is present.

        None (not an empty table) signals "no snapshot available → use the live providers"; an
        empty table would wrongly render a blank page while the export is healthy. Any snapshot/
        DuckDB error degrades to None (fall back to live), never a 5xx.
        """
        project_id = self._cfg.gcp_project_id
        if not project_id:
            return None
        try:
            store = get_cost_snapshot_store(project_id, self._cfg.effective_state_bucket)
            store.ensure_fresh()
            if not store.present_clouds():
                return None
            return store.window_table(start.isoformat(), end.isoformat())
        except Exception as exc:  # snapshot must never break the page — fall back to live
            logger.warning("cost snapshot read failed (%s) — falling back to live query", exc)
            return None

    def _agg(self, table: pa.Table, sql: str, params: Sequence[object] | None = None) -> list[tuple[object, ...]]:
        """Run one GROUP BY over the window ``table`` in DuckDB (small result, no row materialization)."""
        return aggregate_arrow(table, sql, params)

    # -- window math helper ---------------------------------------------------
    def _window(self, days: int) -> tuple[date, date, list[str]]:
        days = max(1, min(_MAX_DAYS, days))
        end = datetime.now(UTC).date() + timedelta(days=1)  # exclusive; includes today (partial)
        start = end - timedelta(days=days)
        dates = [(start + timedelta(days=i)).isoformat() for i in range(days)]
        return start, end, dates

    def _window_from_range(self, start_date: date, end_date: date) -> tuple[date, date, list[str]]:
        """An EXPLICIT window from an operator-picked ``[start_date, end_date]``.

        Both bounds are INCLUSIVE (a picker's "1 May → 15 May" means both endpoints are in), which
        is why ``end`` comes back as ``end_date + 1 day``: every downstream consumer — the cached
        ``_window_table``, the snapshot slice, the providers — takes an EXCLUSIVE end, the same
        contract ``_window`` honours by returning ``today + 1``.

        Returns the identical ``(start, end, dates)`` triple as ``_window`` so every view
        (summary / breakdown / timeseries) is range-capable without touching its aggregation.
        Bounds are normalized rather than raised on (the route is the loud gate — see
        ``routes/costs.py::_resolve_range``): an inverted pair is swapped and an over-long span is
        clamped to ``_MAX_DAYS``, mirroring ``_window``'s own clamp, so a direct service caller
        can't produce an unbounded scan.
        """
        if end_date < start_date:
            start_date, end_date = end_date, start_date
        span = min((end_date - start_date).days + 1, _MAX_DAYS)
        end = start_date + timedelta(days=span)  # exclusive
        dates = [(start_date + timedelta(days=i)).isoformat() for i in range(span)]
        return start_date, end, dates

    def _resolve_window(
        self, days: int, start_date: date | None, end_date: date | None
    ) -> tuple[date, date, list[str]]:
        """Explicit range when BOTH bounds are given, else the trailing ``days`` window.

        Both-or-neither: a half-specified range is ambiguous (is the other end today, or the start
        of the data?), so the route rejects it rather than guessing here.
        """
        if start_date is not None and end_date is not None:
            return self._window_from_range(start_date, end_date)
        return self._window(days)

    # -- per-resource daily cost (the deployment inventory cost column) --------
    def per_resource_daily(self, days: int = 7, *, force: bool = False) -> dict[str, ResourceDailyCost]:
        """Three USD daily-cost figures per billing ``resource_id`` over a trailing window.

        Reuses the cached window fetch (so calling this per inventory refresh is cheap after the
        first load). Net = cost + credit, USD (GCP already converted). Per resource:

        * ``actual_usd`` — net on the most recent COMPLETE day (excludes today's partial/provisional
          figure when a completed day exists; falls back to the latest day otherwise).
        * ``avg_7d_usd`` — total net over the window ÷ the count of days the resource actually has
          billing rows (NOT the fixed window length — a 1-day-old resource averages over its 1 day,
          not 7, so it doesn't read as artificially cheap).
        * ``projected_24h_usd`` — net on the most recent COMPLETE billing day (so a resource with any
          complete day legitimately reads ``actual_usd == projected_24h_usd``); falls back to the
          latest PARTIAL day normalised to a full 24h (``day_cost / hours_billed x 24``) only when no
          complete day exists yet. ``hours_billed`` is wall-clock hours elapsed since UTC midnight for
          that partial day — not a new hourly billing query (the snapshot is daily-grained) — floored
          at 1h so the first few minutes of a new day don't produce a runaway multiplier.

        Rows with no ``resource_id`` (no billing granularity) are skipped — the caller shows None.
        """
        start, end, _ = self._window(days)
        table = self._window_table(start, end, force=force)
        now = datetime.now(UTC)
        today = now.date().isoformat()
        hours_billed = max((now - now.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds() / 3600, 1.0)

        by_res_day: dict[str, dict[str, float]] = {}
        for rid, day, net in self._agg(
            table,
            "SELECT resource_id, day, SUM(cost + credit) FROM cost_records "
            "WHERE resource_id != '' GROUP BY resource_id, day",
        ):
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
            )
        return out

    # -- summary --------------------------------------------------------------
    def summarize(
        self,
        days: int = _DEFAULT_DAYS,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        force: bool = False,
    ) -> SummaryResponse:
        start, end, dates = self._resolve_window(days, start_date, end_date)
        cur = self._window_table(start, end, force=force)
        # The delta's comparison window is the SAME-LENGTH span immediately before this one — true
        # for a trailing window and an explicit range alike, so "vs prior" stays meaningful when an
        # operator picks e.g. 1-15 May (compares against 16-30 April).
        prior = self._window_table(start - (end - start), start, force=force)
        cutoff = self._provisional_cutoff_iso()

        # Per-cloud current aggregates + per-(cloud,day) net for the sparkline + prior net for delta.
        cur_by_cloud = {
            _s(r[0]): r
            for r in self._agg(
                cur,
                "SELECT cloud, SUM(cost) gross, SUM(credit) credit, SUM(cost_native) gross_n, "
                "SUM(credit_native) credit_n, ANY_VALUE(currency) ccy, BOOL_OR(is_placeholder) ph "
                "FROM cost_records GROUP BY cloud",
            )
        }
        day_index = {d: i for i, d in enumerate(dates)}
        daily_by_cloud: dict[str, list[float]] = {c: [0.0] * len(dates) for c in CLOUD_ORDER}
        for c, day, net in self._agg(
            cur, "SELECT cloud, day, SUM(cost + credit) FROM cost_records GROUP BY cloud, day"
        ):
            idx = day_index.get(_s(day))
            if idx is not None and _s(c) in daily_by_cloud:
                daily_by_cloud[_s(c)][idx] = _f(net)
        prior_net = {
            _s(r[0]): _f(r[1])
            for r in self._agg(prior, "SELECT cloud, SUM(cost + credit) FROM cost_records GROUP BY cloud")
        }
        prior_grand = sum(prior_net.values())

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
        grand = round(grand, 2)
        grand_delta = round(((grand - prior_grand) / prior_grand) * 100, 1) if prior_grand else None
        prov_rows = self._agg(cur, "SELECT COUNT(DISTINCT day) FROM cost_records WHERE day >= ?", [cutoff])
        provisional = (_i(prov_rows[0][0]) or 0) if prov_rows else 0
        return SummaryResponse(
            days=len(dates),
            start_date=dates[0] if dates else "",
            end_date=dates[-1] if dates else "",
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
        self,
        dimension: str,
        cloud: str = "all",
        days: int = _DEFAULT_DAYS,
        *,
        label_key: str = "purpose",
        start_date: date | None = None,
        end_date: date | None = None,
        force: bool = False,
    ) -> BreakdownResponse:
        start, end, dates = self._resolve_window(days, start_date, end_date)
        table = self._window_table(start, end, force=force)
        cutoff = self._provisional_cutoff_iso()
        cwhere, cparams = ("cloud = ?", [cloud]) if cloud != "all" else ("TRUE", [])

        # True totals for this dimension's SCOPE, summed from RAW rows in DuckDB (not from rounded
        # per-group rows) so every tab's header total equals the KPI/summary to the cent — the cap +
        # per-group rounding residual are absorbed by the "Other" row. Bucket scope is buckets only;
        # every other dimension covers all rows (resource incl. the unattributed tail, surfaced below).
        scope_where = cwhere + (" AND resource_kind = 'bucket'" if dimension == "bucket" else "")
        tr = self._agg(
            table,
            "SELECT SUM(cost + credit), SUM(cost), SUM(credit), SUM(cost_native + credit_native), "
            "SUM(cost_native), SUM(credit_native), "
            f"CASE WHEN COUNT(DISTINCT currency) = 1 THEN ANY_VALUE(currency) ELSE 'USD' END "  # nosec B608
            f"FROM cost_records WHERE {scope_where}",
            list(cparams),
        )[0]
        totals = (round(_f(tr[0]), 2), round(_f(tr[1]), 2), round(_f(tr[2]), 2))
        native_totals = (round(_f(tr[3]), 2), round(_f(tr[4]), 2), round(_f(tr[5]), 2))
        # One currency only when the scope is a single cloud (the tally case: cloud=gcp → GBP); mixed → USD.
        scope_currency = _s(tr[6]) or "USD"
        total = totals[0]

        # "By day" stays chronological + uncapped (days are inherently bounded and the operator wants
        # every one) — every other dimension caps to the top-N with an honest "Other" roll-up.
        if dimension == "day":
            rows = self._by_day(table, cwhere, cparams, cutoff, dates)
            for r in rows:
                r.share_pct = round((r.cost / total) * 100, 1) if total else 0.0
            return BreakdownResponse(
                dimension=dimension,
                cloud=cloud,
                days=len(dates),
                start_date=dates[0] if dates else "",
                end_date=dates[-1] if dates else "",
                total=total,
                total_groups=len(rows),
                rows=rows,
            )

        extra_aggregates: tuple[BreakdownRow, ...] = ()
        if dimension == "bucket":
            rows_all = self._by_resource(table, cwhere, cparams, cutoff, KIND_BUCKET, window_days=len(dates))
        elif dimension == "resource":
            # window_days so bucket-kind rows in the resource view also get storage detail —
            # the "Top storage buckets" leaf table is fed by this dimension.
            rows_all = self._by_resource(table, cwhere, cparams, cutoff, None, window_days=len(dates))
            # Cost the provider tags to NO resource (Cloud Run, networking, …) is dropped by the
            # per-resource grouping; surface it as one row so the resource total reconciles to the
            # cloud total instead of silently sitting ~$365 low.
            ur = self._agg(
                table,
                "SELECT SUM(cost + credit), SUM(cost_native + credit_native) FROM cost_records "
                f"WHERE resource_id = '' AND {cwhere}",  # nosec B608 — cwhere is 'cloud = ?' or 'TRUE'
                list(cparams),
            )[0]
            unattributed = round(_f(ur[0]), 2)
            unattributed_native = round(_f(ur[1]), 2)
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
            rows_all = self._grouped(table, cwhere, cparams, cutoff, "COALESCE(NULLIF(region, ''), 'global')")
        elif dimension == "zone":
            rows_all = self._grouped(table, cwhere, cparams, cutoff, "COALESCE(NULLIF(zone, ''), 'unknown')")
        elif dimension == "sku":
            rows_all = self._by_sku(table, cwhere, cparams, cutoff)
        elif dimension == "label":
            # Spend by a resource-level GCP business label (purpose/category/venue/asset_group). GCP-only —
            # AWS/GitHub carry no labels, so their spend groups under "(unlabeled)".
            key = label_key if label_key in BUSINESS_LABEL_KEYS else "purpose"
            # NULLIF(labels,'') so an empty-labels row (stored as "") isn't fed to json_extract_string
            # as malformed JSON; a null/absent key then falls back to "(unlabeled)".
            expr = f"COALESCE(NULLIF(json_extract_string(NULLIF(labels, ''), '$.{key}'), ''), '(unlabeled)')"
            rows_all = self._grouped(table, cwhere, cparams, cutoff, expr)
        else:  # service (default)
            rows_all = self._grouped(table, cwhere, cparams, cutoff, "service")

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
            dimension=dimension,
            cloud=cloud,
            days=len(dates),
            start_date=dates[0] if dates else "",
            end_date=dates[-1] if dates else "",
            total=total,
            total_groups=total_groups,
            rows=rows,
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

    def _agg_row(
        self,
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

    def _grouped(
        self, table: pa.Table, cwhere: str, cparams: Sequence[object], cutoff: str, key_expr: str
    ) -> list[BreakdownRow]:
        rows = self._agg(
            table,
            f"SELECT cloud, {key_expr} AS label, {_agg_cols(cutoff)} FROM cost_records "  # nosec B608 — key_expr/cwhere are code-internal SQL fragments; user params are bound
            f"WHERE {cwhere} GROUP BY cloud, label ORDER BY net DESC, label",
            list(cparams),
        )
        return [
            self._agg_row(
                r, 2, label=_s(r[1]), cloud=_s(r[0]), detail=_CLOUD_LABEL.get(_s(r[0]), _s(r[0])), purchase=True
            )
            for r in rows
        ]

    def _by_sku(self, table: pa.Table, cwhere: str, cparams: Sequence[object], cutoff: str) -> list[BreakdownRow]:
        """SKU (GCP) / usage_type (AWS) breakdown — the "why is this service expensive" axis,
        e.g. Regional Coldline Class A Operations hidden inside "Cloud Storage"."""
        rows = self._agg(
            table,
            f"SELECT cloud, service, COALESCE(NULLIF(sku, ''), 'Unknown') AS sku, {_agg_cols(cutoff)} "  # nosec B608 — cwhere is code-internal; user params bound
            f"FROM cost_records WHERE {cwhere} GROUP BY cloud, service, sku ORDER BY net DESC, sku",
            list(cparams),
        )
        return [self._agg_row(r, 3, label=_s(r[2]), cloud=_s(r[0]), detail=_s(r[1])) for r in rows]

    def _by_resource(
        self,
        table: pa.Table,
        cwhere: str,
        cparams: Sequence[object],
        cutoff: str,
        kind: str | None,
        *,
        window_days: int | None = None,
    ) -> list[BreakdownRow]:
        # Heavy money grouping in DuckDB (168K rows → ~13K groups). agg_cols occupy t[2:11]; then
        # service(11), kind(12), machine(13-15), waste flags(16-18).
        kind_filter = " AND resource_kind = 'bucket'" if kind == KIND_BUCKET else ""
        base = self._agg(
            table,
            f"SELECT cloud, resource_id, {_agg_cols(cutoff)}, "  # nosec B608 — cwhere/kind_filter are code-internal; user params bound
            "ANY_VALUE(service) AS service, "
            "COALESCE(ANY_VALUE(resource_kind) FILTER (WHERE resource_kind <> 'other'), 'other') AS kind, "
            "arg_max(machine_type, day) FILTER (WHERE machine_type <> '') AS mtype, "
            "arg_max(vcpu, day) FILTER (WHERE machine_type <> '') AS mvcpu, "
            "arg_max(memory_gb, day) FILTER (WHERE machine_type <> '') AS mmem, "
            "BOOL_OR(sku LIKE '%Static Ip Charge%') AS w_ip, "
            "BOOL_OR(sku LIKE '%PD Capacity%') AS w_pd, "
            "BOOL_OR(sku LIKE '%ElasticIP:IdleAddress%') AS w_eip "
            f"FROM cost_records WHERE {cwhere} AND resource_id <> ''{kind_filter} "
            "GROUP BY cloud, resource_id ORDER BY net DESC, resource_id",
            list(cparams),
        )
        # Storage detail (bucket-kind rows only) — a small subset (~350 buckets x their SKUs), so
        # reuse the tested Python component/class classifiers rather than re-deriving them in SQL.
        unattached: frozenset[str] = self._unattached_disk_names() if kind is None else frozenset()
        comp_by_res: dict[tuple[str, str], dict[str, float]] = {}
        class_by_res: dict[tuple[str, str], dict[str, float]] = {}
        for c, rid, sku, uunit, uamt, net in self._agg(
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

        rows: list[BreakdownRow] = []
        for t in base:
            c, rid = _s(t[0]), _s(t[1])
            waste = ""
            if kind is None:  # bucket dimension never contains idle-IP/orphaned-disk rows
                if c == CLOUD_GCP and bool(t[16]):
                    waste = WASTE_IDLE_STATIC_IP
                elif c == CLOUD_GCP and bool(t[17]) and rid in unattached:
                    waste = WASTE_ORPHANED_DISK
                elif c == CLOUD_AWS and bool(t[18]):
                    waste = WASTE_IDLE_ELASTIC_IP
            row = self._agg_row(
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
                    gb_by_class = {
                        cls: round(amt * _AVG_DAYS_PER_MONTH / window_days, 2) for cls, amt in classes.items()
                    }
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
        # SQL already sorted by net DESC; _finalize_rows applies the top-N cap and keeps idle/
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

    def _by_day(
        self, table: pa.Table, cwhere: str, cparams: Sequence[object], cutoff: str, dates: list[str]
    ) -> list[BreakdownRow]:
        by_day = {
            _s(r[0]): r
            for r in self._agg(
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
                rows.append(self._agg_row(r, 1, label=d, cloud=None, detail=""))
        return rows

    # -- timeseries -----------------------------------------------------------
    def timeseries(
        self,
        days: int = _DEFAULT_DAYS,
        cloud: str = "all",
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        force: bool = False,
    ) -> TimeseriesResponse:
        start, end, dates = self._resolve_window(days, start_date, end_date)
        table = self._window_table(start, end, force=force)
        clouds = CLOUD_ORDER if cloud == "all" else [cloud]
        ph = ", ".join(["?"] * len(clouds))
        rows = self._agg(
            table,
            f"SELECT day, cloud, SUM(cost + credit) AS net FROM cost_records WHERE cloud IN ({ph}) "  # nosec B608 — ph is '?' placeholders
            "GROUP BY day, cloud",
            list(clouds),
        )
        by_day: dict[str, dict[str, float]] = {d: dict.fromkeys(clouds, 0.0) for d in dates}
        for day, c, net in rows:
            day_s, c_s = _s(day), _s(c)
            if day_s in by_day and c_s in by_day[day_s]:
                by_day[day_s][c_s] = _f(net)
        points = [TimeseriesPoint(date=d, values={c: round(by_day[d][c], 4) for c in clouds}) for d in dates]
        return TimeseriesResponse(
            days=len(dates),
            start_date=dates[0] if dates else "",
            end_date=dates[-1] if dates else "",
            clouds=clouds,
            points=points,
        )


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
