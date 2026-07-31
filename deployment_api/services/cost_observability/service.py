"""Cost observability service — fetch, cache, and aggregate normalized cost facts.

One cached primitive (`_window_table`) returns a compact Arrow window (from the GCS parquet
snapshot, or the live providers on fallback); every API view (summary / breakdown / timeseries)
is a DuckDB GROUP BY over that table (`aggregate_arrow`), so the raw fact rows never materialize
in Python. Per-cloud failure is isolated — if Athena is down, GCP + GitHub still render.

Split across sibling modules 2026-07-31 (``plans/active/issues/deployment_api_qg_size_gate_debt_2026_07_30.md``)
to bring this file under the 900-line file-size gate and its 6 oversized methods under the
50-line method-size gate: ``row_builders.py`` (DuckDB-cell coercion + generic BreakdownRow
builders), ``resource_rows.py`` (the "resource"/"bucket"/"waste" dimension's SQL + row assembly),
``stopped_vm_disk.py`` (the `stopped_vm_disk` waste kind), ``summary_rows.py`` (per-cloud
``summarize()`` aggregation), ``breakdown_dimensions.py`` (``breakdown()``'s per-dimension
dispatch). This module keeps every public + previously-private method name unchanged (thin
wrappers delegate to the sibling modules) so callers, ``unittest.mock.patch`` targets, and
``waste.py``'s cross-reference to ``cost_observability.service._stopped_vm_disk_waste_rows`` all
keep working unchanged.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta

import pyarrow as pa

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.services.cost_observability.breakdown_dimensions import (
    _apply_share_pct,  # pyright: ignore[reportPrivateUsage]
    _breakdown_scope_totals,  # pyright: ignore[reportPrivateUsage]
    _build_breakdown_response,  # pyright: ignore[reportPrivateUsage]
    _dispatch_dimension_rows,  # pyright: ignore[reportPrivateUsage]
)
from deployment_api.services.cost_observability.cache import CostWindowCache
from deployment_api.services.cost_observability.models import (
    CLOUD_AWS,
    CLOUD_GCP,
    CLOUD_GITHUB,
    KIND_BUCKET,
    KIND_VM,
    BreakdownResponse,
    BreakdownRow,
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
from deployment_api.services.cost_observability.resource_rows import (
    _AVG_DAYS_PER_MONTH,  # pyright: ignore[reportPrivateUsage]
    _build_resource_rows,  # pyright: ignore[reportPrivateUsage]
    _cost_component,  # pyright: ignore[reportPrivateUsage]
    _resource_base_rows,  # pyright: ignore[reportPrivateUsage]
    _resource_component_class_maps,  # pyright: ignore[reportPrivateUsage]
    _resource_daily_from_grouped_rows,  # pyright: ignore[reportPrivateUsage]
)
from deployment_api.services.cost_observability.row_builders import (
    _BREAKDOWN_LIMIT,  # pyright: ignore[reportPrivateUsage]
    _by_day,  # pyright: ignore[reportPrivateUsage]
    _f,  # pyright: ignore[reportPrivateUsage]
    _finalize_rows,  # pyright: ignore[reportPrivateUsage]
    _i,  # pyright: ignore[reportPrivateUsage]
    _s,  # pyright: ignore[reportPrivateUsage]
)
from deployment_api.services.cost_observability.snapshot import (
    aggregate_arrow,
    get_cost_snapshot_store,
    records_to_table,
)
from deployment_api.services.cost_observability.stopped_vm_disk import (
    _stopped_vm_disk_rows,  # pyright: ignore[reportPrivateUsage]
)
from deployment_api.services.cost_observability.summary_rows import (
    _build_cloud_summaries,  # pyright: ignore[reportPrivateUsage]
    _cloud_current_aggregates,  # pyright: ignore[reportPrivateUsage]
)
from deployment_api.services.cost_observability.waste import (
    DEFAULT_STALE_BACKUP_DAYS,
)
from deployment_api.vm_utils import (
    list_orphaned_image_names,
    list_stale_machine_image_names,
    list_stale_snapshot_names,
    list_unattached_disk_names,
)

logger = logging.getLogger(__name__)

# Re-export marker for lint tools that flag "imported but unused" — every name below is
# unreachable from this module's own code paths post-split, but IS the public/test surface
# (`deployment_api.services.cost_observability.service.<name>`, e.g. `svc._cost_component` /
# `svc._AVG_DAYS_PER_MONTH` in tests/unit/api/test_cost_observability.py) — see the module
# docstring, same convention as `services/data_status/mtds.py`'s facade re-export.
__all__ = [
    "_AVG_DAYS_PER_MONTH",
    "_BREAKDOWN_LIMIT",
    "_MAX_DAYS",
    "CostObservabilityService",
    "_cost_component",
]

CLOUD_ORDER = [CLOUD_GCP, CLOUD_AWS, CLOUD_GITHUB]
_PROVISIONAL_TRAILING_DAYS = 2
_MAX_DAYS = 366
_DEFAULT_DAYS = 30


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
        * ``cost_basis`` — ``"complete"`` when a complete billing day exists (``actual_usd`` /
          ``projected_24h_usd`` both come from it); ``"partial"`` when no complete day exists yet and
          both fall back to the latest, still-accruing day. The UI colour-codes ``actual_usd`` off this
          flag (no text label — operator decision 4, 2026-07-20).

        Rows with no ``resource_id`` (no billing granularity) are skipped — the caller shows None.
        The per-row fold itself lives in :func:`resource_rows._resource_daily_from_grouped_rows`
        (moved out to keep this method under the 50-line size gate).
        """
        start, end, _ = self._window(days)
        table = self._window_table(start, end, force=force)
        now = datetime.now(UTC)
        today = now.date().isoformat()
        hours_billed = max((now - now.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds() / 3600, 1.0)
        rows = self._agg(
            table,
            "SELECT resource_id, day, SUM(cost + credit) FROM cost_records "
            "WHERE resource_id != '' GROUP BY resource_id, day",
        )
        return _resource_daily_from_grouped_rows(rows, today, hours_billed)

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

        cur_by_cloud, daily_by_cloud = _cloud_current_aggregates(cur, dates)
        prior_net = {
            _s(r[0]): _f(r[1])
            for r in self._agg(prior, "SELECT cloud, SUM(cost + credit) FROM cost_records GROUP BY cloud")
        }
        prior_grand = sum(prior_net.values())

        clouds, grand, grand_gross, grand_credit = _build_cloud_summaries(cur_by_cloud, daily_by_cloud, prior_net)
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
        totals, native_totals, scope_currency, total = _breakdown_scope_totals(table, cwhere, cparams, dimension)

        # "By day" stays chronological + uncapped (days are inherently bounded and the operator wants
        # every one) — every other dimension caps to the top-N with an honest "Other" roll-up.
        if dimension == "day":
            rows = _by_day(table, cwhere, cparams, cutoff, dates)
            _apply_share_pct(rows, total)
            return _build_breakdown_response(dimension, cloud, dates, total, len(rows), rows)

        rows_all, extra_aggregates, totals, native_totals, total = _dispatch_dimension_rows(
            dimension,
            table,
            cwhere,
            cparams,
            cutoff,
            dates,
            label_key,
            scope_currency,
            totals,
            native_totals,
            total,
            by_resource=self._by_resource,
            stopped_vm_disk_rows=self._stopped_vm_disk_waste_rows,
        )
        rows, total_groups = _finalize_rows(
            rows_all,
            totals=totals,
            native_totals=native_totals,
            scope_currency=scope_currency,
            extra_aggregates=extra_aggregates,
        )
        _apply_share_pct(rows, total)
        return _build_breakdown_response(dimension, cloud, dates, total, total_groups, rows)

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
        """Per-resource breakdown rows (also the "bucket"/"waste" dimension's row source once the
        caller filters/narrows). The SQL fetch, storage-class aggregation, and row assembly live in
        ``resource_rows`` (moved out to keep this method under the 50-line size gate); only the
        live-GCP waste cross-refs stay here (need ``self._cfg`` + a thread-pool fan-out)."""
        kind_filter = " AND resource_kind = 'bucket'" if kind == KIND_BUCKET else ""
        base = _resource_base_rows(table, cwhere, cparams, cutoff, kind_filter)
        xrefs = self._waste_cross_refs() if kind is None else (frozenset(), frozenset(), frozenset(), frozenset())
        comp_by_res, class_by_res = _resource_component_class_maps(table, cwhere, cparams)
        return _build_resource_rows(base, kind, xrefs, comp_by_res, class_by_res, window_days)

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

    def _orphaned_image_names(self) -> frozenset[str]:
        """Custom-image names NOT referenced as `source_image` by any live disk — see waste.py."""
        project_id = self._cfg.gcp_project_id
        if not project_id:
            return frozenset()
        return frozenset(list_orphaned_image_names(project_id))

    def _stale_machine_image_names(self) -> frozenset[str]:
        project_id = self._cfg.gcp_project_id
        if not project_id:
            return frozenset()
        return frozenset(list_stale_machine_image_names(project_id, DEFAULT_STALE_BACKUP_DAYS, datetime.now(UTC)))

    def _stale_snapshot_names(self) -> frozenset[str]:
        project_id = self._cfg.gcp_project_id
        if not project_id:
            return frozenset()
        return frozenset(list_stale_snapshot_names(project_id, DEFAULT_STALE_BACKUP_DAYS, datetime.now(UTC)))

    def _waste_cross_refs(self) -> tuple[frozenset[str], frozenset[str], frozenset[str], frozenset[str]]:
        """Run the 4 independent live-GCP waste cross-refs CONCURRENTLY (mirrors the
        `ThreadPoolExecutor` pattern in `routes._cloud_run_executions`) — sequential, these measured
        ~6s combined live (unattached disks/orphaned images/stale machine images/stale snapshots each
        their own Compute API round-trip); none depends on another's result.
        """
        fns: tuple[Callable[[], frozenset[str]], ...] = (
            self._unattached_disk_names,
            self._orphaned_image_names,
            self._stale_machine_image_names,
            self._stale_snapshot_names,
        )
        with ThreadPoolExecutor(max_workers=len(fns), thread_name_prefix="waste-xref") as pool:
            unattached, orphaned_images, stale_machine_images, stale_snapshots = pool.map(lambda f: f(), fns)
        return unattached, orphaned_images, stale_machine_images, stale_snapshots

    def _stopped_vm_disk_waste_rows(
        self, table: pa.Table, cwhere: str, cparams: Sequence[object]
    ) -> list[BreakdownRow]:
        """Disk (`PD Capacity`) spend billed after a VM's own compute usage stopped appearing —
        see :func:`stopped_vm_disk._stopped_vm_disk_rows` for the full docstring (moved there to
        keep this wrapper under the method-size gate). Kept as a bound method — not inlined at the
        call site — so ``waste.py``'s ``cost_observability.service._stopped_vm_disk_waste_rows``
        cross-reference stays accurate and ``breakdown()`` can pass it as a callable."""
        return _stopped_vm_disk_rows(table, cwhere, cparams)

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
