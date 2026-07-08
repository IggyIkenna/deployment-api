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
    BreakdownResponse,
    BreakdownRow,
    CloudSummary,
    CostRecord,
    SummaryResponse,
    TimeseriesPoint,
    TimeseriesResponse,
)
from deployment_api.services.cost_observability.providers import aws_facts, gcp_facts, github_facts

logger = logging.getLogger(__name__)

CLOUD_ORDER = [CLOUD_GCP, CLOUD_AWS, CLOUD_GITHUB]
_PROVISIONAL_TRAILING_DAYS = 2
_MAX_DAYS = 366
_DEFAULT_DAYS = 30
_BREAKDOWN_LIMIT = 50


def _net(r: CostRecord) -> float:
    """What you actually pay for a row: usage cost plus its credits (credits are ≤ 0).

    GCP populates `credit` (promotions, CUD/SUD, free-tier); AWS/GitHub carry 0, so net == gross
    there. Every aggregation below sums net so the page reports real invoiced spend, not the
    pre-credit list cost; the summary additionally exposes gross + credit for the
    "you pay = gross - credits" headline. SSOT: codex/05-infrastructure/billing-cost-observability.md.
    """
    return r.cost + r.credit


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

        if dimension == "day":
            rows = self._by_day(recs, dates)
        elif dimension == "bucket":
            rows = self._by_resource(recs, KIND_BUCKET)
        elif dimension == "resource":
            rows = self._by_resource(recs, None)
        elif dimension == "region":
            rows = self._grouped(recs, lambda r: r.region or "global")
        elif dimension == "sku":
            rows = self._by_sku(recs)
        else:  # service (default)
            rows = self._grouped(recs, lambda r: r.service)

        total = round(sum(r.cost for r in rows), 2)
        for r in rows:
            r.share_pct = round((r.cost / total) * 100, 1) if total else 0.0
        return BreakdownResponse(dimension=dimension, cloud=cloud, days=len(dates), total=total, rows=rows)

    def _grouped(self, recs: list[CostRecord], key: Callable[[CostRecord], str]) -> list[BreakdownRow]:
        agg: dict[tuple[str, str], float] = {}
        prov: dict[tuple[str, str], bool] = {}
        for r in recs:
            k = (r.cloud, key(r))
            agg[k] = agg.get(k, 0.0) + _net(r)
            prov[k] = prov.get(k, False) or r.is_provisional
        rows = [
            BreakdownRow(
                label=label,
                cloud=cloud,
                cost=round(v, 2),
                detail=_CLOUD_LABEL.get(cloud, cloud),
                is_provisional=prov[(cloud, label)],
            )
            for (cloud, label), v in agg.items()
        ]
        rows.sort(key=lambda x: x.cost, reverse=True)
        return rows[:_BREAKDOWN_LIMIT]

    def _by_sku(self, recs: list[CostRecord]) -> list[BreakdownRow]:
        """SKU (GCP) / usage_type (AWS) breakdown — the "why is this service expensive" axis,
        e.g. Regional Coldline Class A Operations hidden inside "Cloud Storage"."""
        agg: dict[tuple[str, str, str], float] = {}
        prov: dict[tuple[str, str, str], bool] = {}
        for r in recs:
            k = (r.cloud, r.service, r.sku or "Unknown")
            agg[k] = agg.get(k, 0.0) + _net(r)
            prov[k] = prov.get(k, False) or r.is_provisional
        rows = [
            BreakdownRow(
                label=sku,
                cloud=cloud,
                cost=round(v, 2),
                detail=service,
                is_provisional=prov[(cloud, service, sku)],
            )
            for (cloud, service, sku), v in agg.items()
        ]
        rows.sort(key=lambda x: x.cost, reverse=True)
        return rows[:_BREAKDOWN_LIMIT]

    def _by_resource(self, recs: list[CostRecord], kind: str | None) -> list[BreakdownRow]:
        agg: dict[tuple[str, str], float] = {}
        detail: dict[tuple[str, str], str] = {}
        kind_of: dict[tuple[str, str], str] = {}
        for r in recs:
            if not r.resource_id:
                continue
            if kind is not None and r.resource_kind != kind:
                continue
            k = (r.cloud, r.resource_id)
            agg[k] = agg.get(k, 0.0) + _net(r)
            detail[k] = r.service
            # A resource keeps one kind; the VM/bucket split lets the UI drive its leaf tables.
            if r.resource_kind != KIND_OTHER or k not in kind_of:
                kind_of[k] = r.resource_kind
        rows = [
            BreakdownRow(
                label=rid,
                cloud=cloud,
                cost=round(v, 2),
                detail=detail[(cloud, rid)],
                resource_kind=kind_of[(cloud, rid)],
            )
            for (cloud, rid), v in agg.items()
        ]
        rows.sort(key=lambda x: x.cost, reverse=True)
        return rows[:_BREAKDOWN_LIMIT]

    def _by_day(self, recs: list[CostRecord], dates: list[str]) -> list[BreakdownRow]:
        by_day: dict[str, float] = dict.fromkeys(dates, 0.0)
        prov: dict[str, bool] = dict.fromkeys(dates, False)
        for r in recs:
            if r.day in by_day:
                by_day[r.day] += _net(r)
                prov[r.day] = prov[r.day] or r.is_provisional
        return [
            BreakdownRow(label=d, cloud=None, cost=round(by_day[d], 2), detail="", is_provisional=prov[d])
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
    out += github_facts(start, end)
    return out
