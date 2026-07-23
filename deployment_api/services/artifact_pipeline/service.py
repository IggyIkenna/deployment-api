"""Artifact-pipeline service — the view builders + (coming) the runtime join & drift classifier.

Fetches normalized facts from the providers (each `_safe`-wrapped so one cloud never blanks the
others), caches the compact fact lists briefly, and shapes each of the five API responses. The
headline view's runtime join — a live workload's resolved image digest → Artifact Registry tag →
short SHA → Cloud Build record → git commit → an honest drift verdict — lands with the running
view; this first cut ships the pipeline (builds) view end-to-end.

SSOT: plans/active/artifact_pipeline_observability_2026_07_17.md
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.services.artifact_pipeline import providers
from deployment_api.services.artifact_pipeline.cache import ArtifactWindowCache
from deployment_api.services.artifact_pipeline.models import (
    CHANGE_CONFIG,
    CHANGE_FAILED,
    BuildFact,
    BuildRow,
    BuildsResponse,
    BuildsStats,
    BuildStep,
    DeployFact,
    DeployRow,
    DeploysResponse,
    DeploysStats,
)

_MAX_DAYS = 366


# ── pure helpers (no cloud I/O — unit-tested directly) ──────────────────────────────────────────────
def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _resolve_window(days: int, start_date: date | None, end_date: date | None) -> tuple[date, date]:
    """Both-or-neither explicit range wins; else a trailing `days` window ending today (UTC).

    Defensive normalisation for direct callers (the HTTP route is the loud 400 gate): an inverted
    range is swapped and an over-long one clamped, mirroring `cost_observability`.
    """
    today = datetime.now(UTC).date()
    if start_date is not None and end_date is not None:
        lo, hi = (start_date, end_date) if start_date <= end_date else (end_date, start_date)
        if (hi - lo).days + 1 > _MAX_DAYS:
            lo = hi - timedelta(days=_MAX_DAYS - 1)
        return lo, hi
    span = max(1, min(days, _MAX_DAYS))
    return today - timedelta(days=span - 1), today


def _fmt_duration(seconds: float | None) -> str:
    """Human duration for the 'Took' cell: '' when unknown, '41s', or '3m41s'."""
    if seconds is None:
        return ""
    total = int(seconds)
    if total < 60:
        return f"{total}s"
    return f"{total // 60}m{total % 60:02d}s"


def _in_window(iso_ts: str, lo: date, hi: date) -> bool:
    """Does an ISO timestamp's UTC date fall in [lo, hi]? Unparseable / empty → excluded."""
    if not iso_ts:
        return False
    try:
        parsed = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
    except ValueError:
        return False
    return lo <= parsed.date() <= hi


def _median(sorted_values: list[float]) -> float | None:
    if not sorted_values:
        return None
    n = len(sorted_values)
    mid = n // 2
    if n % 2 == 1:
        return sorted_values[mid]
    return (sorted_values[mid - 1] + sorted_values[mid]) / 2.0


def _one_line_failure(fact: BuildFact) -> str:
    """A single-line 'why it failed' for the row (the drawer carries the full detail)."""
    if fact.status != "FAILURE":
        return ""
    if fact.failure_detail:
        return fact.failure_detail
    if fact.failure_type:
        return fact.failure_type
    for name, status, _ in fact.steps:
        if status == "FAILURE":
            return f'step "{name}" failed'
    return "build failed"


def _build_row(fact: BuildFact, *, dup: bool, cross_lane: bool) -> BuildRow:
    return BuildRow(
        repo=fact.repo,
        lane=fact.lane,
        cloud=fact.cloud,
        status=fact.status,
        trigger=fact.trigger,
        sha=fact.sha,
        branch=fact.branch,
        started_at=fact.started_at,
        duration=_fmt_duration(fact.duration_sec),
        produced=fact.produced,
        build_id=fact.build_id,
        failure=_one_line_failure(fact),
        failure_type=fact.failure_type,
        failure_detail=fact.failure_detail,
        log_url=fact.log_url,
        dup=dup,
        cross_lane=cross_lane,
        steps=[BuildStep(name=name, status=status, seconds=sec) for name, status, sec in fact.steps],
    )


def _builds_stats(facts: list[BuildFact]) -> BuildsStats:
    """Stat tiles computed from the data (never hand-written — the tab-1 lesson)."""
    total = len(facts)
    failed = sum(1 for f in facts if f.status == "FAILURE")
    completed = sum(1 for f in facts if f.status in ("SUCCESS", "FAILURE"))
    success = sum(1 for f in facts if f.status == "SUCCESS")
    success_rate = round(100.0 * success / completed, 1) if completed else 0.0
    durations = sorted(f.duration_sec for f in facts if f.duration_sec is not None)
    counts = _sha_build_counts(facts)
    wasted = sum(c - 1 for c in counts.values() if c > 1)
    return BuildsStats(
        total=total,
        success_rate=success_rate,
        failed=failed,
        median_duration_sec=_median(durations),
        wasted_dup=wasted,
    )


def _sha_build_counts(facts: list[BuildFact]) -> dict[tuple[str, str], int]:
    """(repo, sha) → build count, for dup / wasted-build detection (blank SHAs excluded)."""
    counts: dict[tuple[str, str], int] = {}
    for f in facts:
        if f.sha:
            counts[(f.repo, f.sha)] = counts.get((f.repo, f.sha), 0) + 1
    return counts


def _deploy_row(fact: DeployFact) -> DeployRow:
    return DeployRow(
        workload=fact.workload,
        revision=fact.revision,
        cloud=fact.cloud,
        digest=fact.digest,
        built_from=fact.built_from,
        resolvable=fact.resolvable,
        change_type=fact.change_type,
        at=fact.at,
        held_for=fact.held_for,
        live=fact.live,
        deployer=fact.deployer,
        link_kind=fact.link_kind,
        section=fact.section,
    )


def _deploys_stats(windowed: list[DeployFact], *, live_now: int) -> DeploysStats:
    """Stat tiles from the data. `live_now` is a POINT-IN-TIME count over ALL facts, never the
    windowed subset — narrowing the date range must not make "how many are live right now" lie."""
    total = len(windowed)
    config_only = sum(1 for f in windowed if f.change_type == CHANGE_CONFIG)
    config_only_pct = round(100.0 * config_only / total, 1) if total else 0.0
    failed = sum(1 for f in windowed if f.change_type == CHANGE_FAILED)
    return DeploysStats(total=total, config_only_pct=config_only_pct, live_now=live_now, failed=failed)


class ArtifactPipelineService:
    """Serves the /ops/artifacts views from the (cheaply cached) normalized provider facts."""

    def __init__(self) -> None:
        self._cfg = DeploymentApiConfig()
        self._build_facts: ArtifactWindowCache[list[BuildFact]] = ArtifactWindowCache()
        self._deploy_facts: ArtifactWindowCache[list[DeployFact]] = ArtifactWindowCache()

    def _all_build_facts(self, *, force: bool = False) -> list[BuildFact]:
        """All build records (every lane, both clouds), briefly cached. `_safe` per source."""

        def _load() -> list[BuildFact]:
            facts: list[BuildFact] = []
            facts += providers.safe(lambda: providers.gcp_cloud_builds(self._cfg), "gcp_cloud_builds")
            # AWS CodeBuild (WIF) + the tarball-lane builds land in later increments — each `_safe`.
            return facts

        return self._build_facts.get_or_load("builds", _load, force=force)

    def _all_deploy_facts(self, *, force: bool = False) -> list[DeployFact]:
        """All deploy records (every workload, both clouds), briefly cached. `_safe` per source."""

        def _load() -> list[DeployFact]:
            facts: list[DeployFact] = []
            facts += providers.safe(lambda: providers.gcp_cloud_run_revisions(self._cfg), "gcp_cloud_run_revisions")
            # AWS App Runner/ECS operations + GCE VM launches (the tarball-lane deploy) are later
            # increments — each `_safe`, mirroring how AWS CodeBuild is deferred for `builds()`.
            return facts

        return self._deploy_facts.get_or_load("deploys", _load, force=force)

    def builds(
        self,
        days: int,
        cloud: str = "all",
        lane: str = "all",
        status: str = "all",
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        force: bool = False,
    ) -> BuildsResponse:
        """The Pipeline view: both clouds, both lanes, structured failure, dup / cross-lane flags."""
        lo, hi = _resolve_window(days, start_date, end_date)
        windowed = [f for f in self._all_build_facts(force=force) if _in_window(f.started_at, lo, hi)]

        # cross-lane = one commit built as image AND tarball; dup = same (repo, sha) built >1x.
        lanes_by_sha: dict[str, set[str]] = {}
        for f in windowed:
            if f.sha:
                lanes_by_sha.setdefault(f.sha, set()).add(f.lane)
        counts = _sha_build_counts(windowed)

        rows: list[BuildRow] = []
        for f in windowed:
            if cloud != "all" and f.cloud != cloud:
                continue
            if lane != "all" and f.lane != lane:
                continue
            if status == "failed" and f.status != "FAILURE":
                continue
            rows.append(
                _build_row(
                    f,
                    dup=f.sha != "" and counts.get((f.repo, f.sha), 0) >= 2,
                    cross_lane=f.sha != "" and len(lanes_by_sha.get(f.sha, set())) >= 2,
                )
            )

        return BuildsResponse(
            days=days,
            start_date=lo.isoformat(),
            end_date=hi.isoformat(),
            generated_at=_now_iso(),
            rows=rows,
            stats=_builds_stats(windowed),
        )

    def deploys(
        self,
        days: int,
        cloud: str = "all",
        change: str = "all",
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        force: bool = False,
    ) -> DeploysResponse:
        """The Deploy timeline view: every Cloud Run revision, its change-type, held-for, deployer.

        `change` mirrors the design mock's filter chips exactly: "code" hides the config-only churn
        (same digest redeployed, nothing shipped), "live" is what's serving right now, "fail" is a
        revision that never went ready.
        """
        all_facts = self._all_deploy_facts(force=force)
        lo, hi = _resolve_window(days, start_date, end_date)
        windowed = [f for f in all_facts if _in_window(f.at, lo, hi)]

        rows: list[DeployRow] = []
        for f in windowed:
            if cloud != "all" and f.cloud != cloud:
                continue
            if change == "code" and f.change_type == CHANGE_CONFIG:
                continue
            if change == "live" and not f.live:
                continue
            if change == "fail" and f.change_type != CHANGE_FAILED:
                continue
            rows.append(_deploy_row(f))

        # live_now is a POINT-IN-TIME count over ALL facts, not the windowed subset (see
        # `_deploys_stats`) — a narrow date range must not undercount what's serving right now.
        live_now = sum(1 for f in all_facts if f.live)

        return DeploysResponse(
            days=days,
            start_date=lo.isoformat(),
            end_date=hi.isoformat(),
            generated_at=_now_iso(),
            rows=rows,
            stats=_deploys_stats(windowed, live_now=live_now),
        )
