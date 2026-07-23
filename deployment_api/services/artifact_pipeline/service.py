"""Artifact-pipeline service — the view builders + (coming) the runtime join & drift classifier.

Fetches normalized facts from the providers (each `_safe`-wrapped so one cloud never blanks the
others), caches the compact fact lists briefly, and shapes each of the five API responses. The
headline view's runtime join — a live workload's resolved image digest → Artifact Registry tag →
short SHA → Cloud Build record → git commit → an honest drift verdict — lands with the running
view; this first cut ships the pipeline (builds) view end-to-end.

SSOT: plans/active/artifact_pipeline_observability_2026_07_17.md
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime, timedelta

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.services.artifact_pipeline import providers
from deployment_api.services.artifact_pipeline.cache import ArtifactWindowCache
from deployment_api.services.artifact_pipeline.models import (
    CHANGE_CONFIG,
    CHANGE_FAILED,
    DRIFT_FLOATING,
    DRIFT_HAND,
    DRIFT_OK,
    DRIFT_UNKNOWN,
    LANE_IMAGE,
    SEV_DEFERRED,
    SEV_HIGH,
    SEV_LOW,
    SEV_MED,
    STATE_ACTIVE,
    STATE_EMPTY,
    STATE_LEGACY,
    STATE_PARKED,
    STATE_PAUSED,
    STATE_RUNNING,
    STATE_ZEROED,
    BuildFact,
    BuildRow,
    BuildsResponse,
    BuildsStats,
    BuildStep,
    DeployFact,
    DeployRow,
    DeploysResponse,
    DeploysStats,
    HealthCondition,
    HealthResponse,
    HealthStats,
    ImageRow,
    ImagesResponse,
    ImagesStats,
    RegistryImageFact,
    RunningGroup,
    RunningHost,
    RunningResponse,
    RunningStats,
    RunningVersion,
)

_MAX_DAYS = 366
# A registry repo with no live workload AND nothing pushed in this long is a real GC candidate
# (STATE_LEGACY); more recent-but-idle repos read STATE_ACTIVE ("still pushing, not obviously
# running") rather than a false "stale" verdict.
_LEGACY_AGE_DAYS = 30
# A repo with this many images and no lifecycle/GC policy is a real, measured cost signal for the
# Health view (MEASURED 2026-07-23: market-tick-data-service alone carries 1901).
_SPRAWL_IMAGE_COUNT = 500
_SHA_TAG_RE = re.compile(r"^[0-9a-f]{7,40}$")


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


# ── images (the Artifacts view — a per-repo roll-up of RegistryImageFact) ──────────────────────────
def _image_row(
    cloud: str,
    registry: str,
    repo: str,
    images: list[RegistryImageFact],
    live_digests: dict[str, str],
    now: datetime,
) -> ImageRow:
    newest = max(images, key=lambda i: i.pushed_at)  # "" sorts first — a real timestamp wins if any exist
    sized = [i.size_bytes for i in images if i.size_bytes is not None]
    running_on = next((live_digests[i.digest] for i in images if i.digest and i.digest in live_digests), "")

    if running_on:
        state = STATE_RUNNING
    else:
        stale = not newest.pushed_at  # no measurable push date at all reads as stale, not a false "active"
        if newest.pushed_at:
            try:
                pushed = datetime.fromisoformat(newest.pushed_at.replace("Z", "+00:00"))
                stale = (now - pushed).days >= _LEGACY_AGE_DAYS
            except ValueError:
                stale = False
        state = STATE_LEGACY if stale else STATE_ACTIVE

    return ImageRow(
        repo=repo,
        cloud=cloud,
        registry=registry,
        image_count=len(images),
        tags=newest.tags,
        last_pushed=newest.pushed_at,
        running_on=running_on,
        state=state,
        size_bytes=sum(sized) if sized else None,
    )


def _images_stats(rows: list[ImageRow]) -> ImagesStats:
    return ImagesStats(
        total_repos=len(rows),
        running=sum(1 for r in rows if r.state == STATE_RUNNING),
        parked=sum(1 for r in rows if r.state in (STATE_PAUSED, STATE_ZEROED, STATE_PARKED)),
        legacy=sum(1 for r in rows if r.state == STATE_LEGACY),
        empty=sum(1 for r in rows if r.state == STATE_EMPTY),
    )


# ── running (the headline runtime join: workload digest → AR tag → short SHA → BuildFact) ──────────
def _pick_sha_tag(tags: list[str]) -> str:
    """The first tag on an image that looks like a git SHA (7-40 hex chars), or "" if none does."""
    for t in tags:
        if _SHA_TAG_RE.match(t):
            return t
    return ""


def _running_version(
    deploy: DeployFact,
    images_by_digest: dict[str, RegistryImageFact],
    builds_by_repo_sha: dict[tuple[str, str], BuildFact],
) -> RunningVersion:
    """One live workload's runtime join: digest → the AR image it resolves to → a SHA-shaped tag on
    that image → the BuildFact carrying that (repo, sha) → an honest drift verdict + why.

    Cloud Run's API exposes only the RESOLVED digest, never the tag the operator originally deployed
    with — so this can't distinguish "deployed via an immutable @sha256 pin" from "deployed via a
    :<sha> tag that happened to resolve to this digest, and hasn't moved since". Both read identically
    here, so this collapses them to the single honest `DRIFT_OK` ("traceable to a commit") rather than
    claiming the stronger, unprovable `DRIFT_PINNED`.
    """
    drift: list[str] = []
    why: list[str] = []
    built_from = ""
    artifact = ""
    matched = images_by_digest.get(deploy.digest) if deploy.digest else None

    if not deploy.digest:
        drift.append(DRIFT_UNKNOWN)
        why.append("Cloud Run never resolved a digest for this revision.")
        version = "unknown"
    elif matched is None:
        drift.append(DRIFT_UNKNOWN)
        why.append(
            f"digest {deploy.digest[:19]}… isn't in the current Artifact Registry inventory "
            "(deleted, or never pushed via CI)."
        )
        version = deploy.digest[:19]
    else:
        artifact = f"{matched.registry}/{matched.repo}"
        sha_tag = _pick_sha_tag(matched.tags)
        if sha_tag:
            version = f":{sha_tag}"
            build = builds_by_repo_sha.get((matched.repo, sha_tag[:7]))
            drift.append(DRIFT_OK)
            if build is not None:
                built_from = build.sha
                why.append(
                    f"resolves to {matched.repo}@{build.sha} ({build.branch or 'unknown branch'}), "
                    f"built by {build.trigger or 'CI'}."
                )
            else:
                why.append(f"tagged :{sha_tag} in the registry, but no matching build record in the current window.")
        elif "latest" in matched.tags:
            version = ":latest"
            drift.append(DRIFT_FLOATING)
            why.append(
                "the resolved image is tagged only :latest — no SHA-traceable tag, so a future push could "
                "silently change what this digest means without a new deploy."
            )
        else:
            version = matched.tags[0] if matched.tags else "(untagged)"
            drift.append(DRIFT_UNKNOWN)
            why.append("the resolved image carries no SHA-like or :latest tag this join recognizes.")

    if deploy.deployer and deploy.deployer != "Cloud Build":
        drift.append(DRIFT_HAND)
        why.append(f"deployed by {deploy.deployer}, not the CI pipeline.")

    return RunningVersion(
        version=version,
        artifact=artifact,
        digest=deploy.digest,
        built_from=built_from,
        drift=drift,
        hosts=[RunningHost(name=deploy.workload, kind="Cloud Run svc", launched_at=deploy.at)],
        why=" ".join(why),
    )


# ── health (derived purely from the already-fetched build/deploy/image/running facts) ──────────────
def _health_stats(conditions: list[HealthCondition]) -> HealthStats:
    high = sum(1 for c in conditions if c.severity == SEV_HIGH)
    med = sum(1 for c in conditions if c.severity == SEV_MED)
    low = sum(1 for c in conditions if c.severity == SEV_LOW)
    deferred = sum(1 for c in conditions if c.severity == SEV_DEFERRED)
    return HealthStats(high=high, med=med, low=low, deferred=deferred, real_defects=high + med + low)


class ArtifactPipelineService:
    """Serves the /ops/artifacts views from the (cheaply cached) normalized provider facts."""

    def __init__(self) -> None:
        self._cfg = DeploymentApiConfig()
        self._build_facts: ArtifactWindowCache[list[BuildFact]] = ArtifactWindowCache()
        self._deploy_facts: ArtifactWindowCache[list[DeployFact]] = ArtifactWindowCache()
        self._image_facts: ArtifactWindowCache[list[RegistryImageFact]] = ArtifactWindowCache()

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

    def _all_image_facts(self, *, force: bool = False) -> list[RegistryImageFact]:
        """Every pushed image in the canonical AR registry, briefly cached. `_safe` per source."""

        def _load() -> list[RegistryImageFact]:
            facts: list[RegistryImageFact] = []
            facts += providers.safe(
                lambda: providers.gcp_artifact_registry_images(self._cfg), "gcp_artifact_registry_images"
            )
            # AWS ECR is a later increment (parked/no credits today) — `_safe`, mirroring builds/deploys.
            return facts

        return self._image_facts.get_or_load("images", _load, force=force)

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

    def images(self, cloud: str = "all", force: bool = False) -> ImagesResponse:
        """The Artifacts view: every registry repo, rolled up from its individual pushed images."""
        facts = self._all_image_facts(force=force)
        deploys = self._all_deploy_facts(force=force)
        live_digests = {f.digest: f.workload for f in deploys if f.live and f.digest}

        groups: dict[tuple[str, str, str], list[RegistryImageFact]] = {}
        for f in facts:
            if cloud != "all" and f.cloud != cloud:
                continue
            groups.setdefault((f.cloud, f.registry, f.repo), []).append(f)

        now = datetime.now(UTC)
        rows = [_image_row(c, registry, repo, imgs, live_digests, now) for (c, registry, repo), imgs in groups.items()]
        rows.sort(key=lambda r: r.repo)

        return ImagesResponse(generated_at=_now_iso(), rows=rows, stats=_images_stats(rows))

    def running(self, force: bool = False) -> RunningResponse:
        """The headline view: every live GCP workload, its runtime-joined version, and a drift verdict.

        Scoped to the Cloud Run (image) lane this pass — the VM tarball lane's `git_commit` is
        honestly `""` on every live host until the launch-time stamp change lands (see the plan's
        "Honest gaps"), so it isn't fabricated here; the Health view surfaces that gap explicitly.
        Cloud Run traffic-split (>1 revision serving at once) isn't detected either — this join tracks
        only the single revision `list_cloud_run_services` reports as live, so `fragmented` stays 0.
        """
        deploys = self._all_deploy_facts(force=force)
        images = self._all_image_facts(force=force)
        builds = self._all_build_facts(force=force)

        images_by_digest = {f.digest: f for f in images if f.digest}
        builds_by_repo_sha = {(b.repo, b.sha): b for b in builds if b.sha}

        groups = [
            RunningGroup(
                service=d.workload,
                lane=LANE_IMAGE,
                cloud=d.cloud,
                fragmented=False,
                versions=[_running_version(d, images_by_digest, builds_by_repo_sha)],
            )
            for d in deploys
            if d.live
        ]
        groups.sort(key=lambda g: g.service)

        all_versions = [v for g in groups for v in g.versions]
        stats = RunningStats(
            services=len(groups),
            versions=len(all_versions),
            fragmented=0,
            floating=sum(1 for v in all_versions if DRIFT_FLOATING in v.drift),
            hand=sum(1 for v in all_versions if DRIFT_HAND in v.drift),
            unknown=sum(1 for v in all_versions if DRIFT_UNKNOWN in v.drift),
        )
        return RunningResponse(generated_at=_now_iso(), groups=groups, stats=stats)

    def health(self, force: bool = False) -> HealthResponse:
        """Measured pipeline conditions, severity-ranked — every one derived from the same facts the
        other views already fetched (no new cloud calls), never a hand-written / fabricated entry."""
        builds = self._all_build_facts(force=force)
        deploys = self._all_deploy_facts(force=force)
        images_resp = self.images(force=force)
        running_resp = self.running(force=force)

        conditions: list[HealthCondition] = [
            HealthCondition(
                condition="AWS builds/deploys/registry are not read yet",
                severity=SEV_DEFERRED,
                count="all AWS",
                area="cross-cutting · AWS",
                tab="pipe",
                meaning="The AWS estate is deliberately stopped while credits are unavailable — parked, not broken.",
                evidence="AWS CodeBuild/App Runner/ECS/ECR providers are not yet wired into this page.",
            )
        ]

        live_failed = [d for d in deploys if d.live and d.change_type == CHANGE_FAILED]
        if live_failed:
            conditions.append(
                HealthCondition(
                    condition="A workload is serving its newest revision even though that revision never went ready",
                    severity=SEV_HIGH,
                    count=str(len(live_failed)),
                    area="deploy · GCP",
                    tab="deploy",
                    meaning="Cloud Run has nothing newer to fall back to, so a broken deploy is still what's live.",
                    evidence=", ".join(sorted({d.workload for d in live_failed})),
                )
            )

        lo7, hi7 = _resolve_window(7, None, None)
        recent_failed = [b for b in builds if b.status == "FAILURE" and _in_window(b.started_at, lo7, hi7)]
        if recent_failed:
            conditions.append(
                HealthCondition(
                    condition="Builds failed in the last 7 days",
                    severity=SEV_MED,
                    count=str(len(recent_failed)),
                    area="pipeline · CI",
                    tab="pipe",
                    meaning="Each failure blocked that commit from reaching a registry image.",
                    evidence=", ".join(sorted({b.repo for b in recent_failed})),
                )
            )

        recent_counts = _sha_build_counts([b for b in builds if _in_window(b.started_at, lo7, hi7)])
        wasted = sum(c - 1 for c in recent_counts.values() if c > 1)
        if wasted:
            conditions.append(
                HealthCondition(
                    condition="A commit was built more than once in the last 7 days",
                    severity=SEV_LOW,
                    count=str(wasted),
                    area="pipeline · CI",
                    tab="pipe",
                    meaning="Wasted compute — the second build produces an identical artifact to the first.",
                    evidence="see the Pipeline tab's 'dup' badge",
                )
            )

        all_versions = [(g.service, v) for g in running_resp.groups for v in g.versions]
        floating = [svc for svc, v in all_versions if DRIFT_FLOATING in v.drift]
        if floating:
            conditions.append(
                HealthCondition(
                    condition="A live workload resolves to an image tagged only :latest",
                    severity=SEV_MED,
                    count=str(len(floating)),
                    area="running · GCP",
                    tab="running",
                    meaning=(
                        "No SHA-traceable tag — a future push could silently change what this digest means "
                        "without a new deploy."
                    ),
                    evidence=", ".join(sorted(floating)),
                )
            )

        hand = [svc for svc, v in all_versions if DRIFT_HAND in v.drift]
        if hand:
            conditions.append(
                HealthCondition(
                    condition="A live workload was deployed by something other than the CI pipeline",
                    severity=SEV_MED,
                    count=str(len(hand)),
                    area="running · GCP",
                    tab="running",
                    meaning="Bypasses the build record this page's provenance chain relies on.",
                    evidence=", ".join(sorted(hand)),
                )
            )

        conditions.append(
            HealthCondition(
                condition="VM tarball-lane workloads carry no measured git commit yet",
                severity=SEV_MED,
                count="fleet",
                area="running · GCP tarball lane",
                tab="running",
                meaning=(
                    'The GCE VM registry entry\'s git_commit/image_digest fields are stamped "" at launch today, '
                    "so What's running can only cover the Cloud Run (image) lane until the launch-time stamp "
                    "change lands."
                ),
                evidence="plans/active/artifact_pipeline_observability_2026_07_17.md § Honest gaps",
            )
        )

        sprawl = [r for r in images_resp.rows if r.image_count >= _SPRAWL_IMAGE_COUNT]
        if sprawl:
            top = max(sprawl, key=lambda r: r.image_count)
            conditions.append(
                HealthCondition(
                    condition="A registry repo has accumulated hundreds of images with no lifecycle/GC policy",
                    severity=SEV_LOW,
                    count=str(top.image_count),
                    area="artifacts · GCP AR",
                    tab="art",
                    meaning=(
                        f"{top.repo} alone carries {top.image_count} images — "
                        "storage cost keeps climbing with no expiry."
                    ),
                    evidence=f"repo={top.repo}",
                )
            )

        return HealthResponse(generated_at=_now_iso(), conditions=conditions, stats=_health_stats(conditions))
