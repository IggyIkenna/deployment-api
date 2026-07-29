"""Unit tests for the artifact-pipeline service + route (the /ops/artifacts backend).

Mocks the provider at the module seam (`providers.gcp_cloud_builds` / `gcp_cloud_run_revisions`) so
nothing touches a cloud — `--block-network` safe. Covers the pure helpers, the builds + deploys
views' window filtering / stats / filters, the Cloud Run revision classifier, and the route's loud
400 range gate.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest
from fastapi import HTTPException

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.routes.artifacts import _resolve_range
from deployment_api.services.artifact_pipeline import ArtifactPipelineService
from deployment_api.services.artifact_pipeline import providers as providers_mod
from deployment_api.services.artifact_pipeline import service as service_mod
from deployment_api.services.artifact_pipeline.models import (
    CHANGE_CONFIG,
    CHANGE_FAILED,
    CHANGE_NEW,
    CHANGE_ROLLBACK,
    DRIFT_FLOATING,
    DRIFT_HAND,
    DRIFT_OK,
    DRIFT_UNKNOWN,
    LANE_IMAGE,
    LANE_TARBALL,
    REGISTRY_TARBALL_BUCKET,
    SEV_DEFERRED,
    SEV_HIGH,
    SEV_LOW,
    SEV_MED,
    STATE_LEGACY,
    STATE_RUNNING,
    BuildFact,
    DeployFact,
    RegistryImageFact,
)
from deployment_api.services.artifact_pipeline.service import (
    _fmt_duration,
    _in_window,
    _median,
    _one_line_failure,
    _resolve_window,
)


# ── pure helpers ────────────────────────────────────────────────────────────────────────────────
def test_resolve_window_explicit_range_sorted_and_clamped() -> None:
    # in order → returned as-is
    assert _resolve_window(30, date(2026, 7, 1), date(2026, 7, 31)) == (date(2026, 7, 1), date(2026, 7, 31))
    # inverted → swapped, not rejected (the route is the loud gate; the service is defensive)
    assert _resolve_window(30, date(2026, 7, 31), date(2026, 7, 1)) == (date(2026, 7, 1), date(2026, 7, 31))
    # over-long → clamped to _MAX_DAYS ending at `hi`
    lo, hi = _resolve_window(30, date(2020, 1, 1), date(2026, 7, 31))
    assert hi == date(2026, 7, 31)
    assert (hi - lo).days + 1 == service_mod._MAX_DAYS


def test_fmt_duration() -> None:
    assert _fmt_duration(None) == ""
    assert _fmt_duration(41) == "41s"
    assert _fmt_duration(221) == "3m41s"
    assert _fmt_duration(60) == "1m00s"


def test_in_window() -> None:
    lo, hi = date(2026, 7, 1), date(2026, 7, 31)
    assert _in_window("2026-07-15T14:08:30+00:00", lo, hi) is True
    assert _in_window("2026-07-15T14:08:30Z", lo, hi) is True  # Z is normalised
    assert _in_window("2026-06-01T00:00:00+00:00", lo, hi) is False
    assert _in_window("", lo, hi) is False
    assert _in_window("not-a-date", lo, hi) is False


def test_median() -> None:
    assert _median([]) is None
    assert _median([5.0]) == 5.0
    assert _median([203.0, 222.0, 228.0]) == 222.0
    assert _median([1.0, 3.0]) == 2.0


def test_one_line_failure_prefers_detail_then_type_then_step() -> None:
    ok = BuildFact(
        cloud="gcp",
        lane=LANE_IMAGE,
        repo="x",
        build_id="1",
        status="SUCCESS",
        trigger="",
        sha="a",
        branch="",
        started_at="",
    )
    assert _one_line_failure(ok) == ""
    with_detail = BuildFact(
        cloud="gcp",
        lane=LANE_IMAGE,
        repo="x",
        build_id="1",
        status="FAILURE",
        trigger="",
        sha="a",
        branch="",
        started_at="",
        failure_detail="docker exited 1",
        failure_type="USER_BUILD_STEP",
    )
    assert _one_line_failure(with_detail) == "docker exited 1"
    type_only = BuildFact(
        cloud="gcp",
        lane=LANE_IMAGE,
        repo="x",
        build_id="1",
        status="FAILURE",
        trigger="",
        sha="a",
        branch="",
        started_at="",
        failure_type="TIMEOUT",
    )
    assert _one_line_failure(type_only) == "TIMEOUT"
    step_only = BuildFact(
        cloud="gcp",
        lane=LANE_IMAGE,
        repo="x",
        build_id="1",
        status="FAILURE",
        trigger="",
        sha="a",
        branch="",
        started_at="",
        steps=[("lint", "SUCCESS", 3.0), ("docker-build", "FAILURE", 41.0)],
    )
    assert _one_line_failure(step_only) == 'step "docker-build" failed'


# ── builds view ─────────────────────────────────────────────────────────────────────────────────
def _fact(
    repo: str, sha: str, status: str, started: str, *, lane: str = LANE_IMAGE, dur: float | None = None
) -> BuildFact:
    return BuildFact(
        cloud="gcp",
        lane=lane,
        repo=repo,
        build_id=f"{repo}-{sha}-{started}",
        status=status,
        trigger=f"{repo}-build",
        sha=sha,
        branch="main",
        started_at=started,
        duration_sec=dur,
    )


def _svc_with(monkeypatch: pytest.MonkeyPatch, facts: list[BuildFact]) -> ArtifactPipelineService:
    monkeypatch.setattr(providers_mod, "gcp_cloud_builds", lambda _cfg, scan=400: list(facts))
    return ArtifactPipelineService()


def test_builds_windowing_and_stats(monkeypatch: pytest.MonkeyPatch) -> None:
    facts = [
        _fact("deployment-api", "e5d7ef1", "FAILURE", "2026-07-15T14:08:30+00:00", dur=228),
        _fact("unified-api-contracts", "d4cbe36", "SUCCESS", "2026-07-16T14:09:10+00:00", dur=203),
        _fact("deployment-service", "f000ee3", "SUCCESS", "2026-07-17T14:08:01+00:00", dur=222),
        _fact("deployment-service", "f000ee3", "SUCCESS", "2026-07-17T14:08:01+00:00", dur=None),  # dup of prev
        _fact("old-service", "0000000", "SUCCESS", "2026-06-01T00:00:00+00:00", dur=100),  # out of window
    ]
    svc = _svc_with(monkeypatch, facts)
    resp = svc.builds(31, start_date=date(2026, 7, 1), end_date=date(2026, 7, 31))

    assert resp.start_date == "2026-07-01"
    assert resp.end_date == "2026-07-31"
    assert len(resp.rows) == 4  # the out-of-window row is excluded
    assert resp.stats.total == 4
    assert resp.stats.failed == 1
    assert resp.stats.success_rate == 75.0
    assert resp.stats.median_duration_sec == 222.0  # [203, 222, 228]; the None-duration dup is excluded
    assert resp.stats.wasted_dup == 1  # the two f000ee3 builds count as one wasted

    dup_rows = [r for r in resp.rows if r.sha == "f000ee3"]
    assert len(dup_rows) == 2
    assert all(r.dup for r in dup_rows)
    fail_row = next(r for r in resp.rows if r.status == "FAILURE")
    assert fail_row.failure  # a one-line reason is present


def test_builds_cross_lane_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    facts = [
        _fact("features", "abc1234", "SUCCESS", "2026-07-10T00:00:00+00:00", lane=LANE_IMAGE),
        _fact("features", "abc1234", "SUCCESS", "2026-07-10T00:05:00+00:00", lane=LANE_TARBALL),
        _fact("solo", "def5678", "SUCCESS", "2026-07-10T00:00:00+00:00", lane=LANE_IMAGE),
    ]
    svc = _svc_with(monkeypatch, facts)
    resp = svc.builds(31, start_date=date(2026, 7, 1), end_date=date(2026, 7, 31))
    cross = {r.sha: r.cross_lane for r in resp.rows}
    assert cross["abc1234"] is True  # built as image AND tarball
    assert cross["def5678"] is False


def test_builds_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    facts = [
        _fact("a", "111", "SUCCESS", "2026-07-10T00:00:00+00:00", lane=LANE_IMAGE),
        _fact("b", "222", "FAILURE", "2026-07-11T00:00:00+00:00", lane=LANE_IMAGE),
        _fact("c", "333", "SUCCESS", "2026-07-12T00:00:00+00:00", lane=LANE_TARBALL),
    ]
    svc = _svc_with(monkeypatch, facts)
    failed_only = svc.builds(31, status="failed", start_date=date(2026, 7, 1), end_date=date(2026, 7, 31))
    assert [r.sha for r in failed_only.rows] == ["222"]
    # stats are computed over the whole window, not the filtered subset
    assert failed_only.stats.total == 3

    tarball_only = svc.builds(31, lane="tarball", start_date=date(2026, 7, 1), end_date=date(2026, 7, 31))
    assert [r.sha for r in tarball_only.rows] == ["333"]


def test_builds_provider_failure_degrades_to_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(_cfg: object, scan: int = 400) -> list[BuildFact]:
        raise RuntimeError("cloud build API down")

    monkeypatch.setattr(providers_mod, "gcp_cloud_builds", _boom)
    svc = ArtifactPipelineService()
    resp = svc.builds(30)  # `safe` swallows the failure → empty, honest, never a 5xx
    assert resp.rows == []
    assert resp.stats.total == 0


# ── Phase 3d: the GCS tarball-manifest bucket folded into builds()/images() ────────────────────────
def test_builds_includes_tarball_lane_and_cross_lane_matches_image_lane(monkeypatch: pytest.MonkeyPatch) -> None:
    image_facts = [_fact("features", "abc1234", "SUCCESS", "2026-07-10T00:00:00+00:00", lane=LANE_IMAGE)]
    tarball_facts = [
        BuildFact(
            cloud="gcp",
            lane=LANE_TARBALL,
            repo="features",
            build_id="features-code@abc1234def",
            status="SUCCESS",
            trigger="code-tarball-refresh",
            sha="abc1234",
            branch="live-defi-rollout",
            started_at="2026-07-10T00:05:00+00:00",
            produced="gs://deployment-scripts-test-project/code/features-code@abc1234def.tar.gz",
        )
    ]
    monkeypatch.setattr(providers_mod, "gcp_cloud_builds", lambda _cfg, scan=400: list(image_facts))
    monkeypatch.setattr(providers_mod, "gcp_tarball_manifest_builds", lambda _cfg, scan=20000: list(tarball_facts))
    svc = ArtifactPipelineService()
    resp = svc.builds(31, start_date=date(2026, 7, 1), end_date=date(2026, 7, 31))

    tarball_row = next(r for r in resp.rows if r.lane == LANE_TARBALL)
    assert tarball_row.repo == "features"
    assert tarball_row.status == "SUCCESS"
    assert tarball_row.produced.startswith("gs://deployment-scripts-test-project/code/")
    # the tarball build shares (repo, sha) with the image-lane build → the ⇄ both-lanes badge fires
    assert tarball_row.cross_lane is True
    image_row = next(r for r in resp.rows if r.lane == LANE_IMAGE)
    assert image_row.cross_lane is True


def test_builds_tarball_lane_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    image_facts = [_fact("a", "111", "SUCCESS", "2026-07-10T00:00:00+00:00", lane=LANE_IMAGE)]
    tarball_facts = [
        BuildFact(
            cloud="gcp",
            lane=LANE_TARBALL,
            repo="b",
            build_id="b-code",
            status="SUCCESS",
            trigger="code-tarball-refresh",
            sha="222",
            branch="live-defi-rollout",
            started_at="2026-07-11T00:00:00+00:00",
        )
    ]
    monkeypatch.setattr(providers_mod, "gcp_cloud_builds", lambda _cfg, scan=400: list(image_facts))
    monkeypatch.setattr(providers_mod, "gcp_tarball_manifest_builds", lambda _cfg, scan=20000: list(tarball_facts))
    svc = ArtifactPipelineService()
    tarball_only = svc.builds(31, lane="tarball", start_date=date(2026, 7, 1), end_date=date(2026, 7, 31))
    assert [r.repo for r in tarball_only.rows] == ["b"]


def test_builds_tarball_provider_failure_degrades_to_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(_cfg: object, scan: int = 20000) -> list[BuildFact]:
        raise RuntimeError("GCS list_blobs down")

    monkeypatch.setattr(providers_mod, "gcp_cloud_builds", lambda _cfg, scan=400: [])
    monkeypatch.setattr(providers_mod, "gcp_tarball_manifest_builds", _boom)
    svc = ArtifactPipelineService()
    resp = svc.builds(30)  # the tarball provider's failure is isolated — never blanks the image lane
    assert resp.rows == []
    assert resp.stats.total == 0


# ── deploys view ────────────────────────────────────────────────────────────────────────────────
def _deploy_fact(
    workload: str,
    revision: str,
    change_type: str,
    at: str,
    *,
    live: bool = False,
    digest: str = "sha256:abc",
    held_for: str = "",
) -> DeployFact:
    return DeployFact(
        cloud="gcp",
        workload=workload,
        revision=revision,
        digest=digest,
        built_from="",
        resolvable=False,
        change_type=change_type,
        at=at,
        held_for=held_for,
        live=live,
        deployer="Cloud Build",
        link_kind="revision",
    )


def _deploy_svc_with(monkeypatch: pytest.MonkeyPatch, facts: list[DeployFact]) -> ArtifactPipelineService:
    monkeypatch.setattr(providers_mod, "gcp_cloud_run_revisions", lambda _cfg: list(facts))
    return ArtifactPipelineService()


def test_deploys_windowing_and_stats(monkeypatch: pytest.MonkeyPatch) -> None:
    facts = [
        _deploy_fact("svc-a", "svc-a-00001", CHANGE_NEW, "2026-07-10T00:00:00+00:00", held_for="2h"),
        _deploy_fact("svc-a", "svc-a-00002", CHANGE_CONFIG, "2026-07-11T00:00:00+00:00", held_for="1h"),
        _deploy_fact("svc-a", "svc-a-00003", CHANGE_NEW, "2026-07-12T00:00:00+00:00", live=True),
        _deploy_fact("svc-b", "svc-b-00001", CHANGE_FAILED, "2026-07-12T00:00:00+00:00"),
        _deploy_fact("svc-old", "svc-old-00001", CHANGE_NEW, "2026-06-01T00:00:00+00:00", live=True),  # out of window
    ]
    svc = _deploy_svc_with(monkeypatch, facts)
    resp = svc.deploys(31, start_date=date(2026, 7, 1), end_date=date(2026, 7, 31))

    assert len(resp.rows) == 4  # the out-of-window row is excluded
    assert resp.stats.total == 4
    assert resp.stats.failed == 1
    assert resp.stats.config_only_pct == 25.0  # 1 of 4 windowed rows is config-only
    # live_now is a POINT-IN-TIME count over ALL facts (incl. the out-of-window one), never windowed
    assert resp.stats.live_now == 2


def test_deploys_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    facts = [
        _deploy_fact("svc-a", "r1", CHANGE_NEW, "2026-07-10T00:00:00+00:00"),
        _deploy_fact("svc-a", "r2", CHANGE_CONFIG, "2026-07-11T00:00:00+00:00"),
        _deploy_fact("svc-a", "r3", CHANGE_NEW, "2026-07-12T00:00:00+00:00", live=True),
        _deploy_fact("svc-b", "r4", CHANGE_FAILED, "2026-07-12T00:00:00+00:00"),
    ]
    svc = _deploy_svc_with(monkeypatch, facts)
    window = {"start_date": date(2026, 7, 1), "end_date": date(2026, 7, 31)}

    code_only = svc.deploys(31, change="code", **window)
    assert [r.revision for r in code_only.rows] == ["r1", "r3", "r4"]  # config-only (r2) hidden

    live_only = svc.deploys(31, change="live", **window)
    assert [r.revision for r in live_only.rows] == ["r3"]

    fail_only = svc.deploys(31, change="fail", **window)
    assert [r.revision for r in fail_only.rows] == ["r4"]

    # stats are computed over the whole (change-unfiltered) window, not the filtered subset
    assert fail_only.stats.total == 4


def test_deploys_provider_failure_degrades_to_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(_cfg: object) -> list[DeployFact]:
        raise RuntimeError("cloud run API down")

    monkeypatch.setattr(providers_mod, "gcp_cloud_run_revisions", _boom)
    svc = ArtifactPipelineService()
    resp = svc.deploys(30)  # `safe` swallows the failure → empty, honest, never a 5xx
    assert resp.rows == []
    assert resp.stats.total == 0
    assert resp.stats.live_now == 0


# ── Cloud Run revisions provider — classification + the RepeatedComposite fix ─────────────────────
class _FakeContainer:
    def __init__(self, image: str) -> None:
        self.image = image


class _FakeConditionState:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeCondition:
    def __init__(self, type_: str, state_name: str) -> None:
        self.type_ = type_
        self.state = _FakeConditionState(state_name)


class _FakeRevision:
    """A minimal stand-in for a `run_v2.Revision` — exercises the getattr-defensive extraction
    without needing the real protobuf types. Iterables are plain Python lists (NOT `RepeatedComposite`)
    on purpose — see `test_as_item_list_handles_non_list_sequence` for the proto-shape regression."""

    def __init__(
        self,
        name: str,
        image: str,
        *,
        ready: bool = True,
        created: datetime | None = None,
        creator: str = "",
    ) -> None:
        self.name = name
        self.containers = [_FakeContainer(image)] if image else []
        self.conditions = [_FakeCondition("Ready", "CONDITION_SUCCEEDED" if ready else "CONDITION_FAILED")]
        self.create_time = created
        self.creator = creator


def test_as_item_list_handles_non_list_sequence() -> None:
    """The regression: a protobuf `RepeatedComposite` is a Sequence but NOT a list/tuple — an
    `isinstance(x, (list, tuple))` gate silently drops it (measured live 2026-07-23: Build.steps,
    Build.images, and Revision.containers/conditions all fail that check)."""

    class _ProtoLikeSequence:
        """Iterable + indexable + has __len__, like `proto.marshal.collections.repeated.Repeated` —
        deliberately NOT a list/tuple subclass."""

        def __init__(self, items: list[object]) -> None:
            self._items = items

        def __iter__(self):
            return iter(self._items)

        def __len__(self) -> int:
            return len(self._items)

    assert providers_mod._as_item_list(None) == []
    assert providers_mod._as_item_list("not-a-sequence") == []
    assert providers_mod._as_item_list([1, 2]) == [1, 2]
    assert providers_mod._as_item_list(_ProtoLikeSequence([1, 2, 3])) == [1, 2, 3]


def test_digest_from_image() -> None:
    pinned = "asia-northeast1-docker.pkg.dev/p/r/svc@sha256:abc123"
    assert providers_mod._digest_from_image(pinned) == "sha256:abc123"
    assert providers_mod._digest_from_image("asia-northeast1-docker.pkg.dev/p/r/svc:latest") == ""
    assert providers_mod._digest_from_image("") == ""


def test_format_deployer() -> None:
    assert providers_mod._format_deployer("") == ""
    assert providers_mod._format_deployer("1060025368044@cloudbuild.gserviceaccount.com") == "Cloud Build"
    assert providers_mod._format_deployer("unified-trading-sa@p.iam.gserviceaccount.com") == "unified-trading-sa"
    assert providers_mod._format_deployer("someone@example.com") == "someone@example.com"


def test_revision_ready_reads_the_ready_condition() -> None:
    ready_rev = _FakeRevision("r1", "img@sha256:x", ready=True)
    failed_rev = _FakeRevision("r2", "img@sha256:x", ready=False)
    assert providers_mod._revision_ready(ready_rev) is True
    assert providers_mod._revision_ready(failed_rev) is False


def test_classify_service_revisions_new_config_rollback_failed() -> None:
    t0 = datetime(2026, 7, 20, 0, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 7, 20, 1, 0, 0, tzinfo=UTC)  # +1h
    t2 = datetime(2026, 7, 20, 3, 0, 0, tzinfo=UTC)  # +2h
    t3 = datetime(2026, 7, 20, 3, 30, 0, tzinfo=UTC)  # +30m
    t4 = datetime(2026, 7, 20, 4, 0, 0, tzinfo=UTC)  # +30m

    revisions_newest_first = [
        _FakeRevision("svc-00005", "img@sha256:BROKEN", ready=False, created=t4, creator="human@example.com"),
        _FakeRevision("svc-00004", "img@sha256:AAA", ready=True, created=t3, creator="Cloud Build"),  # rollback→AAA
        _FakeRevision("svc-00003", "img@sha256:BBB", ready=True, created=t2, creator="Cloud Build"),  # new
        _FakeRevision("svc-00002", "img@sha256:AAA", ready=True, created=t1, creator="Cloud Build"),  # config (== t0)
        _FakeRevision("svc-00001", "img@sha256:AAA", ready=True, created=t0, creator="Cloud Build"),  # first → new
    ]

    facts = providers_mod._classify_service_revisions("svc", revisions_newest_first, live_revision="svc-00005")
    by_name = {f.revision: f for f in facts}

    assert by_name["svc-00001"].change_type == CHANGE_NEW  # first revision ever seen
    assert by_name["svc-00002"].change_type == CHANGE_CONFIG  # same digest as 00001 — nothing shipped
    assert by_name["svc-00003"].change_type == CHANGE_NEW  # a genuinely new digest
    assert by_name["svc-00004"].change_type == CHANGE_ROLLBACK  # reverted to AAA, seen at 00001/00002
    assert by_name["svc-00005"].change_type == CHANGE_FAILED  # never went ready

    # held_for looks ONE STEP AHEAD to the successor; the newest revision has none yet.
    assert by_name["svc-00001"].held_for == "1h00m"
    assert by_name["svc-00002"].held_for == "2h00m"
    assert by_name["svc-00003"].held_for == "30m00s"
    assert by_name["svc-00004"].held_for == "30m00s"
    assert by_name["svc-00005"].held_for == ""

    # `live` matches the passed-in live_revision, independent of change_type — a failed revision
    # can still be "what Cloud Run currently has", per the `deployment-service` finding.
    assert by_name["svc-00005"].live is True
    assert by_name["svc-00001"].live is False

    assert by_name["svc-00005"].deployer == "human@example.com"
    assert by_name["svc-00001"].deployer == "Cloud Build"


# ── route range gate ──────────────────────────────────────────────────────────────────────────────
def test_resolve_range_none() -> None:
    assert _resolve_range(None, None) == (None, None)


def test_resolve_range_incomplete_rejected() -> None:
    with pytest.raises(HTTPException) as exc:
        _resolve_range(date(2026, 7, 1), None)
    assert exc.value.status_code == 400


def test_resolve_range_inverted_rejected() -> None:
    with pytest.raises(HTTPException) as exc:
        _resolve_range(date(2026, 7, 31), date(2026, 7, 1))
    assert exc.value.status_code == 400


def test_resolve_range_too_long_rejected() -> None:
    with pytest.raises(HTTPException) as exc:
        _resolve_range(date(2020, 1, 1), date(2026, 7, 31))
    assert exc.value.status_code == 400


# ── images view (per-repo roll-up of RegistryImageFact) ────────────────────────────────────────────
def _reg_image(repo: str, digest: str, tags: list[str], pushed: str, *, size: int | None = 1000) -> RegistryImageFact:
    return RegistryImageFact(
        cloud="gcp",
        registry="unified-trading-system",
        repo=repo,
        digest=digest,
        tags=tags,
        pushed_at=pushed,
        size_bytes=size,
    )


def _image_svc_with(
    monkeypatch: pytest.MonkeyPatch,
    image_facts: list[RegistryImageFact],
    deploy_facts: list[DeployFact] | None = None,
) -> ArtifactPipelineService:
    monkeypatch.setattr(providers_mod, "gcp_artifact_registry_images", lambda _cfg, scan=5000: list(image_facts))
    monkeypatch.setattr(providers_mod, "gcp_cloud_run_revisions", lambda _cfg: list(deploy_facts or []))
    return ArtifactPipelineService()


def test_images_aggregates_per_repo_and_flags_running(monkeypatch: pytest.MonkeyPatch) -> None:
    recent = "2026-07-20T00:00:00+00:00"
    old = "2026-01-01T00:00:00+00:00"
    facts = [
        _reg_image("deployment-api", "sha256:AAA", ["abc1234"], recent, size=1000),
        _reg_image("deployment-api", "sha256:BBB", ["def5678"], old, size=2000),
        _reg_image("legacy-svc", "sha256:CCC", ["9999999"], old, size=500),
    ]
    live = [_deploy_fact("uts-shared-deployment-api", "r1", CHANGE_NEW, recent, live=True, digest="sha256:AAA")]
    svc = _image_svc_with(monkeypatch, facts, live)
    resp = svc.images()

    rows = {r.repo: r for r in resp.rows}
    assert rows["deployment-api"].image_count == 2
    assert rows["deployment-api"].running_on == "uts-shared-deployment-api"
    assert rows["deployment-api"].state == STATE_RUNNING
    assert rows["deployment-api"].size_bytes == 3000  # both images' bytes, summed
    assert rows["legacy-svc"].state == STATE_LEGACY  # no live workload, nothing pushed in >30d
    assert resp.stats.total_repos == 2
    assert resp.stats.running == 1
    assert resp.stats.legacy == 1


def test_images_tarball_lane_rows_use_a_distinct_registry_key(monkeypatch: pytest.MonkeyPatch) -> None:
    ar_facts = [_reg_image("deployment-api", "sha256:AAA", ["abc1234"], "2026-07-20T00:00:00+00:00")]
    tarball_facts = [
        RegistryImageFact(
            cloud="gcp",
            registry=REGISTRY_TARBALL_BUCKET,
            repo="deployment-api",  # SAME repo name as the AR row above — must not collide with it
            digest="",
            tags=["abc1234"],
            pushed_at="2026-07-20T00:05:00+00:00",
            size_bytes=123,
        ),
        RegistryImageFact(
            cloud="gcp",
            registry=REGISTRY_TARBALL_BUCKET,
            repo="alerting-service",
            digest="",
            tags=["floating"],
            pushed_at="2026-01-01T00:00:00+00:00",  # stale — no live cross-ref exists for tarballs yet
            size_bytes=456,
        ),
    ]
    monkeypatch.setattr(providers_mod, "gcp_artifact_registry_images", lambda _cfg, scan=5000: list(ar_facts))
    monkeypatch.setattr(providers_mod, "gcp_cloud_run_revisions", lambda _cfg: [])
    monkeypatch.setattr(providers_mod, "gcp_tarball_manifest_images", lambda _cfg, scan=20000: list(tarball_facts))
    svc = ArtifactPipelineService()
    resp = svc.images()

    ar_row = next(r for r in resp.rows if r.registry != REGISTRY_TARBALL_BUCKET and r.repo == "deployment-api")
    tarball_row = next(r for r in resp.rows if r.registry == REGISTRY_TARBALL_BUCKET and r.repo == "deployment-api")
    assert ar_row is not tarball_row  # two distinct rows, not merged, despite the shared repo name
    assert tarball_row.running_on == ""  # honest absence — no digest to cross-ref a live workload against
    assert resp.stats.total_repos == 3  # 2 tarball repos + 1 AR repo, none collided


def test_images_tarball_provider_failure_degrades_to_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(_cfg: object, scan: int = 20000) -> list[RegistryImageFact]:
        raise RuntimeError("GCS list_blobs down")

    monkeypatch.setattr(providers_mod, "gcp_artifact_registry_images", lambda _cfg, scan=5000: [])
    monkeypatch.setattr(providers_mod, "gcp_cloud_run_revisions", lambda _cfg: [])
    monkeypatch.setattr(providers_mod, "gcp_tarball_manifest_images", _boom)
    svc = ArtifactPipelineService()
    resp = svc.images()  # the tarball provider's failure is isolated — never blanks the AR lane
    assert resp.rows == []
    assert resp.stats.total_repos == 0


def test_images_provider_failure_degrades_to_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(_cfg: object, scan: int = 5000) -> list[RegistryImageFact]:
        raise RuntimeError("AR API down")

    monkeypatch.setattr(providers_mod, "gcp_artifact_registry_images", _boom)
    monkeypatch.setattr(providers_mod, "gcp_cloud_run_revisions", lambda _cfg: [])
    svc = ArtifactPipelineService()
    resp = svc.images()
    assert resp.rows == []
    assert resp.stats.total_repos == 0


# ── running view (the digest → AR tag → short SHA → build runtime join) ────────────────────────────
def _running_svc_with(
    monkeypatch: pytest.MonkeyPatch,
    *,
    deploys: list[DeployFact],
    images: list[RegistryImageFact] | None = None,
    builds: list[BuildFact] | None = None,
) -> ArtifactPipelineService:
    monkeypatch.setattr(providers_mod, "gcp_cloud_run_revisions", lambda _cfg: list(deploys))
    monkeypatch.setattr(providers_mod, "gcp_artifact_registry_images", lambda _cfg, scan=5000: list(images or []))
    monkeypatch.setattr(providers_mod, "gcp_cloud_builds", lambda _cfg, scan=400: list(builds or []))
    return ArtifactPipelineService()


def test_running_ok_when_sha_tag_resolves_to_a_build(monkeypatch: pytest.MonkeyPatch) -> None:
    deploys = [
        _deploy_fact(
            "uts-shared-deployment-api", "r1", CHANGE_NEW, "2026-07-21T00:00:00+00:00", live=True, digest="sha256:AAA"
        )
    ]
    images = [_reg_image("deployment-api", "sha256:AAA", ["a557471", "0.10.0"], "2026-07-21T00:00:00+00:00")]
    builds = [_fact("deployment-api", "a557471", "SUCCESS", "2026-07-21T00:00:00+00:00")]
    svc = _running_svc_with(monkeypatch, deploys=deploys, images=images, builds=builds)
    resp = svc.running()

    assert resp.stats.services == 1
    group = resp.groups[0]
    assert group.service == "uts-shared-deployment-api"
    assert group.fragmented is False
    v = group.versions[0]
    assert v.version == ":a557471"
    assert v.built_from == "a557471"
    assert v.artifact == "unified-trading-system/deployment-api"
    assert DRIFT_OK in v.drift
    assert v.hosts[0].name == "uts-shared-deployment-api"
    assert v.hosts[0].kind == "Cloud Run svc"


def test_running_flags_floating_latest_tag(monkeypatch: pytest.MonkeyPatch) -> None:
    deploys = [_deploy_fact("svc", "r1", CHANGE_NEW, "2026-07-21T00:00:00+00:00", live=True, digest="sha256:BBB")]
    images = [_reg_image("svc", "sha256:BBB", ["latest"], "2026-07-21T00:00:00+00:00")]
    svc = _running_svc_with(monkeypatch, deploys=deploys, images=images)
    resp = svc.running()

    v = resp.groups[0].versions[0]
    assert v.version == ":latest"
    assert DRIFT_FLOATING in v.drift
    assert resp.stats.floating == 1


def test_running_flags_unresolved_digest_and_hand_deploy(monkeypatch: pytest.MonkeyPatch) -> None:
    unresolved = _deploy_fact(
        "svc-a", "r1", CHANGE_NEW, "2026-07-21T00:00:00+00:00", live=True, digest="sha256:NOTFOUND"
    )
    hand = DeployFact(
        cloud="gcp",
        workload="svc-b",
        revision="r2",
        digest="sha256:CCC",
        built_from="",
        resolvable=False,
        change_type=CHANGE_NEW,
        at="2026-07-21T00:00:00+00:00",
        live=True,
        deployer="human@example.com",
    )
    images = [_reg_image("svc-b", "sha256:CCC", ["deadbee"], "2026-07-21T00:00:00+00:00")]
    svc = _running_svc_with(monkeypatch, deploys=[unresolved, hand], images=images)
    resp = svc.running()

    by_service = {g.service: g.versions[0] for g in resp.groups}
    assert DRIFT_UNKNOWN in by_service["svc-a"].drift  # digest not in the current AR inventory
    assert DRIFT_HAND in by_service["svc-b"].drift  # deployed by a human, not Cloud Build
    assert resp.stats.unknown == 1
    assert resp.stats.hand == 1


def test_running_only_includes_currently_live_revisions(monkeypatch: pytest.MonkeyPatch) -> None:
    deploys = [
        _deploy_fact("svc", "r1", CHANGE_NEW, "2026-07-20T00:00:00+00:00", live=False, digest="sha256:AAA"),
        _deploy_fact("svc", "r2", CHANGE_NEW, "2026-07-21T00:00:00+00:00", live=True, digest="sha256:BBB"),
    ]
    svc = _running_svc_with(monkeypatch, deploys=deploys)
    resp = svc.running()
    assert len(resp.groups) == 1
    assert resp.groups[0].versions[0].digest == "sha256:BBB"


def test_pick_sha_tag() -> None:
    assert service_mod._pick_sha_tag(["0.10.0", "a557471"]) == "a557471"
    assert service_mod._pick_sha_tag(["latest"]) == ""
    assert service_mod._pick_sha_tag([]) == ""


def test_repo_from_ar_uri() -> None:
    uri = "asia-northeast1-docker.pkg.dev/proj/unified-trading-system/deployment-api@sha256:abc"
    assert providers_mod._repo_from_ar_uri(uri) == "deployment-api"
    assert providers_mod._repo_from_ar_uri("asia-northeast1-docker.pkg.dev/proj/other-repo/x@sha256:abc") == ""


# ── GCS tarball-manifest bucket provider (Phase 3d) — parsing + the live-shaped listing ─────────────
def test_parse_tarball_stem_floating_and_pinned() -> None:
    assert providers_mod._parse_tarball_stem("alerting-service-code") == ("alerting-service", "")
    assert providers_mod._parse_tarball_stem("alerting-service-code@4b3aad7181cb782c1ea41677fa1e720765aad88f") == (
        "alerting-service",
        "4b3aad7181cb782c1ea41677fa1e720765aad88f",
    )
    assert providers_mod._parse_tarball_stem("batch-live-reconciliation-service-code@503ba57") == (
        "batch-live-reconciliation-service",
        "503ba57",
    )
    # bucket housekeeping files never end in "-code" — not a tarball stem at all
    assert providers_mod._parse_tarball_stem("_refresh_status") is None
    assert providers_mod._parse_tarball_stem("_test_write") is None


class _FakeBlob:
    def __init__(self, name: str, size: int, last_modified: str) -> None:
        self.name = name
        self.size = size
        self.last_modified = last_modified


class _FakeStorageClient:
    def __init__(self, blobs: list[_FakeBlob]) -> None:
        self._blobs = blobs

    def list_blobs(self, *, bucket: str, prefix: str) -> list[_FakeBlob]:
        assert bucket == "deployment-scripts-test-project"  # GCP_PROJECT_ID=test-project (tests/unit/conftest.py)
        assert prefix == "code/"
        return list(self._blobs)


def _patch_tarball_bucket(monkeypatch: pytest.MonkeyPatch, blobs: list[_FakeBlob]) -> None:
    import unified_trading_library

    monkeypatch.setattr(unified_trading_library, "get_storage_client", lambda: _FakeStorageClient(blobs))


def test_gcp_tarball_manifest_builds_reads_the_live_bucket_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    """Mirrors the LIVE-VERIFIED (2026-07-27) object shape: a floating pointer + its paired SHA-pinned
    copy for one repo, plus an unrelated housekeeping file that must be skipped."""
    blobs = [
        _FakeBlob("code/alerting-service-code.manifest.json", 512, "2026-07-12T12:15:54+00:00"),
        _FakeBlob("code/alerting-service-code.tar.gz", 900_000, "2026-07-12T12:15:54+00:00"),
        _FakeBlob(
            "code/alerting-service-code@4b3aad7181cb782c1ea41677fa1e720765aad88f.manifest.json",
            512,
            "2026-07-12T12:15:54+00:00",
        ),
        _FakeBlob(
            "code/alerting-service-code@4b3aad7181cb782c1ea41677fa1e720765aad88f.tar.gz",
            900_000,
            "2026-07-12T12:15:54+00:00",
        ),
        _FakeBlob("code/_refresh_status.json", 40, "2026-07-27T00:00:00+00:00"),
    ]
    _patch_tarball_bucket(monkeypatch, blobs)

    builds = providers_mod.gcp_tarball_manifest_builds(DeploymentApiConfig())
    assert len(builds) == 1  # the floating pointer is skipped — it duplicates the pinned build event
    build = builds[0]
    assert build.repo == "alerting-service"
    assert build.lane == LANE_TARBALL
    assert build.sha == "4b3aad7"  # truncated to 7 chars, matching Cloud Build's convention
    assert build.status == "SUCCESS"
    assert build.started_at == "2026-07-12T12:15:54+00:00"
    assert build.produced == (
        "gs://deployment-scripts-test-project/code/"
        "alerting-service-code@4b3aad7181cb782c1ea41677fa1e720765aad88f.tar.gz"
    )


def test_gcp_tarball_manifest_images_counts_both_floating_and_pinned(monkeypatch: pytest.MonkeyPatch) -> None:
    blobs = [
        _FakeBlob("code/alerting-service-code.manifest.json", 512, "2026-07-12T12:15:54+00:00"),
        _FakeBlob("code/alerting-service-code.tar.gz", 900_000, "2026-07-12T12:15:54+00:00"),
        _FakeBlob(
            "code/alerting-service-code@4b3aad7181cb782c1ea41677fa1e720765aad88f.manifest.json",
            512,
            "2026-07-12T12:15:54+00:00",
        ),
        _FakeBlob(
            "code/alerting-service-code@4b3aad7181cb782c1ea41677fa1e720765aad88f.tar.gz",
            900_000,
            "2026-07-12T12:15:54+00:00",
        ),
    ]
    _patch_tarball_bucket(monkeypatch, blobs)

    images = providers_mod.gcp_tarball_manifest_images(DeploymentApiConfig())
    assert len(images) == 2  # unlike builds(), the Artifacts roll-up counts the floating pointer too
    assert all(i.registry == REGISTRY_TARBALL_BUCKET for i in images)
    assert all(i.digest == "" for i in images)  # tarballs carry no docker digest — honest absence
    tags = {t for i in images for t in i.tags}
    assert tags == {"floating", "4b3aad7"}


def test_gcp_tarball_manifest_images_backfills_size_from_the_sibling_tarball(monkeypatch: pytest.MonkeyPatch) -> None:
    """The manifest object's own size (a small JSON file) must NOT be reported as the artifact's
    size — the sibling `.tar.gz` object's size is the real one."""
    blobs = [
        _FakeBlob("code/deployment-api-code.manifest.json", 512, "2026-07-12T12:15:54+00:00"),
        _FakeBlob("code/deployment-api-code.tar.gz", 12_345_678, "2026-07-12T12:15:54+00:00"),
    ]
    _patch_tarball_bucket(monkeypatch, blobs)
    images = providers_mod.gcp_tarball_manifest_images(DeploymentApiConfig())
    assert images[0].size_bytes == 12_345_678


def test_gcp_tarball_manifest_images_honest_none_size_when_tarball_object_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An orphaned manifest with no matching `.tar.gz` (deleted, or a partial upload) reports an
    honest `None` size — never a fabricated fallback to the manifest's own (unrelated) byte count."""
    blobs = [_FakeBlob("code/orphaned-repo-code.manifest.json", 512, "2026-07-12T12:15:54+00:00")]
    _patch_tarball_bucket(monkeypatch, blobs)
    images = providers_mod.gcp_tarball_manifest_images(DeploymentApiConfig())
    assert images[0].size_bytes is None


# ── health view (derived purely from the already-fetched facts — no new cloud calls) ───────────────
def test_health_always_reports_aws_deferred(monkeypatch: pytest.MonkeyPatch) -> None:
    svc = _running_svc_with(monkeypatch, deploys=[])
    resp = svc.health()
    aws = next(c for c in resp.conditions if "AWS" in c.condition)
    assert aws.severity == SEV_DEFERRED
    assert resp.stats.deferred >= 1


def test_health_flags_live_failed_deploy_as_high(monkeypatch: pytest.MonkeyPatch) -> None:
    deploys = [_deploy_fact("svc", "r1", CHANGE_FAILED, "2026-07-21T00:00:00+00:00", live=True)]
    svc = _running_svc_with(monkeypatch, deploys=deploys)
    resp = svc.health()
    high = [c for c in resp.conditions if c.severity == SEV_HIGH]
    assert len(high) == 1
    assert "never went ready" in high[0].condition
    assert resp.stats.high == 1


def test_health_flags_recent_failures_dup_builds_and_registry_sprawl(monkeypatch: pytest.MonkeyPatch) -> None:
    # Relative to "now" (not a hardcoded literal): health()'s recent-failure/dup-build check
    # uses a rolling 7-day trailing window off datetime.now(UTC), so a fixed calendar date
    # silently ages out of the window as real time passes (it did — 2026-07-22 was in-window
    # when written, out by 2026-07-29). 2 days ago has margin on both edges of the window.
    two_days_ago = (datetime.now(UTC) - timedelta(days=2)).replace(microsecond=0)
    two_days_ago_plus_1h = two_days_ago + timedelta(hours=1)
    builds = [
        _fact("svc-a", "111", "FAILURE", two_days_ago.isoformat()),
        _fact("svc-b", "222", "SUCCESS", two_days_ago.isoformat()),
        _fact("svc-b", "222", "SUCCESS", two_days_ago_plus_1h.isoformat()),  # dup of the row above
    ]
    images = [_reg_image("sprawl-svc", f"sha256:{i:04d}", [f"tag{i}"], "2026-07-20T00:00:00+00:00") for i in range(600)]
    svc = _running_svc_with(monkeypatch, deploys=[], images=images, builds=builds)
    resp = svc.health()

    fail_cond = next(c for c in resp.conditions if "failed in the last 7 days" in c.condition)
    assert fail_cond.severity == SEV_MED
    assert fail_cond.count == "1"

    dup_cond = next(c for c in resp.conditions if "built more than once" in c.condition)
    assert dup_cond.severity == SEV_LOW
    assert dup_cond.count == "1"

    sprawl_cond = next(c for c in resp.conditions if "lifecycle/GC policy" in c.condition)
    assert sprawl_cond.severity == SEV_LOW
    assert sprawl_cond.count == "600"


def test_health_stats_real_defects_excludes_deferred(monkeypatch: pytest.MonkeyPatch) -> None:
    deploys = [_deploy_fact("svc", "r1", CHANGE_FAILED, "2026-07-21T00:00:00+00:00", live=True)]
    svc = _running_svc_with(monkeypatch, deploys=deploys)
    resp = svc.health()
    assert resp.stats.real_defects == resp.stats.high + resp.stats.med + resp.stats.low
    assert resp.stats.deferred >= 1


def _tarball_image(repo: str, *, sha: str = "abc1234") -> RegistryImageFact:
    return RegistryImageFact(
        cloud="gcp",
        registry=REGISTRY_TARBALL_BUCKET,
        repo=repo,
        digest="",
        tags=[sha],
        pushed_at="2026-07-20T00:00:00+00:00",
        size_bytes=100,
    )


def test_health_tarball_git_commit_gap_is_gated_on_tarball_data_existing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Phase 3d (2026-07-27): the condition must NOT fire as a blanket always-on placeholder — only
    when images() actually found tarball-lane repos (previously it fired unconditionally, `count="fleet"`)."""
    svc_no_tarball = _running_svc_with(monkeypatch, deploys=[])
    monkeypatch.setattr(providers_mod, "gcp_tarball_manifest_images", lambda _cfg, scan=20000: [])
    monkeypatch.setattr(providers_mod, "gcp_tarball_manifest_builds", lambda _cfg, scan=20000: [])
    resp_none = svc_no_tarball.health()
    assert not any("tarball-lane workloads carry no measured git commit" in c.condition for c in resp_none.conditions)

    monkeypatch.setattr(
        providers_mod, "gcp_tarball_manifest_images", lambda _cfg, scan=20000: [_tarball_image("features")]
    )
    svc_with_tarball = ArtifactPipelineService()
    resp_some = svc_with_tarball.health()
    gap = next(c for c in resp_some.conditions if "tarball-lane workloads carry no measured git commit" in c.condition)
    assert gap.count == "1 repos"
    assert gap.severity == SEV_MED


def test_health_flags_tarball_provider_likely_failed_when_ar_has_rows_but_tarball_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ar_facts = [_reg_image("deployment-api", "sha256:AAA", ["abc1234"], "2026-07-20T00:00:00+00:00")]
    monkeypatch.setattr(providers_mod, "gcp_artifact_registry_images", lambda _cfg, scan=5000: list(ar_facts))
    monkeypatch.setattr(providers_mod, "gcp_cloud_run_revisions", lambda _cfg: [])
    monkeypatch.setattr(providers_mod, "gcp_cloud_builds", lambda _cfg, scan=400: [])
    monkeypatch.setattr(providers_mod, "gcp_tarball_manifest_images", lambda _cfg, scan=20000: [])  # the symptom
    monkeypatch.setattr(providers_mod, "gcp_tarball_manifest_builds", lambda _cfg, scan=20000: [])
    svc = ArtifactPipelineService()
    resp = svc.health()
    failed = next(c for c in resp.conditions if "tarball-bucket provider returned zero rows" in c.condition)
    assert failed.severity == SEV_MED
    assert failed.tab == "art"


def test_health_does_not_flag_tarball_provider_failure_when_ar_is_also_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """When AR itself is empty too, this can't be distinguished from a genuine full outage — the
    symptom-based condition requires AR rows present as proof images() itself is working."""
    monkeypatch.setattr(providers_mod, "gcp_artifact_registry_images", lambda _cfg, scan=5000: [])
    monkeypatch.setattr(providers_mod, "gcp_cloud_run_revisions", lambda _cfg: [])
    monkeypatch.setattr(providers_mod, "gcp_cloud_builds", lambda _cfg, scan=400: [])
    monkeypatch.setattr(providers_mod, "gcp_tarball_manifest_images", lambda _cfg, scan=20000: [])
    monkeypatch.setattr(providers_mod, "gcp_tarball_manifest_builds", lambda _cfg, scan=20000: [])
    svc = ArtifactPipelineService()
    resp = svc.health()
    assert not any("tarball-bucket provider returned zero rows" in c.condition for c in resp.conditions)
