"""Unit tests for the artifact-pipeline service + route (the /ops/artifacts backend).

Mocks the provider at the module seam (`providers.gcp_cloud_builds`) so nothing touches a cloud —
`--block-network` safe. Covers the pure helpers, the builds view's window filtering / stats / dup /
cross-lane / filters, and the route's loud 400 range gate.
"""

from __future__ import annotations

from datetime import date

import pytest
from fastapi import HTTPException

from deployment_api.routes.artifacts import _resolve_range
from deployment_api.services.artifact_pipeline import ArtifactPipelineService
from deployment_api.services.artifact_pipeline import providers as providers_mod
from deployment_api.services.artifact_pipeline import service as service_mod
from deployment_api.services.artifact_pipeline.models import LANE_IMAGE, LANE_TARBALL, BuildFact
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
