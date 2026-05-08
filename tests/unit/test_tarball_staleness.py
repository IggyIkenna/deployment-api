"""Unit tests for `deployment_api.services.tarball_staleness`.

Phase 1 of plans/active/deploy_missing_auto_launch_2026_05_07.plan.md.

Coverage:
  1. `_bundle_for` — known asset_groups return CORE + group; unknown
     raises; ALL is the dedup'd union.
  2. `get_tarball_mtime` — present blob returns `.updated`; missing
     blob returns None.
  3. `compute_bundle_oldest_mtime` — picks the OLDEST mtime; missing
     tarball forces None.
  4. `is_stale` — fresh bundle returns False; older mtime returns True;
     missing tarball returns True; naive datetime raises.
  5. `trigger_refresh` — emits event, calls invoker.run_trigger; unknown
     asset_group raises before invocation.
  6. `poll_build` — terminal status returns immediately; non-terminal
     loops then returns POLL_TIMEOUT after budget; terminal late within
     budget returns the terminal value.
  7. `ensure_fresh` — FRESH bundle skips trigger; STALE bundle with
     `allow_trigger=False` returns STALE_NO_TRIGGER; STALE + SUCCESS →
     REFRESHED; STALE + FAILURE → REFRESH_FAILED; STALE + POLL_TIMEOUT
     surfaces as POLL_TIMEOUT.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Protocol

# tests/unit/conftest.py + the rest of the deployment-api test fleet rely
# on these env vars being set before any import-time side effect fires.
# Mirrors the pattern in test_backfill_launch.py.
os.environ.setdefault("CLOUD_MOCK_MODE", "true")
os.environ.setdefault("CLOUD_PROVIDER", "local")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("DISABLE_AUTH", "true")
os.environ.setdefault("MOCK_STATE_MODE", "deterministic")

import pytest
from unified_trading_library.events import setup_events

# Initialise event logging in test mode so log_event() inside the helper
# is a no-op rather than a RuntimeError.
setup_events("deployment-api", "test")

from deployment_api.services.tarball_staleness import (
    DEFAULT_REFRESH_BRANCH,
    DEFAULT_REFRESH_TRIGGER_NAME,
    CloudBuildInvoker,
    RefreshResult,
    TarballStalenessChecker,
    TarballStalenessError,
    _bundle_for,
    list_known_asset_groups,
)

pytestmark = [pytest.mark.timeout(30)]


# ---------------------------------------------------------------------------
# In-memory fakes
# ---------------------------------------------------------------------------


@dataclass
class _FakeBlob:
    """Implements the `_BlobLike` Protocol."""

    name: str
    _exists: bool = True
    updated: datetime | None = None

    def exists(self) -> bool:
        return self._exists

    def reload(self) -> None:
        # No-op — the in-memory fake's `updated` is set at construction.
        return None


class _FakeBucket:
    """Implements the `_BucketLike` Protocol."""

    def __init__(self, blobs: dict[str, _FakeBlob]) -> None:
        self._blobs = blobs

    def blob(self, name: str) -> _FakeBlob:
        existing = self._blobs.get(name)
        if existing is not None:
            return existing
        # Mirror google-cloud-storage: blob() never returns None — it
        # returns a Blob whose `.exists()` is False until uploaded.
        return _FakeBlob(name=name, _exists=False, updated=None)


class _FakeStorage:
    """Implements the `_StorageLike` Protocol."""

    def __init__(self, bucket: _FakeBucket) -> None:
        self._bucket = bucket

    def bucket(self, name: str) -> _FakeBucket:
        # Single-bucket fake — every test scopes to the
        # deployment-scripts-{pid} bucket.
        return self._bucket


@dataclass
class _FakeInvoker:
    """Implements the `CloudBuildInvoker` Protocol."""

    build_id: str = "fake-build-id"
    log_url: str | None = "https://console.cloud.google.com/cloud-build/builds/fake-build-id"
    statuses: list[str] = field(default_factory=lambda: ["SUCCESS"])
    run_calls: list[tuple[str, str]] = field(default_factory=list)
    status_calls: list[str] = field(default_factory=list)

    def run_trigger(self, trigger_name: str, branch: str) -> tuple[str, str | None]:
        self.run_calls.append((trigger_name, branch))
        return self.build_id, self.log_url

    def get_build_status(self, build_id: str) -> str:
        self.status_calls.append(build_id)
        if not self.statuses:
            return "WORKING"  # never terminate — caller should hit POLL_TIMEOUT
        # Pop from the front so consecutive calls progress through the
        # configured status sequence (e.g. ["WORKING", "WORKING", "SUCCESS"]).
        return self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def fixed_now() -> datetime:
    return datetime(2026, 5, 8, 5, 0, 0, tzinfo=UTC)


@pytest.fixture
def all_fresh_blobs(fixed_now: datetime) -> dict[str, _FakeBlob]:
    """Every CEFI bundle tarball present + 30min newer than `fixed_now`."""
    bundle = _bundle_for("CEFI")
    fresh_ts = fixed_now + timedelta(minutes=30)
    return {
        f"code/{name}": _FakeBlob(name=f"code/{name}", _exists=True, updated=fresh_ts)
        for name in bundle
    }


@pytest.fixture
def one_stale_blob(fixed_now: datetime) -> dict[str, _FakeBlob]:
    """Every CEFI bundle tarball present, but ONE is 1h older."""
    bundle = _bundle_for("CEFI")
    fresh_ts = fixed_now + timedelta(minutes=30)
    stale_ts = fixed_now - timedelta(hours=1)
    blobs: dict[str, _FakeBlob] = {}
    for i, name in enumerate(bundle):
        ts = stale_ts if i == 3 else fresh_ts
        blobs[f"code/{name}"] = _FakeBlob(name=f"code/{name}", _exists=True, updated=ts)
    return blobs


class _MakeCheckerFactory(Protocol):
    """Callable signature exposed by the `make_checker` fixture below.

    Spelling out the Protocol explicitly lets pyright track the return
    type of `factory(...)` calls inside test methods (the `object`
    shorthand erases too much).
    """

    def __call__(
        self,
        blobs: dict[str, _FakeBlob],
        invoker: CloudBuildInvoker | None = None,
    ) -> tuple[TarballStalenessChecker, _FakeInvoker]: ...


@pytest.fixture
def make_checker() -> Iterator[_MakeCheckerFactory]:
    """Return a factory that builds a TarballStalenessChecker bound to
    in-memory storage + invoker fakes.
    """

    def _factory(
        blobs: dict[str, _FakeBlob],
        invoker: CloudBuildInvoker | None = None,
    ) -> tuple[TarballStalenessChecker, _FakeInvoker]:
        bucket = _FakeBucket(blobs=blobs)
        storage = _FakeStorage(bucket=bucket)
        fake_invoker: _FakeInvoker = (
            invoker if isinstance(invoker, _FakeInvoker) else _FakeInvoker()
        )
        checker = TarballStalenessChecker(
            project_id="test-project",
            invoker=fake_invoker,
            storage=storage,
        )
        return checker, fake_invoker

    yield _factory


# ---------------------------------------------------------------------------
# 1. _bundle_for
# ---------------------------------------------------------------------------


class TestBundleFor:
    def test_known_asset_groups_return_core_plus_group(self) -> None:
        for ag in ("CEFI", "TRADFI", "DEFI", "SPORTS", "PREDICTION"):
            tarballs = _bundle_for(ag)
            # CORE (4 entries) is always at the front.
            assert tarballs[0] == "unified-api-contracts-code.tar.gz"
            assert tarballs[1] == "unified-trading-library-code.tar.gz"
            assert tarballs[2] == "mtds-code.tar.gz"
            assert tarballs[3] == "deployment-service-code.tar.gz"
            # Bundle is non-empty after CORE.
            assert len(tarballs) > 4

    def test_lowercase_asset_group_is_normalised(self) -> None:
        upper = _bundle_for("CEFI")
        lower = _bundle_for("cefi")
        assert upper == lower

    def test_unknown_asset_group_raises(self) -> None:
        with pytest.raises(TarballStalenessError, match="Unknown asset_group"):
            _bundle_for("BOGUS")

    def test_all_dedups_across_groups(self) -> None:
        all_tarballs = _bundle_for("ALL")
        # No duplicates.
        assert len(set(all_tarballs)) == len(all_tarballs)
        # Strict superset of every group.
        for ag in ("CEFI", "TRADFI", "DEFI", "SPORTS", "PREDICTION"):
            assert set(_bundle_for(ag)).issubset(set(all_tarballs))

    def test_list_known_asset_groups_includes_all(self) -> None:
        groups = list(list_known_asset_groups())
        assert set(groups) == {"CEFI", "TRADFI", "DEFI", "SPORTS", "PREDICTION", "ALL"}


# ---------------------------------------------------------------------------
# 2. get_tarball_mtime
# ---------------------------------------------------------------------------


class TestGetTarballMtime:
    def test_present_blob_returns_updated(
        self,
        make_checker: _MakeCheckerFactory,
        all_fresh_blobs: dict[str, _FakeBlob],
        fixed_now: datetime,
    ) -> None:
        checker, _ = make_checker(all_fresh_blobs)
        mtime = checker.get_tarball_mtime("unified-api-contracts-code.tar.gz")
        assert mtime is not None
        assert mtime == fixed_now + timedelta(minutes=30)

    def test_missing_blob_returns_none(self, make_checker: _MakeCheckerFactory) -> None:
        checker, _ = make_checker({})
        assert checker.get_tarball_mtime("nonexistent.tar.gz") is None


# ---------------------------------------------------------------------------
# 3. compute_bundle_oldest_mtime
# ---------------------------------------------------------------------------


class TestComputeBundleOldestMtime:
    def test_all_fresh_returns_oldest_present(
        self,
        make_checker: _MakeCheckerFactory,
        all_fresh_blobs: dict[str, _FakeBlob],
        fixed_now: datetime,
    ) -> None:
        checker, _ = make_checker(all_fresh_blobs)
        oldest = checker.compute_bundle_oldest_mtime("CEFI")
        # All present at fresh_ts; oldest == fresh_ts.
        assert oldest == fixed_now + timedelta(minutes=30)

    def test_one_stale_blob_returns_stale_mtime(
        self,
        make_checker: _MakeCheckerFactory,
        one_stale_blob: dict[str, _FakeBlob],
        fixed_now: datetime,
    ) -> None:
        checker, _ = make_checker(one_stale_blob)
        oldest = checker.compute_bundle_oldest_mtime("CEFI")
        assert oldest == fixed_now - timedelta(hours=1)

    def test_missing_tarball_returns_none(self, make_checker: _MakeCheckerFactory) -> None:
        # Deliberately empty — every CEFI tarball is missing.
        checker, _ = make_checker({})
        assert checker.compute_bundle_oldest_mtime("CEFI") is None


# ---------------------------------------------------------------------------
# 4. is_stale
# ---------------------------------------------------------------------------


class TestIsStale:
    def test_fresh_bundle_returns_false(
        self,
        make_checker: _MakeCheckerFactory,
        all_fresh_blobs: dict[str, _FakeBlob],
        fixed_now: datetime,
    ) -> None:
        checker, _ = make_checker(all_fresh_blobs)
        assert checker.is_stale("CEFI", fixed_now) is False

    def test_older_mtime_returns_true(
        self,
        make_checker: _MakeCheckerFactory,
        one_stale_blob: dict[str, _FakeBlob],
        fixed_now: datetime,
    ) -> None:
        checker, _ = make_checker(one_stale_blob)
        assert checker.is_stale("CEFI", fixed_now) is True

    def test_missing_tarball_returns_true(
        self,
        make_checker: _MakeCheckerFactory,
        fixed_now: datetime,
    ) -> None:
        checker, _ = make_checker({})
        assert checker.is_stale("CEFI", fixed_now) is True

    def test_naive_datetime_raises(
        self,
        make_checker: _MakeCheckerFactory,
        all_fresh_blobs: dict[str, _FakeBlob],
    ) -> None:
        checker, _ = make_checker(all_fresh_blobs)
        with pytest.raises(TarballStalenessError, match="must be tz-aware"):
            checker.is_stale("CEFI", datetime(2026, 5, 8, 5, 0, 0))  # naive


# ---------------------------------------------------------------------------
# 5. trigger_refresh
# ---------------------------------------------------------------------------


class TestTriggerRefresh:
    def test_kicks_invoker_with_default_trigger_name(
        self,
        make_checker: _MakeCheckerFactory,
    ) -> None:
        invoker = _FakeInvoker(build_id="abc-123", log_url="https://logs/abc-123")
        checker, _ = make_checker({}, invoker=invoker)
        build_id, log_url = checker.trigger_refresh("CEFI")
        assert build_id == "abc-123"
        assert log_url == "https://logs/abc-123"
        assert invoker.run_calls == [(DEFAULT_REFRESH_TRIGGER_NAME, DEFAULT_REFRESH_BRANCH)]

    def test_custom_trigger_and_branch_propagate(self, make_checker: _MakeCheckerFactory) -> None:
        invoker = _FakeInvoker()
        checker, _ = make_checker({}, invoker=invoker)
        checker.trigger_refresh("DEFI", trigger_name="custom-trigger", branch="feat/x")
        assert invoker.run_calls == [("custom-trigger", "feat/x")]

    def test_unknown_asset_group_raises_before_invoking(
        self, make_checker: _MakeCheckerFactory
    ) -> None:
        invoker = _FakeInvoker()
        checker, _ = make_checker({}, invoker=invoker)
        with pytest.raises(TarballStalenessError, match="Unknown asset_group"):
            checker.trigger_refresh("BOGUS")
        # Critically: invoker MUST NOT have been called for an unknown group.
        assert invoker.run_calls == []


# ---------------------------------------------------------------------------
# 6. poll_build
# ---------------------------------------------------------------------------


class TestPollBuild:
    def test_terminal_status_returned_immediately(self, make_checker: _MakeCheckerFactory) -> None:
        invoker = _FakeInvoker(statuses=["SUCCESS"])
        checker, _ = make_checker({}, invoker=invoker)
        status = checker.poll_build("build-123", timeout_seconds=5.0, interval_seconds=0.01)
        assert status == "SUCCESS"
        assert invoker.status_calls == ["build-123"]

    def test_failure_terminal_status_propagates(self, make_checker: _MakeCheckerFactory) -> None:
        invoker = _FakeInvoker(statuses=["FAILURE"])
        checker, _ = make_checker({}, invoker=invoker)
        status = checker.poll_build("build-123", timeout_seconds=5.0, interval_seconds=0.01)
        assert status == "FAILURE"

    def test_non_terminal_loops_to_terminal(self, make_checker: _MakeCheckerFactory) -> None:
        invoker = _FakeInvoker(statuses=["WORKING", "WORKING", "SUCCESS"])
        checker, _ = make_checker({}, invoker=invoker)
        status = checker.poll_build("build-123", timeout_seconds=5.0, interval_seconds=0.01)
        assert status == "SUCCESS"
        # Three status reads to reach SUCCESS.
        assert len(invoker.status_calls) == 3

    def test_timeout_returns_poll_timeout(self, make_checker: _MakeCheckerFactory) -> None:
        # Empty statuses → fake invoker always returns "WORKING" → loop
        # exhausts the budget.
        invoker = _FakeInvoker(statuses=[])
        checker, _ = make_checker({}, invoker=invoker)
        status = checker.poll_build("build-123", timeout_seconds=0.05, interval_seconds=0.01)
        assert status == "POLL_TIMEOUT"


# ---------------------------------------------------------------------------
# 7. ensure_fresh — the headline orchestration test
# ---------------------------------------------------------------------------


class TestEnsureFresh:
    def test_fresh_bundle_skips_trigger(
        self,
        make_checker: _MakeCheckerFactory,
        all_fresh_blobs: dict[str, _FakeBlob],
        fixed_now: datetime,
    ) -> None:
        invoker = _FakeInvoker()
        checker, _ = make_checker(all_fresh_blobs, invoker=invoker)
        result = checker.ensure_fresh("CEFI", fixed_now)
        assert isinstance(result, RefreshResult)
        assert result.status == "FRESH"
        assert result.triggered is False
        assert result.build_id is None
        assert invoker.run_calls == []
        assert invoker.status_calls == []

    def test_stale_with_allow_trigger_false(
        self,
        make_checker: _MakeCheckerFactory,
        one_stale_blob: dict[str, _FakeBlob],
        fixed_now: datetime,
    ) -> None:
        invoker = _FakeInvoker()
        checker, _ = make_checker(one_stale_blob, invoker=invoker)
        result = checker.ensure_fresh("CEFI", fixed_now, allow_trigger=False)
        assert result.status == "STALE_NO_TRIGGER"
        assert result.triggered is False
        assert invoker.run_calls == []

    def test_stale_triggers_then_observes_success(
        self,
        make_checker: _MakeCheckerFactory,
        one_stale_blob: dict[str, _FakeBlob],
        fixed_now: datetime,
    ) -> None:
        invoker = _FakeInvoker(build_id="b-100", statuses=["SUCCESS"])
        checker, _ = make_checker(one_stale_blob, invoker=invoker)
        result = checker.ensure_fresh(
            "CEFI",
            fixed_now,
            poll_timeout_seconds=5.0,
            poll_interval_seconds=0.01,
        )
        assert result.status == "REFRESHED"
        assert result.triggered is True
        assert result.build_id == "b-100"
        assert len(invoker.run_calls) == 1

    def test_stale_triggers_then_observes_failure(
        self,
        make_checker: _MakeCheckerFactory,
        one_stale_blob: dict[str, _FakeBlob],
        fixed_now: datetime,
    ) -> None:
        invoker = _FakeInvoker(build_id="b-200", statuses=["FAILURE"])
        checker, _ = make_checker(one_stale_blob, invoker=invoker)
        result = checker.ensure_fresh(
            "CEFI",
            fixed_now,
            poll_timeout_seconds=5.0,
            poll_interval_seconds=0.01,
        )
        assert result.status == "REFRESH_FAILED"
        assert result.triggered is True
        assert result.build_id == "b-200"

    def test_stale_triggers_then_poll_timeout(
        self,
        make_checker: _MakeCheckerFactory,
        one_stale_blob: dict[str, _FakeBlob],
        fixed_now: datetime,
    ) -> None:
        # Empty statuses + zero terminal -> _FakeInvoker returns WORKING
        # forever; ensure_fresh should surface POLL_TIMEOUT.
        invoker = _FakeInvoker(build_id="b-300", statuses=[])
        checker, _ = make_checker(one_stale_blob, invoker=invoker)
        result = checker.ensure_fresh(
            "CEFI",
            fixed_now,
            poll_timeout_seconds=0.05,
            poll_interval_seconds=0.01,
        )
        assert result.status == "POLL_TIMEOUT"
        assert result.triggered is True
        assert result.build_id == "b-300"

    def test_naive_datetime_raises(
        self,
        make_checker: _MakeCheckerFactory,
        all_fresh_blobs: dict[str, _FakeBlob],
    ) -> None:
        checker, _ = make_checker(all_fresh_blobs)
        with pytest.raises(TarballStalenessError, match="must be tz-aware"):
            checker.ensure_fresh("CEFI", datetime(2026, 5, 8, 5, 0, 0))  # naive
