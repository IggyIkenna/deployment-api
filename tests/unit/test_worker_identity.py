"""
Unit tests for deployment_api.utils.worker_identity — the in-process leader
election gunicorn.conf.py's ``post_fork`` + deployment_api.lifespan use to run
singleton background tasks on exactly one worker per Cloud Run instance.
"""

import pytest

from deployment_api.utils import worker_identity


@pytest.fixture(autouse=True)
def _reset_worker_age(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every test starts from the "never set" (plain-uvicorn / not-yet-forked) state."""
    monkeypatch.setattr(worker_identity, "_worker_age", None)


class TestIsLeaderWorkerNoAgeSet:
    """No gunicorn arbiter recorded an age yet (local dev / tests / lone uvicorn)."""

    def test_defaults_to_leader(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(worker_identity, "WORKERS", 4)
        assert worker_identity.is_leader_worker() is True


class TestIsLeaderWorkerSingleWorker:
    """WORKERS <= 1: only one worker exists at all, so it is always the leader —
    regardless of whatever age gunicorn happened to assign it."""

    def test_always_leader_at_worker_count_one(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(worker_identity, "WORKERS", 1)
        worker_identity.set_worker_age(37)
        assert worker_identity.is_leader_worker() is True

    def test_always_leader_at_worker_count_zero(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(worker_identity, "WORKERS", 0)
        worker_identity.set_worker_age(5)
        assert worker_identity.is_leader_worker() is True


class TestIsLeaderWorkerRotation:
    """WORKERS > 1: exactly one age per worker_count-sized window is the leader,
    and leadership rotates (mod worker_count) as gunicorn recycles workers."""

    def test_first_spawned_worker_is_leader(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(worker_identity, "WORKERS", 4)
        worker_identity.set_worker_age(1)
        assert worker_identity.is_leader_worker() is True

    @pytest.mark.parametrize("age", [2, 3, 4])
    def test_non_leader_ages_in_first_window(self, monkeypatch: pytest.MonkeyPatch, age: int) -> None:
        monkeypatch.setattr(worker_identity, "WORKERS", 4)
        worker_identity.set_worker_age(age)
        assert worker_identity.is_leader_worker() is False

    def test_leadership_rotates_to_replacement_worker(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """gunicorn.Worker.age never repeats within one arbiter lifetime — when the
        age=1 leader is recycled (max_requests or a crash) and replaced, the new
        worker's age (5, continuing the arbiter's counter) lands back on the same
        residue class mod WORKERS=4, so leadership self-heals onto it."""
        monkeypatch.setattr(worker_identity, "WORKERS", 4)
        worker_identity.set_worker_age(5)
        assert worker_identity.is_leader_worker() is True

    def test_only_one_worker_in_a_live_set_is_leader(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(worker_identity, "WORKERS", 4)
        leaders = []
        for age in (1, 2, 3, 4):
            worker_identity.set_worker_age(age)
            leaders.append(worker_identity.is_leader_worker())
        assert leaders == [True, False, False, False]


class TestSetWorkerAge:
    def test_records_age_for_later_read(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(worker_identity, "WORKERS", 2)
        worker_identity.set_worker_age(3)
        assert worker_identity.is_leader_worker() is True  # (3-1) % 2 == 0
        worker_identity.set_worker_age(4)
        assert worker_identity.is_leader_worker() is False  # (4-1) % 2 == 1
