"""Unit tests for routes/_cloud_scheduler.py — the Cloud Scheduler on-time / OVERDUE census (#9).

Credential-free: only the pure mappers/verdict are tested (the REST list call is not exercised).
Pins the honest OVERDUE rule — an ENABLED job whose next fire is past + grace, or whose last attempt
failed, is overdue; a paused/disabled job never is.
"""

from __future__ import annotations

from datetime import UTC, datetime

from deployment_api.routes._cloud_scheduler import (
    _is_overdue,  # pyright: ignore[reportPrivateUsage]
    _target,  # pyright: ignore[reportPrivateUsage]
    build_scheduler_status,
)

_NOW = datetime(2026, 7, 10, 12, 0, 0, tzinfo=UTC)
_PAST = datetime(2026, 7, 10, 11, 0, 0, tzinfo=UTC)  # 1h ago (> 15-min grace)
_FUTURE = datetime(2026, 7, 10, 13, 0, 0, tzinfo=UTC)


def test_overdue_when_next_fire_is_past_grace() -> None:
    assert _is_overdue("ENABLED", _PAST, True, _NOW) is True


def test_not_overdue_when_next_fire_is_in_the_future() -> None:
    assert _is_overdue("ENABLED", _FUTURE, True, _NOW) is False


def test_overdue_when_last_attempt_failed_even_if_next_is_future() -> None:
    assert _is_overdue("ENABLED", _FUTURE, False, _NOW) is True


def test_paused_or_disabled_is_never_overdue() -> None:
    assert _is_overdue("PAUSED", _PAST, False, _NOW) is False
    assert _is_overdue("DISABLED", _PAST, False, _NOW) is False


def test_target_extraction_from_each_target_type() -> None:
    assert _target({"httpTarget": {"uri": "https://run.app/prd-consolidator-cefi"}}) == "prd-consolidator-cefi"
    assert _target({"pubsubTarget": {"topicName": "projects/p/topics/ingest"}}) == "ingest"
    assert _target({}) == ""


def test_build_scheduler_status_overdue_job() -> None:
    job: dict[str, object] = {
        "name": "projects/p/locations/asia-northeast1/jobs/consolidator-cron",
        "schedule": "*/15 * * * *",
        "state": "ENABLED",
        "lastAttemptTime": "2026-07-10T10:00:00Z",
        "scheduleTime": "2026-07-10T11:00:00Z",  # past → overdue
        "status": {"code": 0},
        "httpTarget": {"uri": "https://run.app/prd-manifest-consolidator-cefi"},
    }
    status = build_scheduler_status(job, "asia-northeast1", _NOW)
    assert status.name == "consolidator-cron"
    assert status.state == "ENABLED"
    assert status.last_attempt_ok is True
    assert status.target == "prd-manifest-consolidator-cefi"
    assert status.region == "asia-northeast1"
    assert status.overdue is True


def test_build_scheduler_status_healthy_job() -> None:
    job: dict[str, object] = {
        "name": "projects/p/locations/asia-northeast1/jobs/healthy-cron",
        "schedule": "0 * * * *",
        "state": "ENABLED",
        "lastAttemptTime": "2026-07-10T11:55:00Z",
        "scheduleTime": "2026-07-10T13:00:00Z",  # future → on time
        "status": {},
    }
    status = build_scheduler_status(job, "asia-northeast1", _NOW)
    assert status.overdue is False
    assert status.last_attempt_ok is True  # empty status == code 0 == success
