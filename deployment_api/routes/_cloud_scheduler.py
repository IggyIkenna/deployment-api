# Epic: observability_master
# Lifecycle: permanent
"""Cloud Scheduler census — the "fired at the right time?" signal (WS-D #9).

Cloud Scheduler is the ONLY honest source of "did this job fire on its cadence": a Cloud Run job's
latest EXECUTION time cannot tell you whether it fired LATE or was skipped entirely. We read the
Scheduler jobs via the REST API + ADC — deliberately NOT the ``google-cloud-scheduler`` SDK, which
is not in the dependency set (a REST GET through an ``AuthorizedSession`` keeps this self-contained,
no new workspace dep). A job is **OVERDUE** when its next scheduled time is already in the past
beyond a grace window, or its last attempt FAILED.

Honest degradation: any failure (no creds / API disabled / region unsupported / a non-200) is logged
and yields an empty census — a scheduler we cannot read is NEVER a fabricated "on time".
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from google.auth.transport.requests import AuthorizedSession

logger = logging.getLogger(__name__)

_RPC_TIMEOUT_SEC = 30.0
DEFAULT_SCHEDULER_REGION = "asia-northeast1"
# Grace after the next scheduled time before a job is called OVERDUE (a run in flight / clock skew).
_OVERDUE_GRACE = timedelta(minutes=15)
_SCHEDULER_API = "https://cloudscheduler.googleapis.com/v1/projects/{project}/locations/{region}/jobs"


@dataclass(frozen=True)
class SchedulerJobStatus:
    """One Cloud Scheduler job's cadence + last-fire status (the on-time signal)."""

    name: str  # short scheduler job name (last path segment)
    schedule: str  # cron expression
    state: str  # ENABLED / PAUSED / DISABLED / UPDATE_FAILED
    last_attempt_at: str | None  # ISO of the last fire attempt
    next_schedule_at: str | None  # ISO of the next scheduled fire (the overdue signal)
    last_attempt_ok: bool | None  # None = never attempted; else last attempt's status.code == 0
    target: str  # best-effort: the job / URL / topic it triggers
    region: str
    overdue: bool  # computed: next fire is past + grace, or the last attempt failed


def _authed_session() -> AuthorizedSession | None:
    """An ADC-authed HTTP session for the Scheduler REST API, or None if creds are unavailable."""
    try:
        import google.auth  # noqa: imports-inside-functions  # deferred: only the scheduler census needs ADC
        from google.auth.transport.requests import AuthorizedSession  # noqa: imports-inside-functions

        credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        return AuthorizedSession(credentials)
    except Exception as exc:
        logger.warning("scheduler: ADC session unavailable (census degrades to empty): %s", exc)
        return None


def _parse_iso(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _target(job: dict[str, object]) -> str:
    """Best-effort name of what the scheduler triggers (the Cloud Run job / URL / Pub/Sub topic)."""
    for key in ("httpTarget", "pubsubTarget", "appEngineHttpTarget"):
        tgt = job.get(key)
        if isinstance(tgt, dict):
            for field in ("uri", "topicName", "relativeUri"):
                val = tgt.get(field)
                if isinstance(val, str) and val:
                    return val.rstrip("/").rsplit("/", 1)[-1]
    return ""


def _is_overdue(state: str, next_schedule_at: datetime | None, last_attempt_ok: bool | None, now: datetime) -> bool:
    """OVERDUE = an ENABLED job whose next fire is past + grace, or whose last attempt FAILED.

    A PAUSED / DISABLED job is intentionally not firing → never overdue.
    """
    if state != "ENABLED":
        return False
    if last_attempt_ok is False:
        return True
    return next_schedule_at is not None and now > next_schedule_at + _OVERDUE_GRACE


def build_scheduler_status(job: dict[str, object], region: str, now: datetime) -> SchedulerJobStatus:
    """Map one Scheduler REST job resource to a SchedulerJobStatus (+ the computed overdue verdict)."""
    name = str(job.get("name") or "").rsplit("/", 1)[-1]
    state = str(job.get("state") or "")
    schedule = str(job.get("schedule") or "")
    last_attempt = job.get("lastAttemptTime")
    next_schedule = job.get("scheduleTime")
    last_attempt_at = str(last_attempt) if last_attempt else None
    next_schedule_at = str(next_schedule) if next_schedule else None
    last_attempt_ok: bool | None = None
    if last_attempt_at:
        # status is a google.rpc.Status; an absent / empty code means success (0).
        status_obj = job.get("status")
        code = int(status_obj.get("code", 0) or 0) if isinstance(status_obj, dict) else 0
        last_attempt_ok = code == 0
    return SchedulerJobStatus(
        name=name,
        schedule=schedule,
        state=state,
        last_attempt_at=last_attempt_at,
        next_schedule_at=next_schedule_at,
        last_attempt_ok=last_attempt_ok,
        target=_target(job),
        region=region,
        overdue=_is_overdue(state, _parse_iso(next_schedule_at), last_attempt_ok, now),
    )


def list_scheduler_jobs(project_id: str, region: str, now: datetime) -> list[SchedulerJobStatus]:
    """Census every Cloud Scheduler job in one region via the REST API (ADC-authed).

    Honest degradation: no creds / API disabled / a non-200 / unsupported region → an empty list
    (logged), never a fabricated on-time verdict. Paginates ``nextPageToken``.
    """
    session = _authed_session()
    if session is None:
        return []
    url = _SCHEDULER_API.format(project=project_id, region=region)
    jobs: list[SchedulerJobStatus] = []
    try:
        page_token = ""
        while True:
            params = {"pageToken": page_token} if page_token else {}
            resp = session.get(url, params=params, timeout=_RPC_TIMEOUT_SEC)
            if resp.status_code != 200:
                logger.warning("scheduler: list %s/%s -> HTTP %s (degrading)", project_id, region, resp.status_code)
                return jobs
            payload: dict[str, object] = resp.json()
            for job in payload.get("jobs") or []:
                if isinstance(job, dict):
                    jobs.append(build_scheduler_status(job, region, now))
            page_token = str(payload.get("nextPageToken") or "")
            if not page_token:
                break
    except Exception as exc:
        logger.warning("scheduler: census for %s/%s failed (degrading to empty): %s", project_id, region, exc)
    return jobs


__all__ = [
    "DEFAULT_SCHEDULER_REGION",
    "SchedulerJobStatus",
    "build_scheduler_status",
    "list_scheduler_jobs",
]
