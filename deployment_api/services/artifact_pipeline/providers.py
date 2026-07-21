"""Per-source provider adapters — native cloud rows → normalized artifact-pipeline facts.

Each cloud/source is a free function returning a list of normalized facts (`BuildFact` /
`DeployFact` / `ImageFact`) or raising. The caller wraps every one in `_safe`, so a single
cloud's failure (creds, API down, WIF role unset, region) degrades to an empty list and never
blanks the others (shard-level failure isolation — the same discipline the cost page and the
deployment census use). The SDK boundaries are the repo's sanctioned ones:

  * GCP Cloud Build — the UTL `get_cloud_build_client` factory (`_cloud_builds_types` pattern).
  * GCP Cloud Run / Compute — the deployment-service `backends._gcp_sdk` lazy boundary.
  * GCP Artifact Registry — the route-local deferred `from google.cloud import artifactregistry_v1`
    (`# noqa: cloud-sdk-direct`, the one place it is allowed, mirroring `routes/builds.py`).
  * AWS ECR / CodeBuild / App Runner — deferred `import boto3` behind the keyless GCP→AWS WIF
    client (`_code_builds_aws` pattern); the ECS census comes through
    `deployment_service.backends.aws_census` (never inline boto3 from here).

Honesty rule: a value a source genuinely can't resolve is left empty (`digest=""`, `sha=""`),
never guessed — the service turns that into an explicit drift flag, never a fabricated green.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import cast

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.services.artifact_pipeline.models import (
    LANE_IMAGE,
    BuildFact,
)

logger = logging.getLogger(__name__)

# Cloud Build + Artifact Registry live in asia-northeast1 (operator matched-region decision
# 2026-05-11) — NOT the GCS region; listing builds elsewhere 400s. Mirrors
# `settings.CLOUD_BUILD_REGION` / `routes/builds.py`.
_GCP_REGION = "asia-northeast1"

# How many builds back to enumerate per list (Cloud Build returns newest-first). ~400 covers the
# free ~60-day window measured in the plan; the window filter trims to the requested range.
_CLOUD_BUILD_SCAN = 400

# Per-RPC deadline for the Cloud Build list. Without it a long-lived gRPC channel that has gone stale
# (token expiry / dropped connection after the process idles ~1h) makes the call hang *indefinitely* —
# and `safe` cannot catch a hang, only an exception. With a deadline a stale channel raises
# DeadlineExceeded, `safe` degrades to [], and the endpoint never wedges. A cold 400-build scan is
# ~5s, so 30s is comfortably above a legitimate call while bounding the failure mode.
_RPC_TIMEOUT_SECONDS = 30.0


def safe[T](loader: Callable[[], list[T]], source: str) -> list[T]:
    """Run one provider; on ANY failure log it and return `[]` so peers still render.

    The load-bearing isolation primitive (copied from `cost_observability.service._safe`): a
    provider that raises contributes nothing, never a 5xx and never a blanked page.
    """
    try:
        return loader()
    except Exception as exc:  # deliberate catch-all: one source must never blank the others
        logger.warning("artifact-pipeline provider %s failed: %s", source, exc)
        return []


def _project_id(cfg: DeploymentApiConfig) -> str:
    """Resolve the GCP project id (raises inside `safe`'s try if unset — degrades to [])."""
    return cfg.require_gcp_project_id()


# ══════════════════════════════════════════════════════════════════════════════════════════════════
# GCP Cloud Build → BuildFact (the image lane, the active production path)
# ══════════════════════════════════════════════════════════════════════════════════════════════════


def _sub_get(substitutions: object, key: str) -> str:
    """Read one Cloud Build substitution defensively (the proto map is stubbed as `object`)."""
    getter = getattr(substitutions, "get", None)
    if callable(getter):
        value: object = getter(key)
        if value:
            return str(value)
    return ""


def _iso_or_empty(ts: object) -> str:
    """`.isoformat()` a Cloud Build timestamp defensively, or "" when absent."""
    iso = getattr(ts, "isoformat", None)
    return str(iso()) if callable(iso) else ""


def _duration_seconds(create_time: object, finish_time: object) -> float | None:
    """Finish minus create in seconds, or None when either bound is missing/unsubtractable."""
    if create_time is None or finish_time is None:
        return None
    sub = getattr(finish_time, "__sub__", None)
    if not callable(sub):
        return None
    delta: object = sub(create_time)
    total = getattr(delta, "total_seconds", None)
    if callable(total):
        result = total()
        if isinstance(result, (int, float)):
            return float(result)
    return None


def _build_steps(build: object) -> list[tuple[str, str, float]]:
    """Extract (step-id, status, seconds) for the drawer's step timeline, defensively."""
    steps_raw: object = getattr(build, "steps", None)
    if not isinstance(steps_raw, (list, tuple)):
        return []
    out: list[tuple[str, str, float]] = []
    for step in cast("list[object]", steps_raw):
        name = str(getattr(step, "id", "") or getattr(step, "name", "") or "")
        status_obj: object = getattr(step, "status", None)
        status = str(getattr(status_obj, "name", "") or "")
        timing: object = getattr(step, "timing", None)
        seconds = _duration_seconds(getattr(timing, "start_time", None), getattr(timing, "end_time", None)) or 0.0
        out.append((name, status, seconds))
    return out


def _produced_image(build: object) -> str:
    """The first image the build produced (`build.images[0]`), or "" — for the 'Produced' cell."""
    images: object = getattr(build, "images", None)
    if isinstance(images, (list, tuple)) and images:
        return str(cast("list[object]", images)[0])
    return ""


def _build_to_fact(build: object) -> BuildFact:
    """Map one Cloud Build proto → BuildFact. Defensive throughout (the proto is stubbed `object`)."""
    substitutions: object = getattr(build, "substitutions", None)
    status_obj: object = getattr(build, "status", None)
    create_time: object = getattr(build, "create_time", None)
    finish_time: object = getattr(build, "finish_time", None)
    failure: object = getattr(build, "failure_info", None)
    failure_type_obj: object = getattr(failure, "type_", None)

    return BuildFact(
        cloud="gcp",
        lane=LANE_IMAGE,
        repo=_sub_get(substitutions, "REPO_NAME") or _sub_get(substitutions, "_SERVICE_NAME"),
        build_id=str(getattr(build, "id", "") or ""),
        status=str(getattr(status_obj, "name", "") or ""),
        trigger=_sub_get(substitutions, "TRIGGER_NAME") or str(getattr(build, "build_trigger_id", "") or ""),
        sha=_sub_get(substitutions, "COMMIT_SHA")[:7],
        branch=_sub_get(substitutions, "BRANCH_NAME"),
        started_at=_iso_or_empty(create_time),
        finished_at=_iso_or_empty(finish_time),
        duration_sec=_duration_seconds(create_time, finish_time),
        produced=_produced_image(build),
        initiator=_sub_get(substitutions, "TRIGGER_NAME"),
        log_url=str(getattr(build, "log_url", "") or ""),
        failure_type=str(getattr(failure_type_obj, "name", "") or ""),
        failure_detail=str(getattr(failure, "detail", "") or ""),
        steps=_build_steps(build),
    )


def gcp_cloud_builds(cfg: DeploymentApiConfig, scan: int = _CLOUD_BUILD_SCAN) -> list[BuildFact]:
    """List recent GCP Cloud Build history as BuildFacts (newest-first, capped at `scan`).

    Reuses the repo's Cloud Build client factory + region pin. Adds the structured
    `failure_info{type,detail}` + `steps[]` the narrow build-history route never projected —
    the whole point of the pipeline view.
    """
    from itertools import islice

    from deployment_api.routes._cloud_builds_types import get_cloudbuild_v1, get_gcp_build_client

    project = _project_id(cfg)
    cb = get_cloudbuild_v1()
    client = get_gcp_build_client()
    parent = f"projects/{project}/locations/{_GCP_REGION}"
    request = cb.ListBuildsRequest(parent=parent, page_size=100)  # default order: create_time desc
    facts: list[BuildFact] = []
    pager = client.list_builds(request=request, timeout=_RPC_TIMEOUT_SECONDS)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]  # cloudbuild stubs incomplete
    for build in islice(pager, scan):  # pyright: ignore[reportUnknownArgumentType]  # cloudbuild stubs incomplete
        facts.append(_build_to_fact(build))
    return facts
