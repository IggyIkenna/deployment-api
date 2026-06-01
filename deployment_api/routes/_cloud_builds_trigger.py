"""
Trigger listing, caching, and run helpers for cloud_builds.

Covers:
- Trigger ID cache
- Resolving trigger repo / GitHub info
- Building / populating / querying trigger lists
- Running a trigger operation synchronously
- Extracting build ID from operation response
- Finding recent builds after trigger
"""

from __future__ import annotations

import logging
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from itertools import islice
from typing import TYPE_CHECKING, cast

from deployment_api.settings import GCS_REGION as DEFAULT_REGION
from deployment_api.settings import gcp_project_id as default_project_id

from ._cloud_builds_types import (
    INFRASTRUCTURE_WITH_TRIGGERS,
    LIBRARIES_WITH_TRIGGERS,
    SERVICES_WITH_TRIGGERS,
    RecentBuildDict,
    TriggerDict,
    TriggerRunResultDict,
    get_cloudbuild_v1,
    get_gcp_build_client,
)

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# Module-level aliases so tests can patch without reaching into _cloud_builds_types
_get_gcp_build_client = get_gcp_build_client
_cloudbuild_v1 = get_cloudbuild_v1

# ---------------------------------------------------------------------------
# Trigger ID cache
# ---------------------------------------------------------------------------

_trigger_id_cache: dict[str, str] = {}  # trigger_name -> trigger_id
_trigger_cache_time: float = 0
_TRIGGER_CACHE_TTL = 3600  # 1 hour


def _populate_trigger_cache(triggers_list: Sequence[object]) -> None:
    """Populate trigger ID cache from a list of Cloud Build trigger objects."""
    global _trigger_id_cache, _trigger_cache_time
    new_cache: dict[str, str] = {}
    for t in triggers_list:
        t_name = str(getattr(t, "name", "") or "")
        t_id = str(getattr(t, "id", "") or "")
        if t_name and t_id:
            new_cache[t_name] = t_id
    _trigger_id_cache = new_cache
    _trigger_cache_time = time.time()


def _get_cached_trigger_id(trigger_name: str) -> str | None:
    """Get trigger ID from cache. Returns None if not cached or expired."""
    if time.time() - _trigger_cache_time > _TRIGGER_CACHE_TTL:
        return None
    return _trigger_id_cache.get(trigger_name)


# ---------------------------------------------------------------------------
# Trigger classification helpers
# ---------------------------------------------------------------------------


def _resolve_trigger_repo(trigger: object) -> tuple[str | None, str | None]:
    """Resolve repo name and type from a Cloud Build trigger object."""
    t_name = str(getattr(trigger, "name", "") or "")
    for service in SERVICES_WITH_TRIGGERS:
        if t_name == f"{service}-build":
            return service, "service"
    for library in LIBRARIES_WITH_TRIGGERS:
        if t_name == f"{library}-build":
            return library, "library"
    for infra in INFRASTRUCTURE_WITH_TRIGGERS:
        if t_name == f"{infra}-build":
            return infra, "infrastructure"
    return None, None


def _extract_github_info(trigger: object) -> tuple[str | None, str | None]:
    """Extract github_repo and branch_pattern from a Cloud Build trigger object."""
    github: object = cast(object, getattr(trigger, "github", None))
    repo_event: object = cast(object, getattr(trigger, "repository_event_config", None))
    if github:
        owner = str(cast(object, getattr(github, "owner", "")) or "")
        name = str(cast(object, getattr(github, "name", "")) or "")
        github_repo: str | None = f"{owner}/{name}" if owner and name else None
        push: object = cast(object, getattr(github, "push", None))
        branch_pattern: str | None = str(cast(object, getattr(push, "branch", "")) or "") if push else None
        return github_repo, branch_pattern or None
    if repo_event:
        repo_path = str(cast(object, getattr(repo_event, "repository", "")) or "")
        parts = repo_path.split("/")
        github_repo = parts[-1] if parts else None
        push = cast(object, getattr(repo_event, "push", None))
        branch_pattern = str(cast(object, getattr(push, "branch", "")) or "") if push else None
        return github_repo, branch_pattern or None
    return None, None


# ---------------------------------------------------------------------------
# Trigger listing
# ---------------------------------------------------------------------------


def _build_trigger_list_sync() -> list[TriggerDict]:
    """Synchronously fetch and classify all Cloud Build triggers."""
    _cb = _cloudbuild_v1()
    client = _get_gcp_build_client()
    parent = f"projects/{default_project_id}/locations/{DEFAULT_REGION}"
    request = _cb.ListBuildTriggersRequest(parent=parent, page_size=50)
    triggers = list(client.list_build_triggers(request=request))  # pyright: ignore[reportUnknownMemberType]  # CloudBuild stubs incomplete
    _populate_trigger_cache(triggers)
    result: list[TriggerDict] = []
    for trigger in triggers:
        repo_name, repo_type = _resolve_trigger_repo(trigger)
        if not repo_name:
            continue
        github_repo, branch_pattern = _extract_github_info(trigger)
        result.append(
            cast(
                TriggerDict,
                {
                    "trigger_id": str(getattr(trigger, "id", "") or ""),
                    "trigger_name": str(getattr(trigger, "name", "") or ""),
                    "service": repo_name,
                    "type": repo_type,
                    "github_repo": github_repo,
                    "branch_pattern": branch_pattern,
                    "disabled": bool(getattr(trigger, "disabled", False)),
                    "status": "disabled" if getattr(trigger, "disabled", False) else "active",
                },
            )
        )
    return result


# ---------------------------------------------------------------------------
# Trigger ID lookup
# ---------------------------------------------------------------------------


def _get_trigger_id_sync(trigger_name: str) -> str | None:
    """Get the trigger ID for a trigger name (uses cache first, falls back to API)."""
    cached_id = _get_cached_trigger_id(trigger_name)
    if cached_id:
        return cached_id
    _cb = _cloudbuild_v1()
    client = _get_gcp_build_client()
    parent = f"projects/{default_project_id}/locations/{DEFAULT_REGION}"
    triggers_request = _cb.ListBuildTriggersRequest(parent=parent)
    triggers = list(client.list_build_triggers(request=triggers_request))  # pyright: ignore[reportUnknownMemberType]  # CloudBuild stubs incomplete
    _populate_trigger_cache(triggers)
    return _trigger_id_cache.get(trigger_name)


# ---------------------------------------------------------------------------
# Find recent build after trigger run
# ---------------------------------------------------------------------------


def _find_recent_build_sync(trigger_id: str, started_after: datetime) -> RecentBuildDict | None:
    """Find a build for trigger_id that started after the given time."""
    _cb = _cloudbuild_v1()
    client = _get_gcp_build_client()
    parent = f"projects/{default_project_id}/locations/{DEFAULT_REGION}"
    builds_request = _cb.ListBuildsRequest(
        parent=parent,
        page_size=5,
        filter=f'build_trigger_id="{trigger_id}"',
    )
    for build in islice(client.list_builds(request=builds_request), 5):  # pyright: ignore[reportUnknownMemberType]  # CloudBuild stubs incomplete
        if build.create_time and build.create_time >= started_after:  # type: ignore[reportUnknownMemberType]
            return {"build_id": build.id, "log_url": build.log_url, "status": build.status.name}
    return None


# ---------------------------------------------------------------------------
# Extract build ID from operation
# ---------------------------------------------------------------------------


def _extract_build_id_from_op(op_name: str | None, operation: object) -> tuple[str | None, str | None]:
    """Extract build_id and log_url from a Cloud Build operation object."""
    from ._cloud_builds_types import _build_op_meta_cls  # type: ignore[reportPrivateUsage]

    build_id = None
    log_url = None
    try:
        if hasattr(operation, "metadata") and getattr(operation, "metadata", None):
            meta = _build_op_meta_cls()()
            op_metadata: object = getattr(operation, "metadata", None)
            if op_metadata is not None and hasattr(op_metadata, "Unpack") and op_metadata.Unpack(meta) and meta.build:  # type: ignore[reportUnknownMemberType,reportAttributeAccessIssue]
                build_id = meta.build.id
                log_url = meta.build.log_url
    except (OSError, ValueError, RuntimeError) as unpack_err:
        logger.warning("Could not unpack BuildOperationMetadata: %s", unpack_err)
    if not build_id and op_name:
        try:
            parts = op_name.split("/")
            if len(parts) >= 2 and parts[-2] == "operations":
                potential_id = parts[-1]
                if "-" in potential_id and len(potential_id) > 30:
                    build_id = potential_id
        except (ValueError, KeyError, TypeError) as e:
            logger.debug("Suppressed %s during operation: %s", type(e).__name__, e)
    return build_id, log_url


# ---------------------------------------------------------------------------
# Run trigger operation
# ---------------------------------------------------------------------------


def _run_trigger_operation_sync(trigger_name: str, branch: str) -> TriggerRunResultDict:
    """Run the Cloud Build trigger synchronously. Returns trigger result metadata."""
    _cb = _cloudbuild_v1()
    client = _get_gcp_build_client()
    trigger_id = _get_trigger_id_sync(trigger_name)
    if not trigger_id:
        logger.warning("Could not find trigger ID for %s", trigger_name)
    trigger_time = datetime.now(UTC)
    name = f"projects/{default_project_id}/locations/{DEFAULT_REGION}/triggers/{trigger_name}"
    logger.info("Attempting to run trigger: %s on branch %s", name, branch)
    run_request = _cb.RunBuildTriggerRequest(
        name=name,
        source=_cb.RepoSource(branch_name=branch),
    )
    operation = client.run_build_trigger(request=run_request)  # pyright: ignore[reportUnknownMemberType]  # CloudBuild stubs incomplete
    op_name: str | None = cast(str | None, getattr(operation, "name", None))
    op_done = getattr(operation, "done", None)
    logger.info("Trigger operation returned. Operation name: %s, done: %s", op_name, op_done)
    build_id, log_url = _extract_build_id_from_op(op_name, operation)
    if build_id:
        logger.info("Got build info: build_id=%s", build_id)
    return {
        "success": True,
        "build_id": build_id,
        "log_url": log_url,
        "trigger_id": trigger_id,
        "trigger_time": trigger_time,
    }
