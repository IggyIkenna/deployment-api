"""
Build history and library-status helpers for cloud_builds.

Covers:
- _format_build_info: serialise a Cloud Build object to BuildInfoDict
- _get_recent_builds_for_triggers: parallel per-trigger latest-build fetch
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
from contextlib import suppress
from itertools import islice
from typing import TYPE_CHECKING

# Cloud Build builds/triggers live in CLOUD_BUILD_REGION (asia-northeast1, operator
# matched-region decision 2026-05-11) — NOT GCS_REGION (a storage region, e.g. us-central1).
# Listing builds in the GCS region returns 400 InvalidArgument (no Cloud Build there), which
# was swallowed → the repo-CI Image column showed nothing. Match the trigger module's region.
from deployment_api.settings import CLOUD_BUILD_REGION as DEFAULT_REGION
from deployment_api.settings import gcp_project_id as default_project_id

from ._cloud_builds_types import (
    BuildInfoDict,
    get_cloudbuild_v1,  # Use public alias instead of private _cloudbuild_v1
    get_gcp_build_client,  # Use public alias instead of private _get_gcp_build_client
)

if TYPE_CHECKING:
    from google.cloud.devtools import cloudbuild_v1

logger = logging.getLogger(__name__)


def _format_build_info(build: object) -> BuildInfoDict:
    """Format a Cloud Build object into a serializable dict."""
    build_id = str(getattr(build, "id", "") or "")
    status_obj: object = getattr(build, "status", None)
    status_name = str(getattr(status_obj, "name", "") or "")
    create_time: object = getattr(build, "create_time", None)
    finish_time: object = getattr(build, "finish_time", None)
    substitutions: object = getattr(build, "substitutions", None)
    log_url_raw: object = getattr(build, "log_url", None)
    log_url = str(log_url_raw) if log_url_raw is not None else None

    create_time_iso: object = getattr(create_time, "isoformat", None)
    create_time_str: str | None = str(create_time_iso()) if callable(create_time_iso) else None
    finish_time_iso: object = getattr(finish_time, "isoformat", None)
    finish_time_str: str | None = str(finish_time_iso()) if callable(finish_time_iso) else None

    duration_seconds: float | None = None
    if finish_time is not None and create_time is not None:
        with suppress(TypeError, AttributeError):
            sub_method = getattr(finish_time, "__sub__", None)
            if callable(sub_method):
                delta: object = sub_method(create_time)
                if delta is not None:
                    total_seconds_attr = getattr(delta, "total_seconds", None)
                    if callable(total_seconds_attr):
                        total_seconds_result = total_seconds_attr()
                        if isinstance(total_seconds_result, (int, float)):
                            duration_seconds = float(total_seconds_result)

    commit_sha: str | None = None
    branch: str | None = None
    if substitutions is not None:
        sub_get = getattr(substitutions, "get", None)
        if callable(sub_get):
            sha_raw: object = sub_get("COMMIT_SHA") or ""
            commit_sha = str(sha_raw)[:7] if sha_raw else None
            branch_raw: object = sub_get("BRANCH_NAME")
            branch = str(branch_raw) if branch_raw is not None else None

    return {
        "build_id": build_id,
        "status": status_name,
        "create_time": create_time_str,
        "finish_time": finish_time_str,
        "duration_seconds": duration_seconds,
        "commit_sha": commit_sha,
        "branch": branch,
        "log_url": log_url,
    }


async def _get_recent_builds_for_triggers(
    trigger_ids: list[str],
) -> dict[str, BuildInfoDict]:
    """Get the most recent build for each trigger using parallel filtered queries.

    Uses Cloud Build API filter parameter to query per-trigger with page_size=1,
    running all queries in parallel via ThreadPoolExecutor.
    This is much faster than fetching all builds and filtering in memory.
    """
    if not trigger_ids:
        return {}

    try:

        def _fetch_latest_build(
            client: cloudbuild_v1.CloudBuildClient, trigger_id: str
        ) -> tuple[str, BuildInfoDict] | None:
            """Fetch the latest build for a single trigger using API-level filter."""
            _cb = get_cloudbuild_v1()
            parent = f"projects/{default_project_id}/locations/{DEFAULT_REGION}"
            request = _cb.ListBuildsRequest(
                parent=parent,
                page_size=1,
                filter=f'build_trigger_id="{trigger_id}"',
            )
            # Use next(iter(...)) to get only the first build without exhausting the pager
            build = next(iter(client.list_builds(request=request)), None)  # pyright: ignore[reportUnknownMemberType]  # CloudBuild stubs incomplete
            if not build:
                return None
            return (trigger_id, _format_build_info(build))

        def _fetch_all_sync() -> dict[str, BuildInfoDict]:
            _cb = get_cloudbuild_v1()
            client = get_gcp_build_client()
            results: dict[str, BuildInfoDict] = {}

            # Run parallel queries - one per trigger, max 8 concurrent
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(trigger_ids), 8)) as executor:
                futures = {executor.submit(_fetch_latest_build, client, tid): tid for tid in trigger_ids}
                for future in concurrent.futures.as_completed(futures):
                    try:
                        result = future.result()
                        if result:
                            tid, build_info = result
                            results[tid] = build_info
                    except (OSError, ValueError, RuntimeError) as e:
                        tid = futures[future]
                        logger.warning("Error fetching latest build for trigger %s: %s", tid, e)

            return results

        return await asyncio.to_thread(_fetch_all_sync)

    except (OSError, ValueError, RuntimeError) as e:
        logger.warning("Error getting recent builds: %s", e)
        return {}


async def _recent_builds_by_repo_name(
    max_scan: int = 400,
) -> dict[str, tuple[BuildInfoDict, BuildInfoDict | None]]:
    """repo_name -> (latest build, last SUCCESSFUL build | None), grouped from recent builds
    via the REPO_NAME substitution that every Cloud Build carries.

    Robust where the per-trigger path is not: the regional Cloud Build list_builds API rejects
    the `build_trigger_id="..."` filter (400 InvalidArgument), and a build's build_trigger_id
    drifts from the current trigger list whenever a trigger is recreated. REPO_NAME maps 1:1 to
    the repo, so fetch recent builds ONCE (newest-first by create_time) and, per repo, keep the
    first build seen (= latest) AND the first SUCCESS seen (= last good build, so a red latest
    build doesn't hide the last green image). Scans up to max_scan builds; repos with no build in
    that window stay honestly-absent, and last-success is None when no SUCCESS is in the window.
    """

    def _fetch_sync() -> dict[str, tuple[BuildInfoDict, BuildInfoDict | None]]:
        _cb = get_cloudbuild_v1()
        client = get_gcp_build_client()
        parent = f"projects/{default_project_id}/locations/{DEFAULT_REGION}"
        request = _cb.ListBuildsRequest(parent=parent, page_size=100)  # default order: create_time desc
        latest: dict[str, BuildInfoDict] = {}
        success: dict[str, BuildInfoDict] = {}
        for build in islice(client.list_builds(request=request), max_scan):  # pyright: ignore[reportUnknownMemberType]  # CloudBuild stubs incomplete
            substitutions: object = getattr(build, "substitutions", None)
            repo = ""
            if substitutions is not None:
                sub_get = getattr(substitutions, "get", None)
                if callable(sub_get):
                    repo = str(sub_get("REPO_NAME") or "")
            if not repo:
                continue
            info = _format_build_info(build)
            if repo not in latest:  # first occurrence = newest build for that repo
                latest[repo] = info
            if repo not in success and info.get("status") == "SUCCESS":  # first SUCCESS = last good build
                success[repo] = info
        return {repo: (info, success.get(repo)) for repo, info in latest.items()}

    try:
        return await asyncio.to_thread(_fetch_sync)
    except (OSError, ValueError, RuntimeError) as e:
        logger.warning("Error getting recent builds by repo: %s", e)
        return {}
