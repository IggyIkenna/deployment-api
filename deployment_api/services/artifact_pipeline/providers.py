"""Per-source provider adapters — native cloud rows → normalized artifact-pipeline facts.

Each cloud/source is a free function returning a list of normalized facts (`BuildFact` /
`DeployFact` / `ImageFact`) or raising. The caller wraps every one in `_safe`, so a single
cloud's failure (creds, API down, WIF role unset, region) degrades to an empty list and never
blanks the others (shard-level failure isolation — the same discipline the cost page and the
deployment census use). The SDK boundaries are the repo's sanctioned ones:

  * GCP Cloud Build — the UTL `get_cloud_build_client` factory (`_cloud_builds_types` pattern).
  * GCP Cloud Run / Compute — the deployment-service `backends._gcp_sdk` lazy boundary.
  * GCP Artifact Registry — the route-local deferred, `google.cloud`-direct `artifactregistry_v1`
    import (`# noqa: cloud-sdk-direct`, the one place it is allowed, mirroring `routes/builds.py`).
  * AWS ECR / CodeBuild / App Runner — deferred `import boto3` behind the keyless GCP→AWS WIF
    client (`_code_builds_aws` pattern); the ECS census comes through
    `deployment_service.backends.aws_census` (never inline boto3 from here).

Honesty rule: a value a source genuinely can't resolve is left empty (`digest=""`, `sha=""`),
never guessed — the service turns that into an explicit drift flag, never a fabricated green.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Sequence
from itertools import islice
from typing import cast

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.services.artifact_pipeline.models import (
    CHANGE_CONFIG,
    CHANGE_FAILED,
    CHANGE_NEW,
    CHANGE_ROLLBACK,
    LANE_IMAGE,
    LANE_TARBALL,
    REGISTRY_TARBALL_BUCKET,
    BuildFact,
    DeployFact,
    RegistryImageFact,
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


def _as_item_list(value: object) -> list[object]:
    """Normalize a protobuf repeated field (or a plain list/tuple, or None) to a Python list.

    MEASURED 2026-07-23: google-cloud repeated fields (`Build.steps`, `Build.images`,
    `Revision.containers`, `Revision.conditions`, …) are runtime instances of
    `proto.marshal.collections.repeated.Repeated` / `RepeatedComposite` — NOT `list`/`tuple` — so
    an `isinstance(x, (list, tuple))` gate (the pattern used elsewhere in this codebase for proto
    maps) silently drops every real field while a hand-built test double using a plain list sails
    through. Any non-None, non-string iterable is treated as a sequence; anything else degrades to
    `[]` rather than raising.
    """
    if value is None or isinstance(value, (str, bytes)):
        return []
    try:
        return list(cast("Iterable[object]", value))
    except TypeError:
        return []


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
    out: list[tuple[str, str, float]] = []
    for step in _as_item_list(getattr(build, "steps", None)):
        name = str(getattr(step, "id", "") or getattr(step, "name", "") or "")
        status_obj: object = getattr(step, "status", None)
        status = str(getattr(status_obj, "name", "") or "")
        timing: object = getattr(step, "timing", None)
        seconds = _duration_seconds(getattr(timing, "start_time", None), getattr(timing, "end_time", None)) or 0.0
        out.append((name, status, seconds))
    return out


def _produced_image(build: object) -> str:
    """The first image the build produced (`build.images[0]`), or "" — for the 'Produced' cell."""
    images = _as_item_list(getattr(build, "images", None))
    return str(images[0]) if images else ""


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


# ══════════════════════════════════════════════════════════════════════════════════════════════════
# GCP Cloud Run revisions → DeployFact (the image lane's deploy history)
#
# AWS App Runner/ECS operations and GCE VM launches (the tarball-lane "deploy") are later
# increments — deferred the same way AWS CodeBuild was deferred for the builds view: this ships
# the active production path (GCP Cloud Run) first, each `_safe`-isolated so a later addition
# never risks what already works.
# ══════════════════════════════════════════════════════════════════════════════════════════════════

# Per-service revision cap (newest-first). MEASURED 2026-07-23: the busiest live service carries
# 255 revisions and a full 16-service/690-revision scan takes ~9s cold — comfortably fast, so this
# is a generous safety ceiling (a runaway redeploy loop), not a real trim; the window filter in
# `service.py` does the actual date-range narrowing.
_CLOUD_RUN_REVISION_SCAN = 300


def _digest_from_image(image_ref: str) -> str:
    """The `@sha256:...` digest off a Cloud Run container image ref, or "" if not digest-pinned.

    Cloud Run resolves the deploy-time tag (the pipeline deploys `:$SHORT_SHA`, a mutable tag) to
    a digest AT DEPLOY TIME and stores that resolved digest on the revision — so this is honestly
    provable, not a guess, even though the deploy command itself used a tag.
    """
    if "@sha256:" in image_ref:
        return image_ref.split("@", 1)[1]
    return ""


def _revision_ready(revision: object) -> bool:
    """Did this revision's `Ready` condition report CONDITION_SUCCEEDED?

    Defensive: an unreadable/absent conditions list defaults to ready=True rather than mislabeling
    every revision "failed" on a stub/shape surprise (the honest-unknown default leans toward not
    fabricating a red).
    """
    conditions = _as_item_list(getattr(revision, "conditions", None))
    if not conditions:
        return True
    for cond in conditions:
        if str(getattr(cond, "type_", "")) == "Ready":
            state_obj: object = getattr(cond, "state", None)
            return str(getattr(state_obj, "name", "")) == "CONDITION_SUCCEEDED"
    return True


def _format_deployer(creator: str) -> str:
    """A short, honest label for `revision.creator` — a CI service account, or a human email as-is."""
    if not creator:
        return ""
    if creator.endswith("@cloudbuild.gserviceaccount.com"):
        return "Cloud Build"
    if "gserviceaccount.com" in creator:
        return creator.split("@", 1)[0]
    return creator  # a human email — surfaced verbatim; hand-deploy classification is the Running view's job


def _revision_image(revision: object) -> str:
    """The first container's image ref off a Revision, or "" when no container is readable."""
    containers = _as_item_list(getattr(revision, "containers", None))
    return str(getattr(containers[0], "image", "") or "") if containers else ""


def _classify_service_revisions(
    workload: str, revisions_newest_first: Sequence[object], live_revision: str
) -> list[DeployFact]:
    """One service's revisions (newest-first from the API) → classified, chronologically-ordered DeployFacts.

    Two passes over the oldest-first order: change-type walks forward tracking the digest sequence
    (unresolvable-digest and never-ready revisions never claim a false "config-only" match);
    held-for looks ONE STEP AHEAD to the revision that replaced it (the newest revision has no
    successor yet, so it holds "" — still current, not "held for zero"). `built_from`/`resolvable`
    stay honestly empty — the digest→SHA join is the Running view's runtime-join, not this view's.
    """
    oldest_first = list(reversed(revisions_newest_first))
    parsed: list[tuple[str, str, bool, object, str]] = [
        (
            str(getattr(rev, "name", "") or "").rsplit("/", 1)[-1],
            _digest_from_image(_revision_image(rev)),
            _revision_ready(rev),
            getattr(rev, "create_time", None),
            _format_deployer(str(getattr(rev, "creator", "") or "")),
        )
        for rev in oldest_first
    ]

    facts: list[DeployFact] = []
    seen_digests: set[str] = set()
    prev_digest: str | None = None
    for i, (short_name, digest, ready, created, deployer) in enumerate(parsed):
        if not digest:
            change_type = CHANGE_FAILED if not ready else CHANGE_NEW
        elif not ready:
            change_type = CHANGE_FAILED
        elif prev_digest is None:
            change_type = CHANGE_NEW
        elif digest == prev_digest:
            change_type = CHANGE_CONFIG
        elif digest in seen_digests:
            change_type = CHANGE_ROLLBACK
        else:
            change_type = CHANGE_NEW
        if digest:
            seen_digests.add(digest)
            prev_digest = digest

        held_for = ""
        if i + 1 < len(parsed):
            next_created = parsed[i + 1][3]
            if created is not None and next_created is not None:
                secs = _duration_seconds(created, next_created)  # the successor's create_time minus this one's
                if secs is not None:
                    held_for = _fmt_span(secs)

        facts.append(
            DeployFact(
                cloud="gcp",
                workload=workload,
                revision=short_name,
                digest=digest,
                built_from="",
                resolvable=False,
                change_type=change_type,
                at=_iso_or_empty(created),
                held_for=held_for,
                live=short_name == live_revision,
                deployer=deployer,
                link_kind="revision",
            )
        )

    return facts


def _fmt_span(seconds: float) -> str:
    """Human duration for 'held for', up to day granularity: '41s' / '3h12m' / '2d4h'."""
    total = int(seconds)
    if total < 0:
        return ""
    if total < 3600:
        return f"{total // 60}m{total % 60:02d}s" if total >= 60 else f"{total}s"
    if total < 86400:
        return f"{total // 3600}h{(total % 3600) // 60:02d}m"
    return f"{total // 86400}d{(total % 86400) // 3600}h"


def gcp_cloud_run_revisions(cfg: DeploymentApiConfig) -> list[DeployFact]:
    """List every live Cloud Run service's revision history as classified DeployFacts.

    Reuses the inventory's `list_cloud_run_services` (already `_gcp_sdk`-bounded, already honestly
    degrades to [] on failure) both to enumerate workloads and to resolve which revision is
    currently live — no second services-list RPC. Then lists each service's revisions
    (`RevisionsClient`, the same `_gcp_sdk` boundary) and classifies them per-service.
    """
    from deployment_service.backends import _gcp_sdk  # noqa: imports-inside-functions

    from deployment_api.routes._cloud_run_services import list_cloud_run_services

    project = _project_id(cfg)
    services = list_cloud_run_services(project, region=_GCP_REGION)
    run_v2 = _gcp_sdk.run_v2
    client = run_v2.RevisionsClient()

    facts: list[DeployFact] = []
    for svc in services:
        parent = f"projects/{project}/locations/{_GCP_REGION}/services/{svc.name}"
        request = run_v2.ListRevisionsRequest(parent=parent)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]  # run_v2 stubs incomplete
        pager = client.list_revisions(request=request, timeout=_RPC_TIMEOUT_SECONDS)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
        revisions = list(islice(pager, _CLOUD_RUN_REVISION_SCAN))  # pyright: ignore[reportUnknownArgumentType]
        facts.extend(_classify_service_revisions(svc.name, revisions, svc.revision))
    return facts


# ══════════════════════════════════════════════════════════════════════════════════════════════════
# GCP Artifact Registry → RegistryImageFact (the `images` view's source + the `running` view's
# digest→tag→SHA join target)
# ══════════════════════════════════════════════════════════════════════════════════════════════════

# The one canonical AR repository every service's Cloud Build push lands in (Codex fix 2026-07-23:
# `dual-cloud-image-builds.md` says "unified-trading" — that name 404s; the real one is
# "unified-trading-system", ~1.5 TB). One `list_docker_images` call over it returns every image for
# every service — there is no need to enumerate a repo per service.
_AR_REGISTRY = "unified-trading-system"

# MEASURED 2026-07-23: the whole registry is 3365 images across 20 repos, ~4s cold with page_size
# 1000 (dominated by market-tick-data-service's 1901). This cap is a runaway-safety net, not a real
# trim — the registry would have to grow ~1.5x before it started truncating.
_AR_IMAGE_SCAN = 5000


def _repo_from_ar_uri(uri: str) -> str:
    """The service/repo name segment of an AR image URI, or "" if it's not under `_AR_REGISTRY`.

    URI shape: `{host}/{project}/{_AR_REGISTRY}/{repo}@sha256:{digest}`.
    """
    marker = f"/{_AR_REGISTRY}/"
    if marker not in uri:
        return ""
    return uri.split(marker, 1)[1].split("@", 1)[0]


def gcp_artifact_registry_images(cfg: DeploymentApiConfig, scan: int = _AR_IMAGE_SCAN) -> list[RegistryImageFact]:
    """List every pushed image in the canonical AR registry as RegistryImageFacts.

    One row per digest (an image can carry several tags — a version tag, a short-SHA tag, and
    sometimes `:latest` all pointing at the same digest). The sanctioned deferred-import boundary
    for Artifact Registry (mirrors `routes/builds.py`'s `_list_ar_tags_from_repo`, sync client here
    since the rest of this module is sync).
    """
    from google.cloud import (  # noqa: TID251  # noqa: cloud-sdk-direct, imports-inside-functions — the sanctioned AR boundary, mirrors routes/builds.py
        artifactregistry_v1,
    )

    project = _project_id(cfg)
    client = artifactregistry_v1.ArtifactRegistryClient()
    parent = f"projects/{project}/locations/{_GCP_REGION}/repositories/{_AR_REGISTRY}"
    request = artifactregistry_v1.ListDockerImagesRequest(parent=parent, page_size=1000)  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]  # AR stubs incomplete
    pager = client.list_docker_images(request=request, timeout=_RPC_TIMEOUT_SECONDS)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]  # AR stubs incomplete

    facts: list[RegistryImageFact] = []
    for img in islice(pager, scan):  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]  # AR stubs incomplete
        uri = str(getattr(img, "uri", "") or "")
        repo = _repo_from_ar_uri(uri)
        if not repo:
            continue  # a roll-up/other-repository image outside the canonical registry — skip, don't guess
        digest = uri.rsplit("@", 1)[-1] if "@" in uri else ""
        size = getattr(img, "image_size_bytes", None)
        facts.append(
            RegistryImageFact(
                cloud="gcp",
                registry=_AR_REGISTRY,
                repo=repo,
                digest=digest,
                tags=[str(t) for t in _as_item_list(getattr(img, "tags", None))],
                pushed_at=_iso_or_empty(getattr(img, "upload_time", None)),
                size_bytes=int(size) if isinstance(size, (int, float)) and size else None,
            )
        )
    return facts


# ══════════════════════════════════════════════════════════════════════════════════════════════════
# GCS tarball-manifest bucket → BuildFact (Lane B builds) + RegistryImageFact (the Artifacts view's
# tarball rows) — the gap confirmed 2026-07-24: no provider anywhere read this bucket, so the
# tarball lane was silently absent (not filtered-empty) from every view that promises it.
# ══════════════════════════════════════════════════════════════════════════════════════════════════

# The VM-deployment code-tarball bucket (Lane B) is `deployment-scripts-{project_id}` — LIVE-VERIFIED
# 2026-07-27 via `gcloud storage ls`, matching `code_tarball_refresh_scheduler.tf`'s
# `gs://{code-bucket}/code/*-code.tar.gz` comment exactly. Bypasses `resolve_bucket_name()` today — a
# pre-existing, separately-tracked issue (Phase 6 stretch, plan § "Prior art to ABSORB") — but derives
# the project id from config rather than hardcoding it, unlike the shell scripts in this lane
# (`setup-data-pipeline-vm.sh:47`, `create-code-tarballs.sh:46`), which is a separate, already-tracked
# issue this provider doesn't need to repeat.
_TARBALL_BUCKET_PREFIX = "deployment-scripts-"
_TARBALL_PREFIX = "code/"
_TARBALL_BRANCH = "live-defi-rollout"  # the refresh cron's only target (code_tarball_refresh_scheduler.tf)

# Runaway-safety net, not a real trim — MEASURED 2026-07-27: ~4966 manifests / ~1046 tarballs live
# (up from the plan's 2026-07-17 measurement of 4064/163; no lifecycle rule on `code/` means this
# only grows). A `list_blobs` scan over this many objects is still a single cheap metadata call.
_TARBALL_SCAN = 20000


def _parse_tarball_stem(object_name: str) -> tuple[str, str] | None:
    """`<repo>-code[@sha]` (an object's basename, suffix already stripped) → `(repo, sha)`.

    `sha=""` for the floating (no-`@`) pointer. `create-code-tarballs.sh` always writes the SHA-pinned
    copy as a `cp` of the floating manifest ON THE SAME BUILD (`:373`), so the floating name never
    carries a build event distinct from the newest pin — callers building BUILD history skip it (no
    double-count); callers building the Artifacts roll-up still count it (it IS a real, currently-live
    artifact). Returns `None` for a name that isn't a tarball stem at all (the bucket's own
    housekeeping files, e.g. `_refresh_status.json`, `_test_write.txt`).
    """
    stem, _, sha = object_name.partition("@")
    if not stem.endswith("-code"):
        return None
    return stem[: -len("-code")], sha


def _scan_tarball_bucket(cfg: DeploymentApiConfig, scan: int = _TARBALL_SCAN) -> dict[str, tuple[int | None, str]]:
    """One `list_blobs` walk over `code/` → `{tarball_stem: (size_bytes, last_modified)}` for every
    `.manifest.json`, where `size_bytes` is the SIBLING `.tar.gz` object's own size — NEVER the
    manifest's own (unrelated, tiny-JSON) byte count — and is honestly `None` when no matching
    tarball object exists (an orphaned manifest: deleted, or a partial upload).

    Reads ZERO manifest bodies — every field either caller needs (repo, sha, size, timestamp) is
    already on the object's own name/metadata, so this stays a single cheap metadata scan even at
    ~5000 objects, never thousands of small downloads. `commit_sha`/`pyproject_version`/
    `git_status_clean` living inside each manifest's JSON (per the plan's data-feasibility table) are
    NOT read — `BuildFact`/`RegistryImageFact` have no fields for the latter two, and the SHA is
    already in the object name.
    """
    from unified_trading_library import get_storage_client  # noqa: imports-inside-functions

    bucket = f"{_TARBALL_BUCKET_PREFIX}{_project_id(cfg)}"
    client = get_storage_client()
    manifest_modified: dict[str, str] = {}
    tarball_sizes: dict[str, int] = {}
    blobs = client.list_blobs(bucket=bucket, prefix=_TARBALL_PREFIX)
    for blob in islice(blobs, scan):
        name = blob.name.rsplit("/", 1)[-1]
        if name.endswith(".manifest.json"):
            manifest_modified[name[: -len(".manifest.json")]] = blob.last_modified or ""
        elif name.endswith(".tar.gz"):
            tarball_sizes[name[: -len(".tar.gz")]] = blob.size

    return {stem: (tarball_sizes.get(stem), modified) for stem, modified in manifest_modified.items()}


def gcp_tarball_manifest_builds(cfg: DeploymentApiConfig, scan: int = _TARBALL_SCAN) -> list[BuildFact]:
    """The tarball lane's build history — one `BuildFact` per SHA-pinned manifest (the floating
    pointer is skipped: it never represents a build event distinct from the newest pin, see
    `_parse_tarball_stem`).

    Honesty: this bucket only ever records a SUCCESSFUL refresh — a failed one produces no new
    manifest at all, so its absence is not observable here as an explicit `FAILURE` the way Cloud
    Build reports one. Every row is `status="SUCCESS"` for exactly that reason, never a fabricated
    failure rate for what this source structurally cannot see.
    """
    bucket = f"{_TARBALL_BUCKET_PREFIX}{_project_id(cfg)}"
    facts: list[BuildFact] = []
    for stem, (_size, modified) in _scan_tarball_bucket(cfg, scan).items():
        parsed = _parse_tarball_stem(stem)
        if parsed is None:
            continue
        repo, sha = parsed
        if not sha:
            continue  # the floating pointer duplicates the newest pin's build event
        facts.append(
            BuildFact(
                cloud="gcp",
                lane=LANE_TARBALL,
                repo=repo,
                build_id=stem,
                status="SUCCESS",
                trigger="code-tarball-refresh",
                sha=sha[:7],  # match Cloud Build's `_sub_get(...)[:7]` convention for cross-lane (repo, sha) joins
                branch=_TARBALL_BRANCH,
                started_at=modified,
                finished_at=modified,
                produced=f"gs://{bucket}/{_TARBALL_PREFIX}{stem}.tar.gz",  # noqa: gs-uri — display string, config-derived bucket
            )
        )
    return facts


def gcp_tarball_manifest_images(cfg: DeploymentApiConfig, scan: int = _TARBALL_SCAN) -> list[RegistryImageFact]:
    """The tarball lane's Artifacts-view rows — one `RegistryImageFact` per manifest (floating AND
    pinned both count here — unlike the build-history reading, the Artifacts roll-up shows every
    currently-live artifact, and the floating pointer is a real, currently-fetchable tarball).

    `digest=""` always — a tarball carries no Docker digest (honest absence, not a fabricated one);
    `service.images()`'s existing `running_on` cross-ref (keyed on digest) correctly never fires for
    these rows, matching the Health view's already-tracked "no measured git commit" gap.
    """
    facts: list[RegistryImageFact] = []
    for stem, (size, modified) in _scan_tarball_bucket(cfg, scan).items():
        parsed = _parse_tarball_stem(stem)
        if parsed is None:
            continue
        repo, sha = parsed
        facts.append(
            RegistryImageFact(
                cloud="gcp",
                registry=REGISTRY_TARBALL_BUCKET,
                repo=repo,
                digest="",
                tags=[sha[:7]] if sha else ["floating"],
                pushed_at=modified,
                size_bytes=size,
            )
        )
    return facts
