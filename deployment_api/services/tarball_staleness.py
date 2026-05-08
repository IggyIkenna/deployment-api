"""Tarball staleness check + Cloud Build refresh trigger.

Phase 1 of plans/active/deploy_missing_auto_launch_2026_05_07.plan.md.

This is a STANDALONE module — NOT wired into any route yet. Phase 2
will wire it into the auto-launch endpoint as a pre-launch gate that
ensures the freshly-launched VM boots the latest live-defi-rollout
code rather than a stale tarball.

Lifecycle:

    caller -> ensure_fresh(asset_group, latest_commit_timestamp)
                |
                +-> compute_bundle_oldest_mtime(asset_group)
                |       (one GCS HEAD per tarball in the bundle)
                |
                +-> if oldest_mtime >= latest_commit_timestamp:
                |       return RefreshResult(triggered=False, status="FRESH")
                |
                +-> trigger_refresh(asset_group, branch)
                |       (Cloud Build trigger run -> build_id)
                |
                +-> poll_build(build_id, timeout)
                |       (loop until SUCCESS / FAILURE / TIMEOUT)
                |
                +-> return RefreshResult(triggered=True, build_id=..., status=...)

Per CLAUDE.md "VM tarball deployment" SSOT — bundle membership mirrors
the per-asset_group repo lists in
deployment-service/scripts/vm/create-code-tarballs.sh. Drift between
the two is a review-blocking failure (see Phase 1 follow-up: lift the
matrix to a UAC SSOT so both sides read the same registry).

Per CLAUDE.md "no fire-and-forget VM launches" — the refresh polling
honours both the Cloud Build status AND the TARBALLS_REFRESHED event
the wrapper script emits to gs://{pid}-events/events/deployment-
service/.... Phase 2 wires the event-stream bonus check; Phase 1
relies on Cloud Build status alone (sufficient for the staleness
gate).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Protocol, cast

from unified_trading_library.events import log_event

from deployment_api.utils.storage_client import get_storage_client

if TYPE_CHECKING:
    from collections.abc import Sequence

    from unified_trading_library.cloud_interface import StorageClient


logger = logging.getLogger(__name__)


# Default region for Cloud Build trigger lookups. Matches every other
# Cloud Build invocation in deployment-api (see _cloud_builds_trigger.py).
DEFAULT_BUILD_REGION = "asia-northeast1"

# Cloud Build trigger name registered out-of-band (Terraform / gcloud)
# pointing at deployment-service/cloud-build/refresh-tarballs.cloudbuild.yaml.
# Phase 1 ships the YAML; trigger registration is operator-side. Until
# the trigger lands, callers can pass `trigger_name=` to override.
DEFAULT_REFRESH_TRIGGER_NAME = "refresh-tarballs"

# Default branch the trigger clones. Mirrors `active_feature_branch` in
# workspace-manifest.json. Operators bump this in lockstep with every
# branch rotation.
DEFAULT_REFRESH_BRANCH = "live-defi-rollout"

# Cloud Build poll cadence + budget. Matches the YAML's
# `timeout: 1800s` so a stuck build always surfaces as POLL_TIMEOUT
# rather than an indefinite hang.
DEFAULT_POLL_INTERVAL_SECONDS = 10.0
DEFAULT_POLL_TIMEOUT_SECONDS = 1800.0

# Cloud Build status values we treat as terminal. Anything else =
# in-progress (poll continues).
_TERMINAL_STATUSES: frozenset[str] = frozenset(
    {"SUCCESS", "FAILURE", "INTERNAL_ERROR", "TIMEOUT", "CANCELLED", "EXPIRED"}
)


# ---------------------------------------------------------------------------
# Bundle membership — mirrors create-code-tarballs.sh repo lists
# ---------------------------------------------------------------------------
#
# Each entry is the tarball name (the `<repo>-code.tar.gz` artefact) the
# Cloud Build trigger uploads to gs://{bucket}/code/. The CORE bundle is
# always re-tarred regardless of asset_group, so it shows up in every
# entry — the helper checks every member of the bundle and trips on the
# OLDEST mtime.

_CORE_TARBALLS: tuple[str, ...] = (
    "unified-api-contracts-code.tar.gz",
    "unified-trading-library-code.tar.gz",
    "mtds-code.tar.gz",
    "deployment-service-code.tar.gz",
)

# Per-asset_group service tarballs. Mirrors CEFI_REPOS, TRADFI_REPOS,
# DEFI_REPOS, SPORTS_REPOS, PREDICTION_REPOS in
# deployment-service/scripts/vm/create-code-tarballs.sh.
_ASSET_GROUP_TARBALLS: dict[str, tuple[str, ...]] = {
    "CEFI": (
        "instruments-service-code.tar.gz",
        "market-tick-data-service-code.tar.gz",
        "market-data-processing-service-code.tar.gz",
        "features-delta-one-service-code.tar.gz",
        "features-cross-instrument-service-code.tar.gz",
        "features-multi-timeframe-service-code.tar.gz",
        "features-calendar-service-code.tar.gz",
        "ml-training-service-code.tar.gz",
        "ml-inference-service-code.tar.gz",
        "strategy-service-code.tar.gz",
        "execution-service-code.tar.gz",
        "pnl-attribution-service-code.tar.gz",
        "risk-and-exposure-service-code.tar.gz",
        "position-balance-monitor-service-code.tar.gz",
    ),
    "TRADFI": (
        "instruments-service-code.tar.gz",
        "market-tick-data-service-code.tar.gz",
        "market-data-processing-service-code.tar.gz",
        "features-delta-one-service-code.tar.gz",
        "features-cross-instrument-service-code.tar.gz",
        "features-multi-timeframe-service-code.tar.gz",
        "features-calendar-service-code.tar.gz",
        "features-volatility-service-code.tar.gz",
        "ml-training-service-code.tar.gz",
        "ml-inference-service-code.tar.gz",
        "strategy-service-code.tar.gz",
        "execution-service-code.tar.gz",
        "pnl-attribution-service-code.tar.gz",
        "risk-and-exposure-service-code.tar.gz",
        "position-balance-monitor-service-code.tar.gz",
    ),
    "DEFI": (
        "instruments-service-code.tar.gz",
        "market-tick-data-service-code.tar.gz",
        "market-data-processing-service-code.tar.gz",
        "features-onchain-service-code.tar.gz",
        "features-delta-one-service-code.tar.gz",
        "strategy-service-code.tar.gz",
        "execution-service-code.tar.gz",
        "pnl-attribution-service-code.tar.gz",
        "risk-and-exposure-service-code.tar.gz",
        "position-balance-monitor-service-code.tar.gz",
    ),
    "SPORTS": (
        "instruments-service-code.tar.gz",
        "market-tick-data-service-code.tar.gz",
        "market-data-processing-service-code.tar.gz",
        "features-sports-service-code.tar.gz",
        "ml-training-service-code.tar.gz",
        "ml-inference-service-code.tar.gz",
        "strategy-service-code.tar.gz",
        "execution-service-code.tar.gz",
        "pnl-attribution-service-code.tar.gz",
        "risk-and-exposure-service-code.tar.gz",
    ),
    "PREDICTION": (
        "instruments-service-code.tar.gz",
        "market-tick-data-service-code.tar.gz",
        "market-data-processing-service-code.tar.gz",
        "features-cross-instrument-service-code.tar.gz",
        "strategy-service-code.tar.gz",
        "execution-service-code.tar.gz",
        "pnl-attribution-service-code.tar.gz",
        "risk-and-exposure-service-code.tar.gz",
    ),
}


def _bundle_for(asset_group: str) -> tuple[str, ...]:
    """Return CORE + asset_group-specific tarballs.

    Raises:
        TarballStalenessError: if `asset_group` is not in the registry.
    """
    key = asset_group.upper()
    if key == "ALL":
        # Every per-asset_group tarball, deduplicated, plus CORE.
        seen: set[str] = set(_CORE_TARBALLS)
        merged: list[str] = list(_CORE_TARBALLS)
        for group_tarballs in _ASSET_GROUP_TARBALLS.values():
            for tb in group_tarballs:
                if tb not in seen:
                    seen.add(tb)
                    merged.append(tb)
        return tuple(merged)
    group = _ASSET_GROUP_TARBALLS.get(key)
    if group is None:
        raise TarballStalenessError(
            f"Unknown asset_group {asset_group!r}. "
            f"Expected one of: {[*sorted(_ASSET_GROUP_TARBALLS), 'ALL']}"
        )
    return _CORE_TARBALLS + group


# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------


class TarballStalenessError(RuntimeError):
    """Raised on unrecoverable staleness-check / refresh-trigger failures."""


@dataclass(frozen=True)
class RefreshResult:
    """Outcome of `ensure_fresh`. Caller decides what to do per status:

    * ``FRESH`` — no action needed; bundle is already at-or-newer than
      the requested commit.
    * ``REFRESHED`` — Cloud Build kicked off + observed terminal
      ``SUCCESS`` within the poll budget.
    * ``REFRESH_FAILED`` — Cloud Build kicked off + reached terminal
      non-success status.
    * ``POLL_TIMEOUT`` — kicked off but did not reach a terminal status
      within the poll budget. Caller may launch the VM anyway (with the
      stale tarball) or retry.
    * ``STALE_NO_TRIGGER`` — bundle was stale but the caller passed
      ``allow_trigger=False``. Used to gate without auto-firing.
    """

    asset_group: str
    status: str
    triggered: bool
    bundle_oldest_mtime: datetime | None
    latest_commit_timestamp: datetime
    build_id: str | None = None
    log_url: str | None = None


# ---------------------------------------------------------------------------
# Protocols — let unit tests inject in-memory fakes for GCS + Cloud Build
# ---------------------------------------------------------------------------


class _BlobLike(Protocol):
    """Subset of google.cloud.storage.Blob the helper needs."""

    @property
    def updated(self) -> datetime | None: ...

    @property
    def name(self) -> str: ...

    def exists(self) -> bool: ...

    def reload(self) -> None: ...


class _BucketLike(Protocol):
    """Subset of google.cloud.storage.Bucket the helper needs."""

    def blob(self, name: str) -> _BlobLike: ...


class _StorageLike(Protocol):
    """Subset of UCI StorageClient the helper needs."""

    def bucket(self, name: str) -> _BucketLike: ...


class CloudBuildInvoker(Protocol):
    """Phase 1 indirection over the Cloud Build SDK so unit tests can
    swap in a fake without monkeypatching `google.cloud.devtools`.

    Phase 2 wiring uses ``DefaultCloudBuildInvoker`` (below); tests use
    a hand-rolled fake.
    """

    def run_trigger(self, trigger_name: str, branch: str) -> tuple[str, str | None]:
        """Kick the Cloud Build trigger; return ``(build_id, log_url)``."""
        ...

    def get_build_status(self, build_id: str) -> str:
        """Return the build's current status (e.g. ``"WORKING"``, ``"SUCCESS"``)."""
        ...


# ---------------------------------------------------------------------------
# Default Cloud Build invoker — wraps deployment-api's existing helpers
# ---------------------------------------------------------------------------


class DefaultCloudBuildInvoker:
    """Real Cloud Build invocation via `cloudbuild_v1.CloudBuildClient`.

    Mirrors the auth / region wiring used by
    ``deployment_api/routes/_cloud_builds_trigger.py``. Imports are
    deferred to keep test runs that don't exercise this class (the
    typical case) from pulling ``google.cloud.devtools`` into the
    import graph.
    """

    def __init__(self, project_id: str, region: str = DEFAULT_BUILD_REGION) -> None:
        self._project_id = project_id
        self._region = region

    def _build_client(self) -> object:
        # Public alias from _cloud_builds_types — sibling module under
        # deployment_api/routes/. Same package, so cross-import is fine.
        from deployment_api.routes._cloud_builds_types import get_gcp_build_client

        return get_gcp_build_client()

    @staticmethod
    def _cb_module() -> object:
        # Deferred import — `google.cloud.devtools.cloudbuild_v1` has
        # heavy proto deps; pulling it into the import graph during test
        # collection slows pytest start-up.
        from google.cloud.devtools import cloudbuild_v1

        return cloudbuild_v1

    @staticmethod
    def _extract_build_id_from_op_name(op_name: str | None) -> str | None:
        """Parse a build_id out of an operation name.

        Operation names look like
        ``projects/{pid}/locations/{region}/operations/{uuid}``;
        the trailing UUID is the build_id. This is the same fallback
        shape as ``_cloud_builds_trigger._extract_build_id_from_op``;
        we don't need the full BuildOperationMetadata.Unpack() dance
        for the staleness gate.
        """
        if not op_name:
            return None
        parts = op_name.split("/")
        if len(parts) < 2 or parts[-2] != "operations":
            return None
        return parts[-1] or None

    def run_trigger(self, trigger_name: str, branch: str) -> tuple[str, str | None]:
        client = self._build_client()
        cb_module = self._cb_module()
        name = f"projects/{self._project_id}/locations/{self._region}/triggers/{trigger_name}"
        # CloudBuild Python stubs are incomplete (same pattern as
        # deployment_api/routes/_cloud_builds_trigger.py:124+248) — the
        # request constructors and client methods type-check as
        # Unknown. Suppressions match that file's convention.
        run_request = cb_module.RunBuildTriggerRequest(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType, reportAttributeAccessIssue]
            name=name,
            source=cb_module.RepoSource(branch_name=branch),  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue]
        )
        operation = client.run_build_trigger(request=run_request)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType, reportUnknownArgumentType, reportAttributeAccessIssue]
        op_name: str | None = cast(
            str | None,
            getattr(operation, "name", None),  # pyright: ignore[reportUnknownArgumentType]
        )
        build_id = self._extract_build_id_from_op_name(op_name)
        if not build_id:
            raise TarballStalenessError(
                f"Cloud Build trigger {trigger_name!r} returned no build_id "
                f"(op_name={op_name!r}). Verify trigger registration."
            )
        log_url: str | None = cast(
            str | None,
            getattr(operation, "log_url", None),  # pyright: ignore[reportUnknownArgumentType]
        )
        return build_id, log_url

    def get_build_status(self, build_id: str) -> str:
        client = self._build_client()
        cb_module = self._cb_module()
        name = f"projects/{self._project_id}/locations/{self._region}/builds/{build_id}"
        get_request = cb_module.GetBuildRequest(name=name)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType, reportAttributeAccessIssue]
        build = client.get_build(request=get_request)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType, reportUnknownArgumentType, reportAttributeAccessIssue]
        status_obj: object = getattr(build, "status", None)  # pyright: ignore[reportUnknownArgumentType]
        return str(getattr(status_obj, "name", status_obj) or "UNKNOWN")


# ---------------------------------------------------------------------------
# Main checker
# ---------------------------------------------------------------------------


class TarballStalenessChecker:
    """Pre-launch staleness gate for asset_group code tarballs.

    Phase 1 ships the full surface (mtime read, staleness compare,
    Cloud Build trigger kick + poll). Phase 2 wires this into the
    deploy-missing auto-launch endpoint.

    Args:
        project_id: GCP project ID. Used for both the GCS bucket name
            (``deployment-scripts-{project_id}`` by default) and the
            Cloud Build trigger lookup region.
        bucket_name: Override for the tarball bucket. Defaults to
            ``deployment-scripts-{project_id}``.
        invoker: Cloud Build trigger invoker. Defaults to
            ``DefaultCloudBuildInvoker``; tests pass a fake.
        storage: Override the storage client. Defaults to the
            cloud-agnostic UCI client via
            ``deployment_api.utils.storage_client.get_storage_client``.
            Tests pass an in-memory fake.
    """

    def __init__(
        self,
        *,
        project_id: str,
        bucket_name: str | None = None,
        invoker: CloudBuildInvoker | None = None,
        storage: _StorageLike | None = None,
    ) -> None:
        self._project_id = project_id
        self._bucket_name = bucket_name or f"deployment-scripts-{project_id}"
        self._invoker = invoker or DefaultCloudBuildInvoker(project_id=project_id)
        # Late-bind the storage client so the default path doesn't
        # incur a UCI factory call at import time.
        self._storage_override = storage

    def _storage(self) -> _StorageLike:
        if self._storage_override is not None:
            return self._storage_override
        client: StorageClient = get_storage_client(project_id=self._project_id)
        return cast(_StorageLike, client)

    # ------------------------------------------------------------------
    # GCS mtime reads
    # ------------------------------------------------------------------

    def get_tarball_mtime(self, tarball_name: str) -> datetime | None:
        """Return the GCS object's `updated` timestamp, or None if absent.

        Path: ``gs://{bucket}/code/{tarball_name}``.
        """
        bucket = self._storage().bucket(self._bucket_name)
        blob = bucket.blob(f"code/{tarball_name}")
        if not blob.exists():
            return None
        # `exists()` doesn't populate metadata; reload to get .updated.
        blob.reload()
        return blob.updated

    def compute_bundle_oldest_mtime(self, asset_group: str) -> datetime | None:
        """Return the OLDEST mtime across all tarballs in the bundle.

        If any tarball is missing entirely, returns ``None`` — the
        caller should treat a missing tarball as definitionally stale
        (the next launch would fail to start the VM).
        """
        tarballs = _bundle_for(asset_group)
        oldest: datetime | None = None
        for name in tarballs:
            mtime = self.get_tarball_mtime(name)
            if mtime is None:
                logger.warning(
                    "Tarball %r missing from bucket %s — bundle is stale by definition",
                    name,
                    self._bucket_name,
                )
                return None
            if oldest is None or mtime < oldest:
                oldest = mtime
        return oldest

    # ------------------------------------------------------------------
    # Staleness comparison
    # ------------------------------------------------------------------

    def is_stale(
        self,
        asset_group: str,
        latest_commit_timestamp: datetime,
    ) -> bool:
        """Return True iff any tarball in the bundle is older than
        ``latest_commit_timestamp``, OR any tarball is missing.

        ``latest_commit_timestamp`` should be the committer-date of the
        HEAD commit on ``live-defi-rollout`` at decision time. Caller
        is responsible for sourcing it (typically via the GitHub API
        ``GET /repos/{org}/{repo}/commits/live-defi-rollout`` per the
        Phase 2 wire-in contract).
        """
        oldest = self.compute_bundle_oldest_mtime(asset_group)
        if oldest is None:
            return True
        # Both sides MUST be tz-aware for the compare. UTL's GCS Blob
        # `.updated` is tz-aware UTC; reject naive ``latest_commit_timestamp``
        # to avoid silently producing wrong answers.
        if latest_commit_timestamp.tzinfo is None:
            raise TarballStalenessError(
                "latest_commit_timestamp must be tz-aware (UTC); "
                f"got naive datetime {latest_commit_timestamp!r}"
            )
        return oldest < latest_commit_timestamp

    # ------------------------------------------------------------------
    # Cloud Build trigger + poll
    # ------------------------------------------------------------------

    def trigger_refresh(
        self,
        asset_group: str,
        *,
        trigger_name: str = DEFAULT_REFRESH_TRIGGER_NAME,
        branch: str = DEFAULT_REFRESH_BRANCH,
    ) -> tuple[str, str | None]:
        """Kick the Cloud Build trigger that re-bundles ``asset_group``.

        Returns ``(build_id, log_url)``. Validates ``asset_group`` is
        registered before firing; unknown groups raise.
        """
        # Validate asset_group up-front. ALL is allowed for full-fleet
        # refreshes from Phase 2 (e.g. operator-side smoke).
        _bundle_for(asset_group)
        log_event(
            "TARBALLS_REFRESH_TRIGGERED",
            severity="INFO",
            details={
                "asset_group": asset_group.upper(),
                "trigger_name": trigger_name,
                "branch": branch,
            },
        )
        return self._invoker.run_trigger(trigger_name=trigger_name, branch=branch)

    def poll_build(
        self,
        build_id: str,
        *,
        timeout_seconds: float = DEFAULT_POLL_TIMEOUT_SECONDS,
        interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
    ) -> str:
        """Poll the Cloud Build until terminal status or budget exhausted.

        Returns the final status string (one of ``SUCCESS`` / ``FAILURE``
        / ``INTERNAL_ERROR`` / ``TIMEOUT`` / ``CANCELLED`` / ``EXPIRED``
        / ``POLL_TIMEOUT``).
        """
        deadline = time.monotonic() + timeout_seconds
        last_status = "UNKNOWN"
        while time.monotonic() < deadline:
            last_status = self._invoker.get_build_status(build_id)
            if last_status in _TERMINAL_STATUSES:
                return last_status
            time.sleep(interval_seconds)
        logger.warning(
            "poll_build budget exhausted: build_id=%s last_status=%s",
            build_id,
            last_status,
        )
        return "POLL_TIMEOUT"

    # ------------------------------------------------------------------
    # Top-level orchestration
    # ------------------------------------------------------------------

    def ensure_fresh(
        self,
        asset_group: str,
        latest_commit_timestamp: datetime,
        *,
        trigger_name: str = DEFAULT_REFRESH_TRIGGER_NAME,
        branch: str = DEFAULT_REFRESH_BRANCH,
        allow_trigger: bool = True,
        poll_timeout_seconds: float = DEFAULT_POLL_TIMEOUT_SECONDS,
        poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
    ) -> RefreshResult:
        """End-to-end: check staleness, optionally refresh + wait."""
        oldest = self.compute_bundle_oldest_mtime(asset_group)
        if latest_commit_timestamp.tzinfo is None:
            raise TarballStalenessError(
                "latest_commit_timestamp must be tz-aware (UTC); "
                f"got naive datetime {latest_commit_timestamp!r}"
            )
        if oldest is not None and oldest >= latest_commit_timestamp:
            return RefreshResult(
                asset_group=asset_group.upper(),
                status="FRESH",
                triggered=False,
                bundle_oldest_mtime=oldest,
                latest_commit_timestamp=latest_commit_timestamp,
            )
        # Bundle is stale (or missing). If the caller didn't grant
        # auto-trigger permission, surface the gap without firing.
        if not allow_trigger:
            return RefreshResult(
                asset_group=asset_group.upper(),
                status="STALE_NO_TRIGGER",
                triggered=False,
                bundle_oldest_mtime=oldest,
                latest_commit_timestamp=latest_commit_timestamp,
            )
        build_id, log_url = self.trigger_refresh(
            asset_group, trigger_name=trigger_name, branch=branch
        )
        terminal_status = self.poll_build(
            build_id,
            timeout_seconds=poll_timeout_seconds,
            interval_seconds=poll_interval_seconds,
        )
        if terminal_status == "SUCCESS":
            log_event(
                "TARBALLS_REFRESH_OBSERVED_SUCCESS",
                severity="INFO",
                details={
                    "asset_group": asset_group.upper(),
                    "build_id": build_id,
                },
            )
            return RefreshResult(
                asset_group=asset_group.upper(),
                status="REFRESHED",
                triggered=True,
                bundle_oldest_mtime=oldest,
                latest_commit_timestamp=latest_commit_timestamp,
                build_id=build_id,
                log_url=log_url,
            )
        if terminal_status == "POLL_TIMEOUT":
            log_event(
                "TARBALLS_REFRESH_POLL_TIMEOUT",
                severity="WARNING",
                details={
                    "asset_group": asset_group.upper(),
                    "build_id": build_id,
                    "timeout_seconds": str(poll_timeout_seconds),
                },
            )
            return RefreshResult(
                asset_group=asset_group.upper(),
                status="POLL_TIMEOUT",
                triggered=True,
                bundle_oldest_mtime=oldest,
                latest_commit_timestamp=latest_commit_timestamp,
                build_id=build_id,
                log_url=log_url,
            )
        # Any other terminal status = failure (FAILURE / INTERNAL_ERROR
        # / TIMEOUT / CANCELLED / EXPIRED).
        log_event(
            "TARBALLS_REFRESH_FAILED",
            severity="ERROR",
            details={
                "asset_group": asset_group.upper(),
                "build_id": build_id,
                "terminal_status": terminal_status,
            },
        )
        return RefreshResult(
            asset_group=asset_group.upper(),
            status="REFRESH_FAILED",
            triggered=True,
            bundle_oldest_mtime=oldest,
            latest_commit_timestamp=latest_commit_timestamp,
            build_id=build_id,
            log_url=log_url,
        )


# ---------------------------------------------------------------------------
# Public exports
# ---------------------------------------------------------------------------


def list_known_asset_groups() -> Sequence[str]:
    """Enumerate asset_groups with a registered tarball bundle.

    Phase 2 wire-in uses this to render a 400 with a helpful message
    when an unknown asset_group hits the auto-launch endpoint.
    """
    return [*sorted(_ASSET_GROUP_TARBALLS), "ALL"]


__all__ = [
    "DEFAULT_BUILD_REGION",
    "DEFAULT_POLL_INTERVAL_SECONDS",
    "DEFAULT_POLL_TIMEOUT_SECONDS",
    "DEFAULT_REFRESH_BRANCH",
    "DEFAULT_REFRESH_TRIGGER_NAME",
    "CloudBuildInvoker",
    "DefaultCloudBuildInvoker",
    "RefreshResult",
    "TarballStalenessChecker",
    "TarballStalenessError",
    "list_known_asset_groups",
]
