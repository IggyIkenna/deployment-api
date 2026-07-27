"""Normalized artifact-pipeline models — one shape for the /ops/artifacts page's five views.

The whole point of this module: the UI never sees a cloud-native build / registry / runtime schema.
Each provider adapter maps its native rows (Cloud Build, CodeBuild, Artifact Registry, ECR, Cloud
Run revisions, App Runner / ECS operations, GCS tarball manifests) into the internal facts here, and
every API response is built from those. The five views:

  running  — service x artifact VERSION actually live now, with drift flags (the headline view)
  deploys  — the deploy timeline (Cloud Run revisions + App Runner/ECS ops + VM launches)
  builds   — the pipeline (both clouds, both lanes) with structured failure
  images   — the registry inventory (Artifact Registry + ECR + tarball bucket)
  health   — measured pipeline defects, severity-ranked

Honesty rule (the `_image_signal` principle): a value the pipeline genuinely cannot resolve renders
as an explicit "unknown + why", NEVER a fabricated green. An empty `digest` / `built_from` means
exactly "honestly unknown", and the drift flag says so — the single most important invariant of the
whole page (a number that looks precise and isn't is worse than an honest blank).

SSOT: plans/active/artifact_pipeline_observability_2026_07_17.md
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pydantic import BaseModel, Field

# ── Cloud + lane identifiers (the UI colour-keys off these) ────────────────────────────────────────
CLOUD_GCP = "gcp"
CLOUD_AWS = "aws"

LANE_IMAGE = "image"  # Docker image → registry → Cloud Run / App Runner / ECS
LANE_TARBALL = "tarball"  # code tarball → GCS/S3 → a GCE/EC2 VM runs source (a launch IS the deploy)

# The tarball lane's `ImageFact.registry` key (Phase 3d) — distinct from the AR `unified-trading-system`
# key so a tarball "repo" roll-up never collides with an AR repo of the same service name.
REGISTRY_TARBALL_BUCKET = "gcs-tarball-bucket"

# ── Drift classification for a running artifact version (the "What's running" flags) ────────────────
# Ordered strongest-provenance → weakest. A group also carries `fragmented` when >1 version is live.
DRIFT_PINNED = "pinned"  # @sha256 digest — immutable, provable
DRIFT_OK = "ok"  # :sha tag — traceable to a commit, but the tag is mutable
DRIFT_STALE = "stale"  # traceable, but behind the green / sibling version
DRIFT_FLOATING = "floating"  # :latest — each pull may differ from the last
DRIFT_HAND = "hand"  # hand-deployed, bypassing the pipeline
DRIFT_FAKE = "fake"  # fake-populated provenance (the 0.99.0 SETUPTOOLS_SCM_PRETEND_VERSION constant)
DRIFT_UNKNOWN = "unknown"  # honestly unresolvable (e.g. a pre-stamp tarball VM)
DRIFT_FRAGMENTED = "fragmented"  # >1 version of ONE service live at once (group-level flag)

# ── Deploy change-type vs the next-older deploy of the same workload ────────────────────────────────
CHANGE_NEW = "new"  # digest changed — new code shipped
CHANGE_CONFIG = "config"  # same digest redeployed — nothing shipped (the ~40% churn)
CHANGE_ROLLBACK = "rollback"  # digest reverted to an earlier one
CHANGE_FAILED = "failed"  # deploy failed though the build was green

# ── Registry-repo runtime state (the Artifacts view) ────────────────────────────────────────────────
STATE_RUNNING = "running"  # at least one live workload runs an image from this repo
STATE_PAUSED = "paused"  # App Runner PAUSED (AWS intentionally parked)
STATE_ZEROED = "zeroed"  # ECS desired=0 (AWS intentionally parked)
STATE_ACTIVE = "active"  # still pushing, not obviously running a task
STATE_PARKED = "parked"  # AWS deferred, kept for when credits return (NOT a GC candidate)
STATE_LEGACY = "legacy"  # GCP AR legacy / orphaned — a genuine GC candidate
STATE_EMPTY = "empty"  # declared repo with 0 images

# ── Health severity tiers. `deferred` = intentional (AWS parked), NOT a defect ──────────────────────
SEV_HIGH = "high"
SEV_MED = "med"
SEV_LOW = "low"
SEV_DEFERRED = "deferred"


# ══════════════════════════════════════════════════════════════════════════════════════════════════
# Internal normalized facts — flat, primitive-typed rows that snapshot cleanly to parquet and never
# cross the API boundary (the Pydantic response models below do). One dataclass per snapshot-backed
# source; the `running` + `health` views are computed live/derived and go straight to their models.
# ══════════════════════════════════════════════════════════════════════════════════════════════════


@dataclass
class BuildFact:  # CORRECT-LOCAL: internal build record, not a cross-service contract
    """One build from Cloud Build (GCP) or CodeBuild (AWS) — the pipeline row."""

    cloud: str
    lane: str  # image | tarball
    repo: str
    build_id: str
    status: str  # SUCCESS | FAILURE | WORKING | CANCELLED | QUEUED (native, upper-cased)
    trigger: str  # trigger / project name ("" if a webhook fired it)
    sha: str  # git commit ("" = honestly unknown)
    branch: str
    started_at: str  # ISO-8601 UTC ("" if not started)
    finished_at: str = ""
    duration_sec: float | None = None  # None while running / when unknowable
    produced: str = ""  # the artifact it produced (image URI / "wheel" / tarball)
    initiator: str = ""  # who/what triggered it (e.g. GitHub-Hookshot, a user)
    log_url: str = ""
    failure_type: str = ""  # GCP failureInfo.type / an AWS phase name ("" on success)
    failure_detail: str = ""  # the structured failure message ("" on success)
    steps: list[tuple[str, str, float]] = field(default_factory=list)  # (name, status, seconds)


@dataclass
class DeployFact:  # CORRECT-LOCAL: internal deploy event, not a cross-service contract
    """One deploy — a Cloud Run revision, an App Runner/ECS operation, or a VM launch (Lane B)."""

    cloud: str
    workload: str
    revision: str  # "rev 00192" / an op id / the VM name
    digest: str  # resolved image digest ("" for a tarball VM — honestly unknown)
    built_from: str  # git sha the digest joins to ("" if unresolved)
    resolvable: bool  # a digest→SHA join exists (even if not yet computed)
    change_type: str  # new | config | rollback | failed
    at: str  # ISO-8601 UTC deploy time
    held_for: str = ""  # human interval this revision was live before the next replaced it
    live: bool = False  # serving right now
    deployer: str = ""  # serving.knative.dev/creator / an initiator ("human" lit red in the UI)
    link_kind: str = ""  # revision | vm | op — which console link the UI builds
    section: str = ""  # display grouping label (e.g. the workload family)


@dataclass
class ImageFact:  # CORRECT-LOCAL: internal registry-inventory row, not a cross-service contract
    """One registry repo (Artifact Registry or ECR), or a roll-up aggregate row."""

    cloud: str
    registry: str  # "unified-trading-system" (AR) | "ECR"
    repo: str
    image_count: int  # -1 when this is a size-carrying roll-up rather than a real count
    tags: list[str] = field(default_factory=list)  # latest tags on the newest image(s)
    last_pushed: str = ""  # ISO-8601 UTC ("" if unknown)
    running_on: str = ""  # a workload name, or "" if nothing runs it
    state: str = STATE_ACTIVE
    size_bytes: int | None = None  # None when unresolved (GCP AR needs live auth)
    is_aggregate: bool = False  # a synthetic roll-up (e.g. "39 other AR repos ~1.5 TB")
    note: str = ""  # honest caveat for a roll-up / sampled row


@dataclass
class RegistryImageFact:  # CORRECT-LOCAL: internal per-image registry record, not a cross-service contract
    """One concrete pushed image (one digest) in a registry.

    The atom two different views are built from: `images()` aggregates this list per repo into the
    `ImageFact` roll-up rows above; `running()`'s runtime join resolves a live workload's digest
    against this list to recover the tags (and, through a matching `:<sha>` tag, the `BuildFact` that
    produced it) — the digest→tag→SHA→build chain the plan calls "the runtime join".
    """

    cloud: str
    registry: str
    repo: str
    digest: str  # "sha256:…" ("" honestly unknown)
    tags: list[str] = field(default_factory=list)
    pushed_at: str = ""  # ISO-8601 UTC ("" if unknown)
    size_bytes: int | None = None


# ══════════════════════════════════════════════════════════════════════════════════════════════════
# API contract models — what the UI receives. Every response echoes `generated_at`; the two windowed
# views (deploys, builds) also echo the resolved `start_date`/`end_date` (see the cost page: `days`
# alone can't tell which window the numbers describe).
# ══════════════════════════════════════════════════════════════════════════════════════════════════


# ── running ─────────────────────────────────────────────────────────────────────────────────────────
class RunningHost(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    name: str
    kind: str  # "Cloud Run svc" | "Cloud Run job" | "GCE VM" | "App Runner" | "ECS" | "EC2 VM"
    launched_at: str = ""  # ISO-8601 UTC ("" when a Cloud Run job has no persistent host)


class RunningVersion(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    version: str  # the human tag/pin label (":latest", ":<sha>", "@sha256:…", "cohort …")
    artifact: str  # full artifact ref (image URI or tarball path)
    digest: str = ""  # resolved sha256 ("" = unresolved / not a digest pin)
    built_from: str = ""  # git commit this version resolves to ("" = honestly unknown)
    drift: list[str] = Field(default_factory=list)  # DRIFT_* flags
    hosts: list[RunningHost] = Field(default_factory=list)
    why: str = ""  # one-paragraph, honest explanation of the drift verdict


class RunningGroup(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    service: str
    lane: str  # image | tarball
    cloud: str
    fragmented: bool = False  # >1 version of this service live at once
    frag_note: str = ""
    versions: list[RunningVersion] = Field(default_factory=list)


class RunningStats(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    services: int = 0
    versions: int = 0
    fragmented: int = 0  # services running >1 version right now (the headline number)
    floating: int = 0  # versions on a floating :latest
    hand: int = 0  # hand-deployed versions
    unknown: int = 0  # versions with an unresolvable commit


class RunningResponse(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    generated_at: str = ""
    groups: list[RunningGroup] = Field(default_factory=list)
    stats: RunningStats = Field(default_factory=RunningStats)


# ── deploys ───────────────────────────────────────────────────────────────────────────────────────
class DeployRow(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    workload: str
    revision: str
    cloud: str
    digest: str = ""
    built_from: str = ""
    resolvable: bool = False
    change_type: str = CHANGE_NEW  # new | config | rollback | failed
    at: str = ""
    held_for: str = ""
    live: bool = False
    deployer: str = ""
    link_kind: str = ""
    section: str = ""


class DeploysStats(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    total: int = 0
    config_only_pct: float = 0.0  # share that changed nothing (same digest)
    live_now: int = 0
    failed: int = 0


class DeploysResponse(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    days: int
    start_date: str = ""
    end_date: str = ""
    generated_at: str = ""
    rows: list[DeployRow] = Field(default_factory=list)
    stats: DeploysStats = Field(default_factory=DeploysStats)


# ── builds (pipeline) ────────────────────────────────────────────────────────────────────────────
class BuildStep(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    name: str
    status: str  # ok | FAILED | skipped
    seconds: float = 0.0


class BuildRow(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    repo: str
    lane: str
    cloud: str
    status: str  # SUCCESS | FAILURE | WORKING | …
    trigger: str = ""
    sha: str = ""
    branch: str = ""
    started_at: str = ""
    duration: str = ""  # human-formatted ("3m41s") — the raw seconds live on the fact
    produced: str = ""
    build_id: str = ""
    failure: str = ""  # short one-line reason ("" on success)
    failure_type: str = ""
    failure_detail: str = ""
    log_url: str = ""
    dup: bool = False  # same commit built twice (wasted)
    cross_lane: bool = False  # one commit built as image AND tarball (the ⇄ badge)
    steps: list[BuildStep] = Field(default_factory=list)


class BuildsStats(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    total: int = 0
    success_rate: float = 0.0
    failed: int = 0
    median_duration_sec: float | None = None
    wasted_dup: int = 0


class BuildsResponse(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    days: int
    start_date: str = ""
    end_date: str = ""
    generated_at: str = ""
    rows: list[BuildRow] = Field(default_factory=list)
    stats: BuildsStats = Field(default_factory=BuildsStats)


# ── images (artifacts) ───────────────────────────────────────────────────────────────────────────
class ImageRow(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    repo: str
    cloud: str
    registry: str
    image_count: int  # -1 for a roll-up carrying a size instead of a count
    tags: list[str] = Field(default_factory=list)
    last_pushed: str = ""
    running_on: str = ""
    state: str = STATE_ACTIVE
    size_bytes: int | None = None
    is_aggregate: bool = False
    note: str = ""


class ImagesStats(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    total_repos: int = 0
    running: int = 0
    parked: int = 0  # AWS deferred (paused / zeroed / idle) — kept, not GC
    legacy: int = 0  # GCP AR GC candidates
    empty: int = 0


class ImagesResponse(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    generated_at: str = ""
    rows: list[ImageRow] = Field(default_factory=list)
    stats: ImagesStats = Field(default_factory=ImagesStats)


# ── health ──────────────────────────────────────────────────────────────────────────────────────
class HealthCondition(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    condition: str
    severity: str  # high | med | low | deferred
    count: str = ""  # heterogeneous ("2 svc" | "fleet" | "40%" | "13") — a display string
    area: str = ""  # "pipeline · CI" | "deploy · AWS" | …
    tab: str = ""  # which view proves it: running | deploy | pipe | art
    meaning: str = ""  # what it means (may carry inline markup)
    evidence: str = ""  # the measured proof (a command / file:line)


class HealthStats(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    high: int = 0
    med: int = 0
    low: int = 0
    deferred: int = 0
    real_defects: int = 0  # high + med + low (excludes the intentional deferred tier)


class HealthResponse(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    generated_at: str = ""
    conditions: list[HealthCondition] = Field(default_factory=list)
    stats: HealthStats = Field(default_factory=HealthStats)
