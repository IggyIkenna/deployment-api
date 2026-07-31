"""Deployment-inventory HTTP routes — the FastAPI endpoints + their date-range/filter query logic.

Split from ``routes/deployments_inventory.py`` (pure code motion; plan:
``deployment_api_qg_size_gate_debt_2026_07_30.md``). Routes register on the package facade's
shared ``router``; patched module-level collaborators (``_cfg`` / ``resolve_run_log_location`` /
``read_run_log_tail`` / ``generate_download_url`` / ``list_job_executions`` /
``list_gcp_region_names`` / ``object_delta_for_asset_group`` / ``_load_registry_entries_for_date_range``
— the census "seams" ``tests/mocks.py``'s ``patch_inventory_secondary_census`` documents) are
resolved through the facade module (``_inv``) at call time so the existing test patch surface
``deployment_api.routes.deployments_inventory.<name>`` keeps intercepting.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi import HTTPException, Query
from pydantic import BaseModel
from unified_api_contracts import DeploymentCloud, DeploymentKind, DeploymentUmbrella
from unified_trading_library import split_gcs_uri

import deployment_api.routes.deployments_inventory as _inv
from deployment_api.routes._cloud_run_executions import DEFAULT_CLOUD_RUN_REGION
from deployment_api.routes.deployments_inventory import (
    _CONFIGURED_GCP_REGIONS,  # pyright: ignore[reportPrivateUsage]
    _DEFAULT_GCP_REGION,  # pyright: ignore[reportPrivateUsage]
    DeploymentDetailResponse,
    DeploymentInventoryResponse,
    DeploymentItem,
    UmbrellaStatusFailure,
    UmbrellaSummaryResponse,
    _vm_entry_by_name_cache,  # pyright: ignore[reportPrivateUsage]
    _vm_entry_by_name_lock,  # pyright: ignore[reportPrivateUsage]
    router,
)
from deployment_api.routes.deployments_inventory._aggregation import (
    _load_inventory,  # pyright: ignore[reportPrivateUsage]
)
from deployment_api.routes.deployments_inventory._classification import (
    _parse_iso,  # pyright: ignore[reportPrivateUsage]
    _vm_item,  # pyright: ignore[reportPrivateUsage]
)
from deployment_api.routes.deployments_inventory._registry_io import (
    _archive_floor_date,  # pyright: ignore[reportPrivateUsage]
)

__all__ = [
    "DeploymentRegionsResponse",
    "RunLogDownloadResponse",
    "RunLogMetadataResponse",
    "RunLogTailResponse",
    "_apply_date_range",
    "_counts_by_kind",
    "_filter_items",
    "_job_object_delta",
    "_job_run_history",
    "_normalize_region_scope",
    "_parse_date_query",
    "_single_timestamp_overlaps",
    "_vm_overlap_basis",
    "build_umbrella_summary",
    "get_deployment_detail",
    "get_deployment_inventory",
    "get_deployment_regions",
    "get_run_log_download",
    "get_run_log_metadata",
    "get_run_log_tail",
    "get_umbrella_summary",
]

# Heartbeat-staleness threshold for the date-range overlap formula — the SAME constant
# ``DeploymentsRegistry.reap_stale`` uses (``max_age_hours: int = 6``,
# unified_trading_library/deployment_registry.py). Reusing it keeps "still running" honest for
# overlap math: the 2026-07-20 audit found 219 registry rows reading ``status=running`` while only
# 12 GCE instances were actually RUNNING — a naive ``completed_at is None -> still open`` overlap
# test would badly overcount those heartbeat-stale zombies as live for every date range.
_REAP_STALE_HOURS = 6


def _parse_date_query(raw: str | None, *, end_of_day: bool) -> datetime | None:
    """Parse a ``date_from``/``date_to`` query value to a UTC datetime; 400s on a bad value.

    Accepts a bare ``YYYY-MM-DD`` (anchored to 00:00:00, or 23:59:59.999999 when ``end_of_day`` so a
    single-day range ``date_from == date_to`` is still inclusive) or a full ISO-8601 instant (used
    exactly as given).
    """
    if not raw:
        return None
    parsed = _parse_iso(raw)
    if parsed is None:
        raise HTTPException(status_code=400, detail=f"invalid date {raw!r} — expected YYYY-MM-DD or ISO-8601")
    if end_of_day and len(raw) == 10:  # bare date, no time component
        return parsed.replace(hour=23, minute=59, second=59, microsecond=999999)
    return parsed


def _vm_overlap_basis(
    *,
    started_at: datetime | None,
    completed_at: datetime | None,
    last_heartbeat_at: datetime | None,
    now: datetime,
    date_from: datetime | None,
    date_to: datetime | None,
) -> tuple[bool, str | None]:
    """VM/registry overlap test against ``[date_from, date_to]`` (WS-2 date-range filter).

    ``effective_end`` = ``completed_at`` when the row is terminal; else ``last_heartbeat_at`` once
    the heartbeat is stale (``>_REAP_STALE_HOURS``); else the row is truly live and open-ended
    (always overlaps). A heartbeat-derived ``effective_end`` is honestly approximate, so the caller
    gets ``basis="approx"`` back to stamp on the row (decision 4 — colour-only, no text label).
    ``started_at is None`` (no interval data at all) never filters the row OUT — honest-absence, it
    simply isn't date-range-scoped.
    """
    if date_from is None and date_to is None:
        return True, None
    if started_at is None:
        return True, None
    if date_to is not None and started_at > date_to:
        return False, None
    if completed_at is not None:
        return (date_from is None or completed_at >= date_from), None
    if last_heartbeat_at is not None and (now - last_heartbeat_at) > timedelta(hours=_REAP_STALE_HOURS):
        return (date_from is None or last_heartbeat_at >= date_from), "approx"
    return True, None  # open-ended — truly live, always overlaps


# Kinds with only ONE observable timestamp — no true start/end interval exists (WS-2 decision:
# match on it anyway, honestly marked approx). CLOUD_RUN_JOB covers BOTH the GCP Cloud Run job
# builder and the AWS Batch (Fargate) builder — they share this wire kind (see ``_aws_deployments``
# ``_batch_item``). SCHEDULER is Cloud Scheduler's single fire time (``last_attempt_at``).
_SINGLE_TIMESTAMP_KINDS = frozenset({DeploymentKind.CLOUD_RUN_JOB.value, "SCHEDULER"})


def _single_timestamp_overlaps(
    last_run_at: datetime | None, date_from: datetime | None, date_to: datetime | None
) -> tuple[bool, str | None]:
    """Point-in-range test for a kind with only ONE observable timestamp, not a true interval:
    an unmanaged VM's creation time, a Cloud Run/AWS Batch job's last run, or a Cloud Scheduler
    job's last fire (``item.last_run_at`` in every case — the field the respective row builders
    already populate it into). The single point stands in for the whole interval, so a MATCH is
    always ``basis="approx"`` — never claimed authoritative. ``last_run_at is None`` (no signal at
    all) is never filtered out — honest-absence, same convention as ``_vm_overlap_basis``.
    """
    if date_from is None and date_to is None:
        return True, None
    if last_run_at is None:
        return True, None
    if date_from is not None and last_run_at < date_from:
        return False, "approx"
    if date_to is not None and last_run_at > date_to:
        return False, "approx"
    return True, "approx"


def _apply_date_range(
    items: list[DeploymentItem],
    now: datetime,
    date_from: datetime | None,
    date_to: datetime | None,
) -> list[DeploymentItem]:
    """Scope registry-backed + single-timestamp rows to ``[date_from, date_to]`` (WS-2 overlap query).

    VM rows with a real registry interval (``started_at`` set) use ``_vm_overlap_basis``. VM rows
    with NO interval (unmanaged/AWS-EC2 — no registry entry to source one from) fall back to the
    single-timestamp match on ``last_run_at``, same as Cloud Run jobs/AWS Batch/Scheduler
    (``_SINGLE_TIMESTAMP_KINDS``). Every other kind (services, functions, disks, ...) passes through
    unfiltered — they carry no timestamp signal at all to scope on. Never mutates a cached
    ``DeploymentItem`` in place: ``_inventory_cache`` entries are shared across concurrent requests
    with different date ranges, so a stamped ``basis`` must land on a COPY, not the cached object.
    """
    if date_from is None and date_to is None:
        return items
    kept: list[DeploymentItem] = []
    for item in items:
        started_at = _parse_iso(item.started_at) if item.kind == DeploymentKind.VM.value else None
        if started_at is not None:
            overlaps, basis = _vm_overlap_basis(
                started_at=started_at,
                completed_at=_parse_iso(item.completed_at),
                last_heartbeat_at=_parse_iso(item.last_heartbeat_at),
                now=now,
                date_from=date_from,
                date_to=date_to,
            )
        elif item.kind == DeploymentKind.VM.value or item.kind in _SINGLE_TIMESTAMP_KINDS:
            overlaps, basis = _single_timestamp_overlaps(_parse_iso(item.last_run_at), date_from, date_to)
        else:
            kept.append(item)
            continue
        if not overlaps:
            continue
        kept.append(item.model_copy(update={"basis": basis}) if basis else item)
    return kept


def _filter_items(
    items: list[DeploymentItem],
    *,
    umbrella: str | None,
    cloud: str | None,
    service: str | None,
    asset_group: str | None,
    status: str | None,
    kind: str | None = None,
) -> list[DeploymentItem]:
    """Apply the inventory query filters (case-insensitive on the enum axes)."""

    def _keep(item: DeploymentItem) -> bool:
        if umbrella and item.umbrella.upper() != umbrella.upper():
            return False
        if cloud and item.cloud.upper() != cloud.upper():
            return False
        if service and item.service != service:
            return False
        if asset_group and item.asset_group != asset_group:
            return False
        if status and item.status != status:
            return False
        return not (kind and item.kind.upper() != kind.upper())

    return [item for item in items if _keep(item)]


_VALID_UMBRELLAS = frozenset(u.value for u in DeploymentUmbrella)
_VALID_CLOUDS = frozenset(c.value for c in DeploymentCloud)


def _normalize_region_scope(region: str | None, all_regions: bool) -> str:
    """Map the ``region`` (and legacy ``all_regions``) query params to an internal census scope token:
    ``""`` the configured default, ``"ALL"`` the every-region sweep, or a specific GCP region name.

    The default region resolves to ``""`` so it stays identical to the configured default census —
    asia-northeast1 GCP + ap-northeast-1 AWS, both Tokyo, where the orchestrator + VMs actually run."""
    value = (region or "").strip().lower()
    if all_regions or value == "all":
        return "ALL"
    if not value or value == _DEFAULT_GCP_REGION:
        return ""
    return value


def _counts_by_kind(items: list[DeploymentItem]) -> dict[str, int]:
    """Per-kind row counts, one key per kind actually present — never a zero-filled 6-key map.

    A kind absent from ``items`` (its census hasn't shipped yet, or failed this cycle) is simply
    absent from the map — honest degradation, not a fabricated 0 masquerading as "censused, found
    none".
    """
    counts: dict[str, int] = {}
    for item in items:
        counts[item.kind] = counts.get(item.kind, 0) + 1
    return counts


@router.get("/deployments/inventory", response_model=DeploymentInventoryResponse)
def get_deployment_inventory(
    umbrella: str | None = Query(None, description="live|batch|paper|experiment (case-insensitive)"),
    cloud: str | None = Query(None, description="gcp|aws (case-insensitive)"),
    service: str | None = Query(None, description="Exact service stem filter"),
    asset_group: str | None = Query(None, description="cefi|defi|tradfi|sports|prediction"),
    status: str | None = Query(None, description="Exact status filter (running|succeeded|failed|stale|...)"),
    kind: str | None = Query(
        None,
        description="VM|CLOUD_RUN_JOB|CLOUD_RUN_SERVICE|ECS_SERVICE|LAMBDA|CLOUD_FUNCTION (case-insensitive)",
    ),
    all_regions: bool = Query(
        False,
        description="Sweep EVERY region (a one-off surprise-check) instead of the configured region set. "
        "Off by default (the census stays on the configured regions for determinism).",
    ),
    region: str | None = Query(
        None,
        description="GCP region to census (e.g. asia-northeast1, europe-west1). Empty or the default "
        "region = the configured default; 'all' sweeps every region. AWS is scoped to the region's "
        "geographic equivalent. Only regional resources (Cloud Run / functions / scheduler) honour "
        "this — VMs / disks / IPs are all-region aggregated regardless.",
    ),
    date_from: str | None = Query(
        None,
        description="Scope VM/registry rows to those overlapping [date_from, date_to] (YYYY-MM-DD or "
        "ISO-8601). VM overlap = started_at <= date_to AND effective_end >= date_from, where "
        "effective_end is completed_at, or last_heartbeat_at once heartbeat-stale (>6h), or "
        "open-ended while truly live. Other kinds (no registry interval) are unaffected.",
    ),
    date_to: str | None = Query(
        None,
        description="See date_from. A bare date is inclusive of its whole day.",
    ),
) -> DeploymentInventoryResponse:
    """Unified deployment inventory: every VM + Cloud Run job, classified by umbrella.

    GCP **and** AWS (Phase 5 parity) — AWS EC2 backfill VMs + Batch Fargate jobs ride
    the same ``DeploymentItem`` contract with ``cloud=AWS``. Each item carries its
    umbrella/cloud/service/asset_group classification + live status / last-run /
    exit_code / heartbeat / captured-progress. ``all_regions=true`` sweeps every region
    (the periodic surprise-check); the default censuses the configured region set.
    """
    now = datetime.now(UTC)
    region_scope = _normalize_region_scope(region, all_regions)
    parsed_date_from = _parse_date_query(date_from, end_of_day=False)
    parsed_date_to = _parse_date_query(date_to, end_of_day=True)
    items = _load_inventory(now, cloud=cloud, region_scope=region_scope)

    archive_floor_str: str | None = None
    out_of_range = False
    if parsed_date_from is not None or parsed_date_to is not None:
        floor_date = _archive_floor_date(now)
        archive_floor_str = floor_date.isoformat()
        out_of_range = parsed_date_from is not None and parsed_date_from.date() < floor_date
        # Bypass the default 7-day archive cap for THIS date-range request only — a bounded,
        # day-partitioned read of exactly the requested range (never the whole 30-day corpus
        # unless asked), merged in alongside any VM already present from the cached census
        # (never re-added, never double-counted). Never runs in mock mode / cloud=aws-only.
        if not _inv._cfg.is_mock_mode() and (cloud is None or cloud.upper() == DeploymentCloud.GCP.value):  # pyright: ignore[reportPrivateUsage]
            range_entries, _out_of_range = _inv._load_registry_entries_for_date_range(  # pyright: ignore[reportPrivateUsage]
                now, parsed_date_from, parsed_date_to
            )
            existing_vm_names = {item.name for item in items if item.kind == DeploymentKind.VM.value}
            items = items + [_vm_item(entry, now) for entry in range_entries if entry.vm_name not in existing_vm_names]

    items = _apply_date_range(items, now, parsed_date_from, parsed_date_to)
    filtered = _filter_items(
        items,
        umbrella=umbrella,
        cloud=cloud,
        service=service,
        asset_group=asset_group,
        status=status,
        kind=kind,
    )
    vm_count = sum(1 for i in filtered if i.kind == DeploymentKind.VM.value)
    job_count = sum(1 for i in filtered if i.kind == DeploymentKind.CLOUD_RUN_JOB.value)
    return DeploymentInventoryResponse(
        items=filtered,
        total=len(filtered),
        vm_count=vm_count,
        cloud_run_job_count=job_count,
        counts_by_kind=_counts_by_kind(filtered),
        archive_floor=archive_floor_str,
        date_range_out_of_range=out_of_range,
    )


class DeploymentRegionsResponse(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Region options for the cockpit's region selector (WS-D region reconciliation).

    ``default`` is the region the selector opens on; ``regions`` is the selectable list (default first);
    ``all_value`` is the sentinel a caller passes as ``?region=`` to sweep every region.
    """

    default: str
    regions: list[str]
    all_value: str


@router.get("/deployments/regions", response_model=DeploymentRegionsResponse)
def get_deployment_regions() -> DeploymentRegionsResponse:
    """Dynamic region options for the region selector: the default region first, then every GCP compute
    region (so a region shows up the moment infra lands there), plus the ``all`` sweep sentinel.

    A cheap region-list metadata read; degrades to the configured default set if the read fails, so the
    selector always has at least its default entry.
    """
    if _inv._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
        names: list[str] = ["asia-northeast1", "europe-west1", "europe-west3", "us-central1", "us-east1"]
    else:
        listed = _inv.list_gcp_region_names(_inv._cfg.require_gcp_project_id())  # pyright: ignore[reportPrivateUsage]
        names = list(listed) if listed else list(_CONFIGURED_GCP_REGIONS)
    # Default region pinned first, the remainder alphabetical + de-duplicated.
    ordered = [_DEFAULT_GCP_REGION, *sorted(n for n in dict.fromkeys(names) if n and n != _DEFAULT_GCP_REGION)]
    return DeploymentRegionsResponse(default=_DEFAULT_GCP_REGION, regions=ordered, all_value="all")


def _job_run_history(item: DeploymentItem) -> list[dict[str, str | float | None]]:
    """Last-N Cloud Run job executions for the detail popover (#11).

    Empty for any non-GCP-Cloud-Run-job kind, mock mode, or on any error (the popover simply shows
    no history). Fetched on the detail path only — ``list_job_executions`` uses ``page_size=10``; the
    thin-list census stays at 1, so this adds no cost to the list endpoint.
    """
    if item.kind != DeploymentKind.CLOUD_RUN_JOB.value or item.cloud != DeploymentCloud.GCP.value:
        return []
    if _inv._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
        return []
    try:
        project_id = _inv._cfg.require_gcp_project_id()  # pyright: ignore[reportPrivateUsage]
    except (HTTPException, ValueError, RuntimeError):
        return []
    records = _inv.list_job_executions(project_id, item.name, region=item.region or DEFAULT_CLOUD_RUN_REGION)
    return [
        {
            "name": record.name,
            "status": record.status,
            "started_at": record.started_at,
            "completed_at": record.completed_at,
            "duration_seconds": record.duration_seconds,
        }
        for record in records
    ]


def _job_object_delta(item: DeploymentItem, now: datetime) -> int | None:
    """Rows since the last manifest snapshot for a Cloud Run job's asset_group — the #12 "rows since
    last run" HINT.

    A link + hint only: the AUTHORITATIVE "did the run produce its data" verdict lives on the
    consolidator page (``consolidator_throughput_backlog_monitor`` plan), keyed by the full job
    short-name; this deployments popover only helps spot a fired-but-produced-nothing job. Reuses the
    same ``object_delta_for_asset_group`` the census batches (which catches its own errors → None);
    None for non-jobs / no asset_group / mock.
    """
    if item.kind != DeploymentKind.CLOUD_RUN_JOB.value or not item.asset_group:
        return None
    if _inv._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
        return None
    return _inv.object_delta_for_asset_group(item.asset_group, now)[0]


@router.get("/deployments/{name}/detail", response_model=DeploymentDetailResponse)
def get_deployment_detail(name: str) -> DeploymentDetailResponse:
    """Per-target drill-down: the thin-list item plus the D.1 metrics vector (popover).

    ``name`` is the ``DeploymentItem.name`` (VM name / Cloud Run job or service name), not
    an orchestration ``deployment_id`` — this endpoint reads the SAME cached census
    ``/deployments/inventory`` already computes (no new bucket walk). A Cloud Run job additionally
    gets its last-N run-history (``page_size=10`` on the detail path only). 404 if the name isn't in
    the current (cached) inventory.
    """
    now = datetime.now(UTC)
    items = _load_inventory(now)
    item = next((i for i in items if i.name == name), None)
    if item is None:
        raise HTTPException(status_code=404, detail=f"Deployment {name!r} not found in the current inventory")
    run_history = _job_run_history(item)
    object_delta = _job_object_delta(item, now)
    with _vm_entry_by_name_lock:
        entry = _vm_entry_by_name_cache.get(name)
    if entry is None:
        return DeploymentDetailResponse(item=item, run_history=run_history, object_delta=object_delta)
    return DeploymentDetailResponse(
        item=item,
        cpu_pct=entry.cpu_pct,
        mem_pct=entry.mem_pct,
        mem_slope=entry.mem_slope,
        disk_pct=entry.disk_pct,
        io_write_rate_bytes_sec=entry.io_write_rate_bytes_sec,
        net_recv_rate_bytes_sec=entry.net_recv_rate_bytes_sec,
        workload_alive=entry.workload_alive,
        host_metrics_window=entry.host_metrics_window,
        run_history=run_history,
        object_delta=object_delta,
    )


class RunLogMetadataResponse(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Size + last-modified for a VM's run.log — resolved live-first, archive-fallback."""

    name: str
    exists: bool
    location: str | None = None  # "live" | "archive"; None when no log object exists anywhere
    uri: str = ""
    size_bytes: int | None = None
    last_modified: str | None = None


@router.get("/deployments/{name}/run-log/metadata", response_model=RunLogMetadataResponse)
def get_run_log_metadata(name: str) -> RunLogMetadataResponse:
    """Size + last-modified for ``name``'s run.log — live-first, archive-fallback (WS-4 decision 2).

    Tries ``vm-logs/{name}/run.log`` first (regardless of ``completed_at``); on miss, falls back to
    the durable final-snapshot archive path. ``location`` tells the UI which one resolved so it can
    label the panel ("showing archive copy" vs live). ``exists=False`` means neither path has an
    object yet (e.g. a VM that completed before the final-snapshot writer shipped) — an honest
    "no log available" state, never a silent blank panel.
    """
    if _inv._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
        return RunLogMetadataResponse(
            name=name,
            exists=True,
            location="live",
            uri=f"gs://deployment-scripts-mock/vm-logs/{name}/run.log",  # noqa: gs-uri (mock fixture URI)
            size_bytes=842_331,
            last_modified="2026-07-21T04:00:00Z",
        )
    project_id = _inv._cfg.require_gcp_project_id()  # pyright: ignore[reportPrivateUsage]
    resolved = _inv.resolve_run_log_location(name, project_id)
    if resolved.metadata is None:
        return RunLogMetadataResponse(name=name, exists=False)
    return RunLogMetadataResponse(
        name=name,
        exists=True,
        location=resolved.location,
        uri=resolved.uri,
        size_bytes=resolved.metadata.size,
        last_modified=resolved.metadata.last_modified,
    )


class RunLogTailResponse(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Bounded tail of a VM's run.log — capped lines from a capped byte-range read."""

    name: str
    exists: bool
    location: str | None = None  # "live" | "archive"; None when no log object exists anywhere
    uri: str = ""
    size_bytes: int | None = None
    last_modified: str | None = None
    lines: list[str] = []
    line_count: int = 0
    tail_bytes: int = 0  # actual bytes read from GCS for this tail (<= configured max_bytes)


@router.get("/deployments/{name}/run-log/tail", response_model=RunLogTailResponse)
def get_run_log_tail(name: str, lines: int | None = None) -> RunLogTailResponse:
    """Bounded tail of ``name``'s run.log — byte-range read of only the last
    ``DeploymentApiConfig.run_log_tail_max_bytes`` (default 256KB), split to the last
    ``lines`` (clamped to ``run_log_tail_max_lines``, default 300). Never loads the full
    object into API memory or the response — the GCS read itself is capped at
    ``run_log_tail_max_bytes`` regardless of the object's real size (observed up to
    13.4MB; 20-30MB is a plausible worst case).
    """
    line_cap = _inv._cfg.run_log_tail_max_lines  # pyright: ignore[reportPrivateUsage]
    max_lines = line_cap if lines is None else max(1, min(lines, line_cap))
    if _inv._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
        mock_lines = [f"[mock] run.log line {i}" for i in range(max_lines)]
        return RunLogTailResponse(
            name=name,
            exists=True,
            location="live",
            uri=f"gs://deployment-scripts-mock/vm-logs/{name}/run.log",  # noqa: gs-uri (mock fixture URI)
            size_bytes=842_331,
            last_modified="2026-07-21T04:00:00Z",
            lines=mock_lines,
            line_count=len(mock_lines),
            tail_bytes=sum(len(line.encode("utf-8")) + 1 for line in mock_lines),
        )
    project_id = _inv._cfg.require_gcp_project_id()  # pyright: ignore[reportPrivateUsage]
    resolved = _inv.resolve_run_log_location(name, project_id)
    if resolved.metadata is None:
        return RunLogTailResponse(name=name, exists=False)
    tail_lines, tail_bytes = _inv.read_run_log_tail(
        resolved.uri,
        resolved.metadata.size,
        max_bytes=_inv._cfg.run_log_tail_max_bytes,  # pyright: ignore[reportPrivateUsage]
        max_lines=max_lines,
    )
    return RunLogTailResponse(
        name=name,
        exists=True,
        location=resolved.location,
        uri=resolved.uri,
        size_bytes=resolved.metadata.size,
        last_modified=resolved.metadata.last_modified,
        lines=tail_lines,
        line_count=len(tail_lines),
        tail_bytes=tail_bytes,
    )


class RunLogDownloadResponse(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    """Short-lived signed URL for a VM's run.log — client downloads directly from GCS."""

    name: str
    exists: bool
    location: str | None = None  # "live" | "archive"; None when no log object exists anywhere
    download_url: str = ""
    expires_in_seconds: int = 0


@router.get("/deployments/{name}/run-log/download", response_model=RunLogDownloadResponse)
def get_run_log_download(name: str) -> RunLogDownloadResponse:
    """Signed download URL for ``name``'s run.log (WS-4 decision 4).

    Resolves the object live-first/archive-fallback (same resolution as the metadata/tail
    endpoints), then returns a short-lived signed URL the client downloads directly from
    GCS — the API never streams the object (up to 13.4MB observed, 20-30MB plausible)
    through itself.
    """
    if _inv._cfg.is_mock_mode():  # pyright: ignore[reportPrivateUsage]
        return RunLogDownloadResponse(
            name=name,
            exists=True,
            location="live",
            download_url=f"https://storage.googleapis.com/deployment-scripts-mock/vm-logs/{name}/run.log?mock-signed",
            expires_in_seconds=_inv._cfg.run_log_download_url_expiry_minutes * 60,  # pyright: ignore[reportPrivateUsage]
        )
    project_id = _inv._cfg.require_gcp_project_id()  # pyright: ignore[reportPrivateUsage]
    resolved = _inv.resolve_run_log_location(name, project_id)
    if resolved.metadata is None:
        return RunLogDownloadResponse(name=name, exists=False)
    bucket, object_path = split_gcs_uri(resolved.uri)
    expiry_minutes = _inv._cfg.run_log_download_url_expiry_minutes  # pyright: ignore[reportPrivateUsage]
    url = _inv.generate_download_url(bucket, object_path, expiry_minutes=expiry_minutes)
    return RunLogDownloadResponse(
        name=name,
        exists=True,
        location=resolved.location,
        download_url=url,
        expires_in_seconds=expiry_minutes * 60,
    )


def build_umbrella_summary(umbrella: str, items: list[DeploymentItem]) -> UmbrellaSummaryResponse:
    """Roll the inventory items of one umbrella into the /repos-overview summary."""
    scoped = [i for i in items if i.umbrella.upper() == umbrella.upper()]
    counts: dict[str, int] = {}
    for item in scoped:
        counts[item.status] = counts.get(item.status, 0) + 1
    stale_count = sum(1 for i in scoped if i.status == "stale")
    failures = [i for i in scoped if i.status == "failed"]
    last_failure: UmbrellaStatusFailure | None = None
    if failures:
        # Most-recent failing target by last_run_at (lexicographic ISO sort; None last).
        worst = max(failures, key=lambda i: i.last_run_at or "")
        last_failure = UmbrellaStatusFailure(
            name=worst.name,
            exit_code=worst.exit_code,
            last_run_at=worst.last_run_at,
        )
    return UmbrellaSummaryResponse(
        umbrella=umbrella.upper(),
        total=len(scoped),
        counts_by_status=counts,
        stale_count=stale_count,
        last_failure=last_failure,
    )


@router.get("/deployments/umbrella/{umbrella}/summary", response_model=UmbrellaSummaryResponse)
def get_umbrella_summary(umbrella: str) -> UmbrellaSummaryResponse:
    """Per-umbrella rollup: counts by status, stale count, last failure.

    The /repos-overview equivalent for one umbrella (live|batch|paper|experiment).
    A 404 on an unknown umbrella (the closed set is the UAC ``DeploymentUmbrella``).
    """
    if umbrella.upper() not in _VALID_UMBRELLAS:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown umbrella {umbrella!r}; expected one of {sorted(_VALID_UMBRELLAS)}",
        )
    now = datetime.now(UTC)
    items = _load_inventory(now)
    return build_umbrella_summary(umbrella, items)
