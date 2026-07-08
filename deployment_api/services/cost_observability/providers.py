"""Provider adapters — native billing rows → normalized CostRecord.

GCP + AWS go through the UTL analytics wrappers (`execute_query` → list[dict]); no direct
`google.cloud`/`boto3` here. AWS returns every value as a string (Athena VarCharValue), so
costs are coerced. GitHub is a deterministic dummy until a billing PAT lands.
"""

from __future__ import annotations

import logging
from datetime import date

from unified_trading_library import get_analytics_client

# AWSAnalyticsClient is not in the UTL public API, and the factory can't set the CUR's
# us-east-1 region + explicit Athena output bucket (it needs ATHENA_OUTPUT_BUCKET in env) —
# so instantiate the concrete client directly.
from unified_trading_library.cloud_interface.providers.aws import (  # noqa: qg-deep-import
    AWSAnalyticsClient,
)

from deployment_api.services.cost_observability.models import (
    CLOUD_AWS,
    CLOUD_GCP,
    CLOUD_GITHUB,
    KIND_BUCKET,
    KIND_OTHER,
    KIND_VM,
    PURCHASE_ON_DEMAND,
    PURCHASE_OTHER,
    PURCHASE_SPOT,
    CostRecord,
)
from deployment_api.services.cost_observability.queries import aws_facts_sql, gcp_facts_sql

logger = logging.getLogger(__name__)


def _as_float(v: object) -> float:
    if isinstance(v, bool):
        return float(v)
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v) if v.strip() else 0.0
        except ValueError:
            return 0.0
    return 0.0


def _as_str(v: object) -> str:
    if isinstance(v, str):
        return v
    return "" if v is None else str(v)


def _as_int_or_none(v: object) -> int | None:
    """Parse a `system_labels` numeric string (`cores`); missing/blank → None, never 0."""
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str) and v.strip():
        try:
            return int(v)
        except ValueError:
            return None
    return None


def _mib_to_gb(v: object) -> float | None:
    """`system_labels` compute.googleapis.com/memory is MiB (verified live: e2-small → 2048 →
    2 GB; n2-highmem-16 → 131072 → 128 GB)."""
    mib = _as_int_or_none(v)
    return round(mib / 1024, 1) if mib is not None else None


def _short_name(resource_id: str) -> str:
    """GCE/Cloud Run resource names arrive as ``projects/<n>/instances/<name>`` — the last
    path segment is the human-recognizable name (unique within our single project)."""
    return resource_id.rsplit("/", 1)[-1] if "/" in resource_id else resource_id


def _gcp_kind(service: str, resource_id: str) -> str:
    if not resource_id:
        return KIND_OTHER
    if service == "Compute Engine":
        return KIND_VM
    if service == "Cloud Storage":
        return KIND_BUCKET
    return KIND_OTHER


def _aws_kind(service_code: str, resource_id: str) -> str:
    if service_code == "AmazonEC2" and resource_id.startswith("i-"):
        return KIND_VM
    if service_code == "AmazonS3" and resource_id and not resource_id.startswith("i-"):
        return KIND_BUCKET
    return KIND_OTHER


def _purchase_option(cloud: str, sku: str) -> str:
    """Classify a billing row's SKU (GCP) / usage_type (AWS) into spot | on-demand | other.

    Neither export carries a dedicated purchase-option column — both encode it in the SKU/
    usage-type text, so this is a text-pattern derivation, same spirit as `_gcp_kind`/`_aws_kind`.
    "other" covers every non-compute-instance SKU (storage, network, licensing, …) — the
    SPOT-VMs HARD RULE (codex/05-infrastructure/spot-vms-for-backfill.md) only concerns compute
    instances, so this axis is deliberately silent for the rest.
    """
    s = sku.lower()
    if cloud == CLOUD_GCP:
        if "spot" in s or "preemptible" in s:
            return PURCHASE_SPOT
        if "instance core" in s or "instance ram" in s:
            return PURCHASE_ON_DEMAND
        return PURCHASE_OTHER
    if cloud == CLOUD_AWS:
        # EC2 usage_type embeds the purchase mode: "APN1-SpotUsage:c5.xlarge" (spot) vs.
        # "APN1-BoxUsage:c5.xlarge" (on-demand) / "...HeavyUsage.../...DedicatedUsage..."
        # (Reserved/Dedicated — grouped with on-demand here, neither is preemptible/spot).
        if "spotusage" in s:
            return PURCHASE_SPOT
        if "boxusage" in s or "heavyusage" in s or "dedicatedusage" in s:
            return PURCHASE_ON_DEMAND
        return PURCHASE_OTHER
    return PURCHASE_OTHER


def gcp_facts(table: str, start: date, end: date, provisional_cutoff: date) -> list[CostRecord]:
    client = get_analytics_client(provider="gcp")
    rows = client.execute_query(gcp_facts_sql(table, start, end))
    out: list[CostRecord] = []
    for r in rows:
        day = _as_str(r.get("day"))
        service = _as_str(r.get("service")) or "Unknown"
        resource_id = _short_name(_as_str(r.get("resource_id")))
        sku = _as_str(r.get("sku"))
        out.append(
            CostRecord(
                cloud=CLOUD_GCP,
                day=day,
                service=service,
                resource_id=resource_id,
                resource_kind=_gcp_kind(service, resource_id),
                region=_as_str(r.get("region")) or "global",
                cost=_as_float(r.get("cost")),
                credit=_as_float(r.get("credit")),
                sku=sku,
                usage_amount=_as_float(r.get("usage_amount")),
                usage_unit=_as_str(r.get("usage_unit")),
                zone=_as_str(r.get("zone")),
                purchase_option=_purchase_option(CLOUD_GCP, sku),
                machine_type=_as_str(r.get("machine_spec")),
                vcpu=_as_int_or_none(r.get("machine_cores")),
                memory_gb=_mib_to_gb(r.get("machine_memory_mib")),
                is_provisional=_is_provisional(day, provisional_cutoff),
            )
        )
    return out


def aws_facts(
    database: str,
    table: str,
    region: str,
    output_bucket: str,
    start: date,
    end: date,
    provisional_cutoff: date,
) -> list[CostRecord]:
    # CUR lives in us-east-1 with its own results bucket — pin both explicitly; the app's
    # default AWS region (ap-northeast-1) would point Athena at an empty catalog.
    client = AWSAnalyticsClient(region=region, output_bucket=output_bucket)
    rows = client.execute_query(aws_facts_sql(database, table, start, end))
    out: list[CostRecord] = []
    for r in rows:
        day = _as_str(r.get("day"))
        service = _as_str(r.get("service")) or "Unknown"
        service_code = _as_str(r.get("service_code"))
        resource_id = _as_str(r.get("resource_id"))
        usage_type = _as_str(r.get("usage_type"))
        out.append(
            CostRecord(
                cloud=CLOUD_AWS,
                day=day,
                service=service,
                resource_id=resource_id,
                resource_kind=_aws_kind(service_code, resource_id),
                region=_as_str(r.get("region")) or "global",
                cost=_as_float(r.get("cost")),
                sku=usage_type,
                usage_amount=_as_float(r.get("usage_amount")),
                zone=_as_str(r.get("zone")),
                purchase_option=_purchase_option(CLOUD_AWS, usage_type),
                is_provisional=_is_provisional(day, provisional_cutoff),
            )
        )
    return out


# --- GitHub dummy provider ---------------------------------------------------
# Labelled placeholder until a classic PAT with `user` scope exists. Same normalized
# shape as the real clouds so the UI renders a third cloud today and the swap-to-real
# is a provider change only. Deterministic (day-ordinal driven), never random.
_GH_REPOS = [
    "agent-orchestrator",
    "market-tick-data-service",
    "deployment-api",
    "instruments-service",
    "execution-service",
]


def github_facts(start: date, end: date) -> list[CostRecord]:
    out: list[CostRecord] = []
    day = start
    while day < end:
        ordinal = day.toordinal()
        iso = day.isoformat()
        # Actions minutes — spread across a few repos, gently varied per day.
        for i, repo in enumerate(_GH_REPOS):
            amount = 1.05 + 0.18 * ((ordinal + i * 3) % 5)
            out.append(
                CostRecord(
                    cloud=CLOUD_GITHUB,
                    day=iso,
                    service="GitHub Actions",
                    resource_id=f"{repo} · CI",
                    resource_kind=KIND_OTHER,
                    region="global",
                    cost=round(amount, 4),
                    is_placeholder=True,
                )
            )
        out.append(
            CostRecord(
                cloud=CLOUD_GITHUB,
                day=iso,
                service="Copilot (3 seats)",
                resource_id="Copilot seats",
                resource_kind=KIND_OTHER,
                region="global",
                cost=1.90,
                is_placeholder=True,
            )
        )
        out.append(
            CostRecord(
                cloud=CLOUD_GITHUB,
                day=iso,
                service="Packages & Storage",
                resource_id="Packages & Storage",
                resource_kind=KIND_OTHER,
                region="global",
                cost=round(0.72 + 0.06 * (ordinal % 4), 4),
                is_placeholder=True,
            )
        )
        day = day.fromordinal(day.toordinal() + 1)
    return out


def _is_provisional(day: str, cutoff: date) -> bool:
    try:
        return date.fromisoformat(day) >= cutoff
    except ValueError:
        return False
