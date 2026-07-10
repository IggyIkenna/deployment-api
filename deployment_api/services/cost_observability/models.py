"""Normalized cross-cloud cost models — one shape for GCP, AWS, and GitHub.

The whole point of this module: the UI never sees a cloud-native billing schema.
Each provider adapter maps its native rows into `CostRecord`; every API response is
built from a list of those. Adding a fourth cloud is a new adapter, not a UI change.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pydantic import BaseModel, Field

# Cloud identifiers used across the whole feature (UI colour-keys off these).
CLOUD_GCP = "gcp"
CLOUD_AWS = "aws"
CLOUD_GITHUB = "github"

# Resource kinds — how a resource is surfaced in the granular leaf tables.
KIND_VM = "vm"
KIND_BUCKET = "bucket"
KIND_OTHER = "other"

# Purchase-option classification (derived from SKU/usage-type, not a billed-export column) —
# "other" covers non-compute SKUs (storage, network, …) where the axis doesn't apply.
PURCHASE_SPOT = "spot"
PURCHASE_ON_DEMAND = "on-demand"
PURCHASE_OTHER = "other"

# Resource-level GCP `labels` surfaced for the "By label" breakdown dimension (spend by business
# context, not infra). GCP-only — AWS/GitHub carry no cost-allocation tags. Shared by the query
# builder (columns), the provider (mapping), and the route (valid label_key values).
BUSINESS_LABEL_KEYS = ("purpose", "category", "venue", "asset_group")


@dataclass
class CostRecord:  # CORRECT-LOCAL: internal aggregation struct, not a cross-service contract
    """One day of cost for one (cloud, service, resource, region) tuple.

    Internal plumbing — never crosses the API boundary (the response models below do), so
    it is a plain dataclass, not a UAC domain contract. `cost` is USD, already net of the
    export's own rounding; `credit` is negative-or-zero discount applied that day.
    """

    cloud: str
    day: str  # YYYY-MM-DD (UTC usage day)
    service: str
    resource_id: str  # "" when the billing row has no resource granularity
    resource_kind: str  # vm | bucket | other
    region: str
    cost: float  # USD — GCP converted from its native currency at query time (see queries.gcp_facts_sql)
    credit: float = 0.0
    # Native-currency figures for the GCP-invoice tally view. GCP bills in GBP; `cost`/`credit` above are
    # the USD conversion, these are the raw pre-conversion amounts. AWS/GitHub are USD-native → native == USD.
    currency: str = "USD"  # native billing currency code (GCP="GBP")
    cost_native: float = 0.0  # `cost` in `currency`; == cost for USD-native clouds
    credit_native: float = 0.0  # `credit` in `currency`; == credit for USD-native clouds
    sku: str = ""  # GCP sku.description / AWS line_item_usage_type ("" if not sourced)
    usage_amount: float = 0.0  # GCP usage.amount_in_pricing_units / AWS line_item_usage_amount
    usage_unit: str = ""  # GCP usage.pricing_unit ("" for AWS — not in the export column set)
    zone: str = ""  # GCP location.zone / AWS line_item_availability_zone ("" — finer than region)
    purchase_option: str = PURCHASE_OTHER  # spot | on-demand | other, derived from sku (see providers._purchase_option)
    machine_type: str = ""  # GCP system_labels compute.googleapis.com/machine_spec, e.g. "e2-highmem-16"
    vcpu: int | None = None  # GCP system_labels compute.googleapis.com/cores
    memory_gb: float | None = None  # GCP system_labels compute.googleapis.com/memory (MiB / 1024)
    is_provisional: bool = False  # recent day, still reconciling
    is_placeholder: bool = False  # dummy data (GitHub until PAT lands)
    # Resource-level business-context labels (GCP `labels`: purpose/category/venue/asset_group) for the
    # "By label" breakdown dimension. Empty for AWS/GitHub (no cost-allocation tags).
    labels: dict[str, str] = field(default_factory=dict)


class CloudSummary(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    cloud: str
    total: float  # NET — what you actually pay = gross + credit
    gross: float = 0.0  # usage cost at list/contract rate, before credits
    credit: float = 0.0  # credits applied this window (≤ 0: promo, CUD/SUD, free-tier)
    delta_pct: float | None = None  # net vs the prior equal-length window
    daily: list[float] = Field(default_factory=list)  # NET sparkline, oldest → newest
    is_placeholder: bool = False
    # Native-currency KPI figures (the GBP-tally view toggle). GCP="GBP"; USD-native clouds repeat the USD values.
    currency: str = "USD"
    total_native: float = 0.0
    gross_native: float = 0.0
    credit_native: float = 0.0


class SummaryResponse(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    days: int
    total: float  # NET grand total — what you actually pay = gross + credit
    gross: float = 0.0  # sum of usage cost before credits
    credit: float = 0.0  # sum of credits applied (≤ 0)
    run_rate_daily: float
    delta_pct: float | None = None
    dates: list[str] = Field(default_factory=list)  # window days, oldest → newest
    clouds: list[CloudSummary] = Field(default_factory=list)
    provisional_days: int = 0
    generated_at: str = ""


class BreakdownRow(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    label: str
    cloud: str | None  # None only for cross-cloud "by day" rows
    cost: float  # NET — usage cost after this group's credits (matches the summary net total; primary)
    gross: float = 0.0  # usage cost at list/contract rate before credits (Σcost for this group)
    credit: float = 0.0  # credits applied to this group (≤ 0); cost == gross + credit
    # Native-currency figures for the GBP-tally view (GCP="GBP"; USD-native clouds repeat the USD values).
    currency: str = "USD"
    cost_native: float = 0.0
    gross_native: float = 0.0
    credit_native: float = 0.0
    detail: str = ""  # e.g. machine type, "GCS", region name
    resource_kind: str = KIND_OTHER
    share_pct: float = 0.0
    is_provisional: bool = False
    # True for a synthetic roll-up row (the "Other (N more)" tail sum or the "Unattributed" no-resource
    # sum) — not a real group; the UI pins it to the bottom, excludes it from sort, and skips its bar.
    is_aggregate: bool = False
    is_idle: bool = False  # cost-waste flag (resource dimension only) — see services.cost_observability.waste
    waste_kind: str = ""  # "" | idle_static_ip | orphaned_disk | idle_elastic_ip
    # Bucket-only (dimension=bucket rows): derived from the storage-volume SKUs' usage_amount,
    # never Cloud Monitoring/CloudWatch. None when the row isn't a bucket or carries no storage-
    # volume usage this window.
    storage_gb: float | None = None  # avg GB stored over the window (GiB/GB-month -> avg GB)
    storage_class_gb: dict[str, float] | None = None  # {"Standard"|"Nearline"|"Coldline"|"Archive": gb}
    cost_per_gb: float | None = None  # net cost / storage_gb for the window
    # Bucket-only: net cost split by SKU component so a bucket's total is legible (an event-log
    # bucket is ~all operations, not storage). Keys: storage | operations | egress | other; only
    # components that round to a non-zero amount are present, and they sum to ~`cost` (net).
    cost_by_component: dict[str, float] | None = None
    purchase_option: str = ""  # spot | on-demand | other; "" when the axis doesn't apply to this row
    machine_type: str = ""  # VM rows only, e.g. "e2-highmem-16" (from GCP system_labels)
    vcpu: int | None = None  # VM rows only
    memory_gb: float | None = None  # VM rows only


class BreakdownResponse(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    dimension: str  # service | resource | bucket | region | day | sku | zone
    cloud: str  # all | gcp | aws | github
    days: int
    total: float  # TRUE window total for this dimension (all groups, pre-cap) — consistent across tabs
    total_groups: int = 0  # distinct real groups before the top-N cap (excludes synthetic aggregate rows)
    rows: list[BreakdownRow] = Field(default_factory=list)


class TimeseriesPoint(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    date: str
    values: dict[str, float] = Field(default_factory=dict)  # cloud -> USD that day


class TimeseriesResponse(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    days: int
    clouds: list[str] = Field(default_factory=list)
    points: list[TimeseriesPoint] = Field(default_factory=list)
