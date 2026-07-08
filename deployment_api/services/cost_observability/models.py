"""Normalized cross-cloud cost models — one shape for GCP, AWS, and GitHub.

The whole point of this module: the UI never sees a cloud-native billing schema.
Each provider adapter maps its native rows into `CostRecord`; every API response is
built from a list of those. Adding a fourth cloud is a new adapter, not a UI change.
"""

from __future__ import annotations

from dataclasses import dataclass

from pydantic import BaseModel, Field

# Cloud identifiers used across the whole feature (UI colour-keys off these).
CLOUD_GCP = "gcp"
CLOUD_AWS = "aws"
CLOUD_GITHUB = "github"

# Resource kinds — how a resource is surfaced in the granular leaf tables.
KIND_VM = "vm"
KIND_BUCKET = "bucket"
KIND_OTHER = "other"


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
    cost: float
    credit: float = 0.0
    is_provisional: bool = False  # recent day, still reconciling
    is_placeholder: bool = False  # dummy data (GitHub until PAT lands)


class CloudSummary(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    cloud: str
    total: float  # NET — what you actually pay = gross + credit
    gross: float = 0.0  # usage cost at list/contract rate, before credits
    credit: float = 0.0  # credits applied this window (≤ 0: promo, CUD/SUD, free-tier)
    delta_pct: float | None = None  # net vs the prior equal-length window
    daily: list[float] = Field(default_factory=list)  # NET sparkline, oldest → newest
    is_placeholder: bool = False


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
    cost: float  # NET — usage cost after this group's credits (matches the summary net total)
    detail: str = ""  # e.g. machine type, "GCS", region name
    resource_kind: str = KIND_OTHER
    share_pct: float = 0.0
    is_provisional: bool = False


class BreakdownResponse(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    dimension: str  # service | resource | bucket | region | day
    cloud: str  # all | gcp | aws | github
    days: int
    total: float
    rows: list[BreakdownRow] = Field(default_factory=list)


class TimeseriesPoint(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    date: str
    values: dict[str, float] = Field(default_factory=dict)  # cloud -> USD that day


class TimeseriesResponse(BaseModel):  # CORRECT-LOCAL: FastAPI API contract model
    days: int
    clouds: list[str] = Field(default_factory=list)
    points: list[TimeseriesPoint] = Field(default_factory=list)
