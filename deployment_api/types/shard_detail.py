"""Response schemas for ``GET /api/data-status/shard-detail``.

Local to deployment-api for now; see
``deployment_api/types/__init__.py`` for the migration TODO into
``unified_api_contracts.internal.architecture_v2``.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

# ``shard_class`` drives UI rendering of the payload branch.  The mapping
# from ``(service, asset_group, instrument_type, data_type)`` to one of these
# four values is owned by
# ``deployment_api.services.shard_detail._classify_shard``.
ShardClassLiteral = Literal["grouped", "per_symbol", "reference", "fixtures"]

CaptureStatusLiteral = Literal["captured", "empty_confirmed", "attempted_failed", "missing"]


class ShardCoord(BaseModel):  # CORRECT-LOCAL — API response shape, no other consumer
    """Echo of the request coordinates the response corresponds to."""

    model_config = ConfigDict(frozen=True)

    service: str
    asset_group: str
    instrument_type: str
    data_type: str
    day: str
    venue: str | None = None
    underlying: str | None = None
    instrument_id: str | None = None


class ShardSchemaColumn(BaseModel):  # CORRECT-LOCAL — API response shape
    """One column in the declared ``SchemaContract`` for a shard."""

    model_config = ConfigDict(frozen=True)

    name: str
    dtype: str
    nullable: bool
    required: bool
    provided_by_venues: list[str] | None = None
    description: str = ""


class ShardSchema(BaseModel):  # CORRECT-LOCAL — API response shape
    """Schema block of a shard-detail response."""

    model_config = ConfigDict(frozen=True)

    registered: bool
    source: Literal["CONTRACT_REGISTRY", "VENUE_CONTRACT_OVERRIDES", "none"]
    symbol_column: str | None
    columns: list[ShardSchemaColumn] = Field(default_factory=list)
    message: str = ""
    instrument_type_resolved_via: Literal["explicit", "auto", "none"] = "explicit"
    """``explicit`` — caller passed a concrete instrument_type and the lookup
    succeeded against it. ``auto`` — caller passed ``AUTO`` / ``UNKNOWN`` and
    the backend resolved the instrument_type by scanning the registry for
    any ``(asset_group, *, data_type)`` tuple. ``none`` — caller passed AUTO
    but no contract matched the (asset_group, data_type) pair, so the schema
    is unregistered."""

    instrument_type_resolved: str | None = None
    """The concrete instrument_type used for the lookup. Echoes the caller
    value when ``explicit``; populated with the resolver's pick when
    ``auto`` so the UI can display "resolved as <X>". ``None`` when
    resolution failed."""


class ShardGcsMetadata(BaseModel):  # CORRECT-LOCAL — API response shape
    """GCS footer + manifest metadata for one parquet shard."""

    model_config = ConfigDict(frozen=True)

    path: str | None
    file_size_bytes: int | None
    row_count: int | None
    captured_at: str | None
    capture_status: CaptureStatusLiteral
    error_reason: str | None = None


class ShardDownloadUrls(BaseModel):  # CORRECT-LOCAL — API response shape
    """Links the UI can render for downloading a shard."""

    model_config = ConfigDict(frozen=True)

    parquet_signed_url: str | None
    csv_projected: str | None


class ShardPayloadGrouped(BaseModel):  # CORRECT-LOCAL — API response shape
    """Payload branch for bundle shards (options_chain, dex_swaps, …)."""

    model_config = ConfigDict(frozen=True)

    instrument_list: list[dict[str, str]] = Field(default_factory=list)


class ShardPayloadPerSymbol(BaseModel):  # CORRECT-LOCAL — API response shape
    """Payload branch for per-symbol time-series shards (PERPETUAL / SPOT)."""

    model_config = ConfigDict(frozen=True)

    instrument_list: list[dict[str, str]] = Field(default_factory=list)


class ShardPayloadReference(BaseModel):  # CORRECT-LOCAL — API response shape
    """Payload branch for instruments-service reference-data shards."""

    model_config = ConfigDict(frozen=True)

    instrument_definitions: list[dict[str, object]] = Field(default_factory=list)


class ShardPayloadFixtures(BaseModel):  # CORRECT-LOCAL — API response shape
    """Payload branch for sports fixtures shards."""

    model_config = ConfigDict(frozen=True)

    fixtures: list[dict[str, object]] = Field(default_factory=list)


class ShardDetailResponse(BaseModel):  # CORRECT-LOCAL — API response shape
    """Full response envelope for ``GET /api/data-status/shard-detail``.

    One of ``payload_grouped`` / ``payload_per_symbol`` / ``payload_reference``
    / ``payload_fixtures`` is populated based on ``shard_class``; the others
    are ``None``.  Keeping them as explicit fields keeps the response OpenAPI
    schema statically typed for UI clients.
    """

    model_config = ConfigDict(frozen=True)

    coord: ShardCoord
    shard_class: ShardClassLiteral
    schema_: ShardSchema = Field(alias="schema")
    gcs: ShardGcsMetadata
    download_urls: ShardDownloadUrls
    sample_rows: list[dict[str, object]] = Field(default_factory=list)
    payload_grouped: ShardPayloadGrouped | None = None
    payload_per_symbol: ShardPayloadPerSymbol | None = None
    payload_reference: ShardPayloadReference | None = None
    payload_fixtures: ShardPayloadFixtures | None = None


class VenueDetailResponse(BaseModel):  # CORRECT-LOCAL — API response shape
    """Response envelope for ``fetch_venue_detail`` (CeFi + DeFi branches).

    DeFi responses may carry either chain-level aggregates (``protocols``,
    ``total_pools``, ``total_tokens``) or composite protocol-chain pool
    listings (``pools``, ``tokens``).  ``asset_group`` is always echoed so the
    UI can render the correct view without inferring from the venue string.
    """

    model_config = ConfigDict(frozen=True)

    asset_group: str
    venue: str
    chain: str | None = None
    protocol: str | None = None
    total_instruments: int = 0
    total_pools: int = 0
    total_tokens: int = 0
    instruments: list[dict[str, object]] = Field(default_factory=list)
    protocols: list[dict[str, object]] = Field(default_factory=list)
    pools: list[dict[str, object]] = Field(default_factory=list)
    tokens: list[dict[str, object]] = Field(default_factory=list)
    day: str | None = None
