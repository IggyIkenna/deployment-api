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

# Mirrors UAC ``ServiceEmissionStateEnum`` value set
# (``unified_api_contracts.canonical.crosscutting.service_emission_state``).
# The Pydantic ``Literal`` here is intentionally a thin type-level alias for the
# closed-set UAC enum values; the enum itself owns SSOT semantics (see workspace
# rule "Single Source of Truth — types in UAC"). Drift between this Literal and
# the UAC enum is review-blocking; if a new state lands in UAC, bump this and
# the deployment-ui TypeScript union in lockstep.
ServiceEmissionStateLiteral = Literal[
    "PUBLISHED_OK",
    "PUBLISHED_DEGRADED",
    "STALE_DATA_HEARTBEAT_ONLY",
    "BLOCKED",
]


class ShardCoord(BaseModel):  # CORRECT-LOCAL — API response shape, no other consumer
    """Echo of the request coordinates the response corresponds to.

    ``feature_family`` is the parent classification of ``feature_group`` —
    the closed-set UAC ``FeatureFamily`` enum (``calendar`` / ``commodity`` /
    ``cross_instrument`` / ``delta_one`` / ``multi_timeframe`` / ``onchain`` /
    ``sports`` / ``volatility``). Populated when the request resolves a
    features-* shard; ``None`` for non-features services. Plan: features-repo
    consolidation Phase 8B (deployment-api side).
    """

    model_config = ConfigDict(frozen=True)

    service: str
    asset_group: str
    instrument_type: str
    data_type: str
    day: str
    venue: str | None = None
    underlying: str | None = None
    instrument_id: str | None = None
    feature_family: str | None = None


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
    """GCS footer + manifest metadata for one parquet shard.

    The four v8 manifest columns (``pipeline_mode`` / ``service_emission_state``
    / ``last_emission_decision_at`` / ``expected_window_completeness_fraction``)
    surface here when the underlying manifest row carries them (writegate Phase
    4). Pre-v8 manifest rows have these fields absent on disk; the service
    threads ``None`` so the UI renders a placeholder pill (forward-compat).
    """

    model_config = ConfigDict(frozen=True)

    path: str | None
    file_size_bytes: int | None
    row_count: int | None
    captured_at: str | None
    capture_status: CaptureStatusLiteral
    error_reason: str | None = None

    pipeline_mode: str | None = None
    """Source-and-mode tag from UAC ``PipelineMode`` enum (one of the
    ``batch_*`` source values or ``live_websocket``). The UI renders this as a
    small badge next to ``capture_status`` (e.g. ``batch_databento`` /
    ``live_websocket``). ``None`` on pre-v8 manifest rows or when the writer
    did not stamp the column."""

    service_emission_state: ServiceEmissionStateLiteral | None = None
    """Service-output emission-policy state from UAC
    ``ServiceEmissionStateEnum`` (writegate slice (b) / v8). ``None`` for
    shards whose writer predates the emission-policy rollout (slice (c) Phase
    6.1-6.9 will fill it in)."""

    last_emission_decision_at: str | None = None
    """ISO-8601 UTC timestamp when the publish-side emission policy last
    decided on this row (writegate Phase 4). ``None`` on pre-v8 rows."""

    expected_window_completeness_fraction: float | None = None
    """Numeric in ``[0.0, 1.0]`` — manifest-row-level completeness derived
    from ``(captured + empty_confirmed) / (captured + empty_confirmed +
    attempted_failed + expected_unattempted)`` at write time. Distinct from
    :class:`LeafCompletenessEnvelope` which carries the per-parquet-row
    ``completeness_fraction`` envelope. ``None`` on pre-v8 rows."""


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


class LeafParquetColumnStat(BaseModel):  # CORRECT-LOCAL — API response shape
    """Per-column live stats for one leaf parquet (writegate Phase 4.A.3)."""

    model_config = ConfigDict(frozen=True)

    name: str
    dtype: str
    non_null_count: int
    null_count: int
    nan_ratio: float
    """``null_count / row_count`` clamped to [0.0, 1.0]; 0.0 when row_count is 0."""


class LeafAvailableAtEnvelope(BaseModel):  # CORRECT-LOCAL — API response shape
    """Envelope of the ``available_at`` column for one leaf parquet (Phase 4.A.3).

    Populated when the parquet has an ``available_at`` column (UAC contract
    requires it on every shard's parquet — see workspace CLAUDE.md
    "available_at is per-row, write-time, equal to live-pipeline-arrival").
    Absent envelope means the column is missing from the parquet, which is
    a writegate Phase 1A.future ``MissingAvailableAt`` failure mode.
    """

    model_config = ConfigDict(frozen=True)

    present: bool
    min_iso: str | None = None
    max_iso: str | None = None
    null_count: int = 0


class LeafCompletenessEnvelope(BaseModel):  # CORRECT-LOCAL — API response shape
    """Envelope of the ``completeness_fraction`` column for one leaf parquet
    (writegate slice (b) Phase 5.5; ships forward-compatible).

    Populated when the parquet has a ``completeness_fraction`` column written
    by a service consuming the service-output emission policy
    (`publish_with_policy()` / `publish_with_manifest_lookup()`). The
    completeness_fraction is per-row in ``[0.0, 1.0]``: 1.0 means the upstream
    window was fully captured at write-time; < 1.0 means a permissive policy
    (``PARTIAL_OK`` / ``NAN_FILL``) admitted gaps.

    Absent envelope (``present=False``) means the parquet predates the
    emission-policy rollout (writegate slice (c) per-service Phase 6.1-6.9).
    The UI renders a placeholder pill instead of the colour-coded envelope.

    Distinct from :class:`LeafAvailableAtEnvelope` because completeness ranges
    are numeric (vs ISO timestamps) and the colour-coding is value-driven
    (green ≥ 1.0, amber 0.95-1.0, red < 0.95).
    """

    model_config = ConfigDict(frozen=True)

    present: bool
    min_fraction: float | None = None
    max_fraction: float | None = None
    mean_fraction: float | None = None
    null_count: int = 0
    incomplete_window_present_count: int = 0
    """Number of rows whose ``incomplete_window`` column is non-empty / non-null
    (separate signal from `min_fraction < 1.0` — a row may have
    `completeness_fraction == 1.0` and an empty `incomplete_window` list; both
    columns track honest absence at row grain). Zero when the
    ``incomplete_window`` column is absent."""


class LeafParquetStats(BaseModel):  # CORRECT-LOCAL — API response shape
    """Live per-leaf-parquet stats response (writegate Phase 4.A.3).

    Distinct from the existing ``/data-status/schema`` endpoint, which
    returns the DECLARED ``SchemaContract`` columns from UAC. This
    response carries the ACTUAL parquet stats: row count, per-column
    non_null + NaN ratio, and the ``available_at`` envelope. Used by the
    deployment-ui schema-view modal (Phase 4.B.3) to surface real shard
    health alongside the contract.
    """

    model_config = ConfigDict(frozen=True)

    coord: ShardCoord
    gs_uri: str | None
    """Resolved gs:// URI for the leaf parquet, or ``None`` when no path
    matches the coordinates (e.g. fixtures branch — sports/fixtures live
    under sports_reference/, separate resolver)."""

    available: bool
    """True when the parquet exists on GCS AND was readable. False on
    missing path / corrupt parquet / read error — caller renders the
    error_reason field for diagnosis."""

    error_reason: str | None = None
    """Populated when ``available`` is False — exception class + message
    (size-bounded). ``None`` on success."""

    row_count: int = 0
    column_count: int = 0
    columns: list[LeafParquetColumnStat] = Field(default_factory=list)
    available_at: LeafAvailableAtEnvelope = Field(default_factory=lambda: LeafAvailableAtEnvelope(present=False))

    completeness: LeafCompletenessEnvelope = Field(default_factory=lambda: LeafCompletenessEnvelope(present=False))
    """Envelope of the ``completeness_fraction`` + ``incomplete_window`` columns
    when the parquet was written through the service-output emission policy
    (writegate slice (b) Phase 5.5). ``present=False`` means the columns are
    absent — the parquet predates the emission-policy rollout (slice (c) per-
    service Phase 6.1-6.9 will fill it in). UI renders a placeholder pill
    instead of the colour-coded envelope in that case."""

    file_size_bytes: int | None = None
    """Size of the parquet on GCS (footer call); useful sanity check
    alongside row_count + column_count."""

    truncated: bool = False
    """True when the parquet exceeds the safety row limit and stats were
    computed on the prefix slice. The UI should render a "stats from
    first N rows" hint when truncated is True."""

    truncated_at_rows: int | None = None
    """Row prefix used when ``truncated`` is True (e.g. 500_000)."""

    feature_family: str | None = None
    """Top-level convenience echo of ``coord.feature_family``. Populated for
    features-* shards via UAC ``get_feature_family(feature_group)`` / the
    parquet's ``feature_family`` column when present (writer-stamped per
    UTL ``MissingFeatureFamilyError`` enforcement). ``None`` for non-features
    services or when the feature_group → family mapping is missing. Plan:
    features-repo consolidation Phase 8B (deployment-api side)."""


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
    total_instruments_unfiltered: int = 0
    total_pools: int = 0
    total_tokens: int = 0
    instruments: list[dict[str, object]] = Field(default_factory=list)
    protocols: list[dict[str, object]] = Field(default_factory=list)
    pools: list[dict[str, object]] = Field(default_factory=list)
    tokens: list[dict[str, object]] = Field(default_factory=list)
    day: str | None = None
