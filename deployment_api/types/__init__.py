"""Local deployment-api response types.

TODO: Migrate ``shard_detail`` response types into UAC
``unified_api_contracts.internal.architecture_v2`` (deployment API domain)
once the downstream UI consumer is stable. Tracked in plan
``data_status_institutional_drilldown_2026_04_24``.
"""

from deployment_api.types.shard_detail import (
    ShardCoord,
    ShardDetailResponse,
    ShardDownloadUrls,
    ShardGcsMetadata,
    ShardPayloadFixtures,
    ShardPayloadGrouped,
    ShardPayloadPerSymbol,
    ShardPayloadReference,
    ShardSchema,
    ShardSchemaColumn,
    VenueDetailResponse,
)

__all__ = [
    "ShardCoord",
    "ShardDetailResponse",
    "ShardDownloadUrls",
    "ShardGcsMetadata",
    "ShardPayloadFixtures",
    "ShardPayloadGrouped",
    "ShardPayloadPerSymbol",
    "ShardPayloadReference",
    "ShardSchema",
    "ShardSchemaColumn",
    "VenueDetailResponse",
]
