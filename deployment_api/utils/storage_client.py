"""
Cloud-agnostic storage client with connection pool optimization.

This module provides a thin wrapper around unified-trading-library storage client
exposing the native cloud storage client for code that requires the provider API directly.

For high-concurrency workloads (e.g. TURBO data status endpoint making 7+ years
of parallel directory queries), the connection pool size is configurable via
GCS_POOL_SIZE env var.
"""

from unified_trading_library import get_storage_client as _get_unified_storage_client

from deployment_api.settings import GCP_PROJECT_ID as _DEFAULT_PROJECT_ID
from deployment_api.settings import GCS_POOL_SIZE as DEFAULT_POOL_SIZE


def get_storage_client(
    project_id: str | None = None,
    pool_size: int = DEFAULT_POOL_SIZE,
):
    """
    Get a cloud storage client (cloud-agnostic via unified-trading-library).

    Uses unified-trading-library for cloud-agnostic storage operations.
    Connection pool size is managed by the underlying implementation.

    Args:
        project_id: Cloud project ID (default: from env or ADC)
        pool_size: Max connections per host (default: 200, configurable via GCS_POOL_SIZE env)
                  Note: pool_size configuration depends on underlying cloud provider support

    Returns:
        Native cloud storage client (google.cloud.storage.Client for GCP)

    Note:
        Returns the native GCS client extracted from the unified-trading-library wrapper
        for callers that require the google.cloud.storage.Client API directly.
        Migration to unified-cloud-interface is tracked in QUALITY_GATE_BYPASS_AUDIT.md §2.2.
    """
    # Get project ID
    if project_id is None:
        project_id = _DEFAULT_PROJECT_ID

    # Get the unified client wrapper
    unified_client = _get_unified_storage_client(project_id=project_id)

    # Extract the native GCS client — GCSStorageClient stores it in _client attribute
    if hasattr(unified_client, "_client"):
        return unified_client._client

    # Fallback: return the unified client (may not have full GCS API compatibility)
    return unified_client
