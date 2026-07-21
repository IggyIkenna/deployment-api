"""Unit tests for routes/_run_log_resolution.py — live-first, archive-fallback resolution.

Credential-free: ``gcs_describe_object`` is mocked at the module's import site — no real
GCS access. Pins WS-4 decision 2: try ``vm-logs/{vm}/run.log`` first regardless of
completed_at; fall back to the final-snapshot archive path only when the live object is
confirmed absent; report an honest "neither exists" (``metadata is None``) rather than a
guessed URI that would 404.
"""

from __future__ import annotations

from unittest.mock import patch

from unified_trading_library import BlobMetadata

from deployment_api.routes._run_log_resolution import resolve_run_log_location


def _meta(name: str) -> BlobMetadata:
    return BlobMetadata(name=name, bucket="deployment-scripts-test", size=1024, last_modified="2026-07-21T00:00:00Z")


def test_resolves_live_path_when_present() -> None:
    """Live object exists → resolved immediately, archive is never probed."""
    live_meta = _meta("vm-logs/vm-x/run.log")
    with patch(
        "deployment_api.routes._run_log_resolution.gcs_describe_object",
        side_effect=[live_meta],
    ) as mock_describe:
        result = resolve_run_log_location("vm-x", "test-project")
    assert result.location == "live"
    assert result.uri == "gs://deployment-scripts-test-project/vm-logs/vm-x/run.log"
    assert result.metadata is live_meta
    mock_describe.assert_called_once()  # archive never probed once live hits


def test_falls_back_to_archive_when_live_absent() -> None:
    """Live miss → falls back to the final-snapshot archive path."""
    archive_meta = _meta("log-archive/final/vm-x/run.log")
    with patch(
        "deployment_api.routes._run_log_resolution.gcs_describe_object",
        side_effect=[None, archive_meta],
    ):
        result = resolve_run_log_location("vm-x", "test-project")
    assert result.location == "archive"
    assert result.uri == "gs://deployment-scripts-test-project/log-archive/final/vm-x/run.log"
    assert result.metadata is archive_meta


def test_honest_absence_when_neither_path_exists() -> None:
    """Neither live nor archive exists — metadata=None, never a fabricated hit."""
    with patch(
        "deployment_api.routes._run_log_resolution.gcs_describe_object",
        side_effect=[None, None],
    ):
        result = resolve_run_log_location("vm-x-ancient", "test-project")
    assert result.location == "archive"
    assert result.metadata is None
