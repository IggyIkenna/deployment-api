"""Unit tests for registry_reader.resolve_active_registry (Phase-2 migration).

Covers the four branches with injected seams (no real Firestore / GCS):
  * flag on + Firestore returns entries → Firestore result (the indexed scale path)
  * flag on + Firestore empty → loud GCS fallback
  * flag on + Firestore raises → loud GCS fallback
  * flag off → GCS only (Firestore not consulted)
"""

from __future__ import annotations

import os

os.environ.setdefault("CLOUD_MOCK_MODE", "false")
os.environ.setdefault("CLOUD_PROVIDER", "local")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("DISABLE_AUTH", "true")

from unified_trading_library import DeploymentRegistryEntry

from deployment_api.registry_reader import resolve_active_registry


def _entry(deployment_id: str, status: str = "running") -> DeploymentRegistryEntry:
    return DeploymentRegistryEntry(
        deployment_id=deployment_id,
        vm_name=f"vm-{deployment_id}",
        asset_group="cefi",
        task="backfill",
        mode="full",
        start_date="2024-01-01",
        end_date="2024-01-31",
        status=status,
        started_at="2026-07-14T01:00:00Z",
        last_heartbeat_at="2026-07-14T01:00:00Z",
        completed_at=None,
        exit_code=None,
        rows_in=0,
        rows_out=0,
        rows_error=0,
        events_emitted=1,
        log_uri="gs://bucket/x/run.log",
    )


class _Store:
    """Fake DeploymentRegistryStore — returns a preset list or raises."""

    def __init__(self, entries: list[DeploymentRegistryEntry] | None = None, raises: bool = False) -> None:
        self._entries = entries or []
        self._raises = raises
        self.queried = False

    def query_by_status(self, status: str) -> list[DeploymentRegistryEntry]:
        self.queried = True
        if self._raises:
            raise RuntimeError("firestore unavailable")
        return [e for e in self._entries if e.status == status]

    # unused Protocol members (present for structural conformance)
    def register(self, entry: DeploymentRegistryEntry) -> None: ...
    def heartbeat(self, entry: DeploymentRegistryEntry) -> None: ...
    def complete(self, entry: DeploymentRegistryEntry) -> None: ...
    def list_active(self) -> list[DeploymentRegistryEntry]:
        return self.query_by_status("running")

    def get(self, deployment_id: str) -> DeploymentRegistryEntry | None:
        return None


class _Gcs:
    """Fake GCS DeploymentsRegistry — records whether list_active() was called."""

    def __init__(self, entries: list[DeploymentRegistryEntry]) -> None:
        self._entries = entries
        self.called = False

    def list_active(self) -> list[DeploymentRegistryEntry]:
        self.called = True
        return self._entries


def test_firestore_hit_returns_firestore_entries() -> None:
    store = _Store(entries=[_entry("fs-1"), _entry("fs-2")])
    gcs = _Gcs(entries=[_entry("gcs-1")])
    result = resolve_active_registry(store=store, gcs=gcs, firestore_enabled=True)  # type: ignore[arg-type]
    assert {e.deployment_id for e in result} == {"fs-1", "fs-2"}
    assert store.queried is True
    assert gcs.called is False  # GCS not consulted when Firestore has data


def test_firestore_empty_falls_back_to_gcs() -> None:
    store = _Store(entries=[])  # Firestore returns nothing (still filling)
    gcs = _Gcs(entries=[_entry("gcs-1")])
    result = resolve_active_registry(store=store, gcs=gcs, firestore_enabled=True)  # type: ignore[arg-type]
    assert {e.deployment_id for e in result} == {"gcs-1"}
    assert store.queried is True
    assert gcs.called is True  # loud fallback to GCS on empty


def test_firestore_error_falls_back_to_gcs() -> None:
    store = _Store(raises=True)
    gcs = _Gcs(entries=[_entry("gcs-1"), _entry("gcs-2")])
    result = resolve_active_registry(store=store, gcs=gcs, firestore_enabled=True)  # type: ignore[arg-type]
    assert {e.deployment_id for e in result} == {"gcs-1", "gcs-2"}
    assert gcs.called is True  # loud fallback to GCS on error


def test_flag_off_uses_gcs_only() -> None:
    store = _Store(entries=[_entry("fs-1")])
    gcs = _Gcs(entries=[_entry("gcs-1")])
    result = resolve_active_registry(store=store, gcs=gcs, firestore_enabled=False)  # type: ignore[arg-type]
    assert {e.deployment_id for e in result} == {"gcs-1"}
    assert store.queried is False  # Firestore never consulted when the flag is off
    assert gcs.called is True
