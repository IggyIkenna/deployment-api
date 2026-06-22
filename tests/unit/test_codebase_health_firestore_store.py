"""Unit tests for resolve_codebase_health_map in _ci_status_firestore_store."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from deployment_api.routes._ci_status_firestore_store import (
    COLLECTION,
    resolve_codebase_health_map,
)

# ── Fake Firestore plumbing (mirrors test_ci_status_firestore_store.py) ─────────


def _make_doc(doc_id: str, data: dict[str, object]) -> MagicMock:
    doc = MagicMock()
    doc.id = doc_id
    doc.to_dict.return_value = data
    return doc


def _make_firestore_module_factory(docs: list[MagicMock]):
    """Build a fake firestore_module_factory returning the given collection docs."""
    collection_mock = MagicMock()
    collection_mock.stream.return_value = docs

    client_mock = MagicMock()
    client_mock.collection.return_value = collection_mock

    module_mock = MagicMock()
    module_mock.Client.return_value = client_mock

    def factory():
        return module_mock

    return factory, module_mock, client_mock, collection_mock


# ── Tests ───────────────────────────────────────────────────────────────────────


class TestResolveCodebaseHealthFirestoreOverlay:
    def test_firestore_overlay_wins_over_manifest(self) -> None:
        """Firestore codebase_health sub-field takes precedence over the manifest entry."""
        manifest: dict[str, object] = {
            "repositories": {
                "repo-a": {"codebase_health": {"coverage_pct": 70.0, "qg_red_reason": "ruff"}},
            }
        }
        fs_health = {"coverage_pct": 85.5, "qg_red_reason": None, "large_file_count": 2, "warn_file_count": 4}
        docs = [_make_doc("repo-a", {"codebase_health": fs_health})]
        factory, *_ = _make_firestore_module_factory(docs)
        result = resolve_codebase_health_map(manifest, firestore_module_factory=factory)
        assert result["repo-a"] == fs_health

    def test_manifest_fallback_kept_for_repo_without_firestore_doc(self) -> None:
        """Repos absent from Firestore retain their manifest codebase_health."""
        manifest: dict[str, object] = {
            "repositories": {
                "repo-a": {"codebase_health": {"coverage_pct": 75.0}},
                "repo-b": {"codebase_health": {"coverage_pct": 60.0, "qg_red_reason": "pytest"}},
            }
        }
        # Firestore only covers repo-a
        docs = [_make_doc("repo-a", {"codebase_health": {"coverage_pct": 90.0}})]
        factory, *_ = _make_firestore_module_factory(docs)
        result = resolve_codebase_health_map(manifest, firestore_module_factory=factory)
        assert result["repo-a"]["coverage_pct"] == 90.0  # Firestore wins
        assert result["repo-b"]["coverage_pct"] == 60.0  # manifest kept

    def test_firestore_adds_repo_absent_from_manifest(self) -> None:
        """Repos present only in Firestore (not in manifest) are included in the result."""
        manifest: dict[str, object] = {"repositories": {}}
        fs_health = {"coverage_pct": 88.0, "large_file_count": 0, "warn_file_count": 1}
        docs = [_make_doc("new-repo", {"codebase_health": fs_health})]
        factory, *_ = _make_firestore_module_factory(docs)
        result = resolve_codebase_health_map(manifest, firestore_module_factory=factory)
        assert result["new-repo"] == fs_health

    def test_empty_firestore_returns_manifest_only(self) -> None:
        """When Firestore returns no docs, result equals the manifest-derived map."""
        manifest: dict[str, object] = {
            "repositories": {
                "repo-a": {"codebase_health": {"coverage_pct": 77.0}},
            }
        }
        factory, *_ = _make_firestore_module_factory([])
        result = resolve_codebase_health_map(manifest, firestore_module_factory=factory)
        assert result == {"repo-a": {"coverage_pct": 77.0}}

    def test_firestore_doc_missing_codebase_health_field_not_overlaid(self) -> None:
        """A Firestore doc without a codebase_health field does not displace the manifest entry."""
        manifest: dict[str, object] = {
            "repositories": {
                "repo-a": {"codebase_health": {"coverage_pct": 80.0}},
            }
        }
        docs = [_make_doc("repo-a", {"status": "MAIN_GREEN"})]  # no codebase_health key
        factory, *_ = _make_firestore_module_factory(docs)
        result = resolve_codebase_health_map(manifest, firestore_module_factory=factory)
        assert result["repo-a"] == {"coverage_pct": 80.0}

    def test_collection_queried_with_canonical_name(self) -> None:
        """The Firestore query targets the canonical ci_status collection."""
        factory, module_mock, client_mock, collection_mock = _make_firestore_module_factory([])
        resolve_codebase_health_map({}, firestore_module_factory=factory)
        client_mock.collection.assert_called_once_with(COLLECTION)

    def test_project_id_forwarded_to_client(self) -> None:
        factory, module_mock, client_mock, _ = _make_firestore_module_factory([])
        resolve_codebase_health_map({}, project_id="my-project", firestore_module_factory=factory)
        module_mock.Client.assert_called_once_with(project="my-project")

    def test_repo_without_codebase_health_in_manifest_absent_from_result(self) -> None:
        """Repos whose manifest entry has no codebase_health key are absent (not None)."""
        manifest: dict[str, object] = {
            "repositories": {
                "repo-a": {"ci_status": "MAIN_GREEN"},  # no codebase_health
            }
        }
        factory, *_ = _make_firestore_module_factory([])
        result = resolve_codebase_health_map(manifest, firestore_module_factory=factory)
        assert "repo-a" not in result


class TestResolveCodebaseHealthFirestoreUnavailable:
    def test_degrades_to_manifest_on_exception(self) -> None:
        """Any Firestore exception degrades to the manifest-only map (never raises)."""
        manifest: dict[str, object] = {
            "repositories": {
                "repo-a": {"codebase_health": {"coverage_pct": 71.0}},
            }
        }

        def broken_factory():
            raise RuntimeError("connection refused")

        result = resolve_codebase_health_map(manifest, firestore_module_factory=broken_factory)
        assert result == {"repo-a": {"coverage_pct": 71.0}}

    def test_warning_logged_on_firestore_error(self, caplog: pytest.LogCaptureFixture) -> None:
        def raise_os_error():
            raise OSError("timeout")

        with caplog.at_level(logging.WARNING):
            resolve_codebase_health_map({}, firestore_module_factory=raise_os_error)
        assert any("codebase_health Firestore read unavailable" in r.message for r in caplog.records)

    def test_empty_manifest_and_firestore_unavailable_returns_empty(self) -> None:
        def broken_factory():
            raise Exception("sdk not installed")

        result = resolve_codebase_health_map({}, firestore_module_factory=broken_factory)
        assert result == {}


class TestManifestCodebaseHealthSeed:
    def test_list_format_repositories_tolerated(self) -> None:
        """resolve_codebase_health_map handles list-form repositories."""
        manifest: dict[str, object] = {
            "repositories": [
                {"name": "repo-a", "codebase_health": {"coverage_pct": 90.0}},
                {"name": "repo-b", "codebase_health": {"coverage_pct": 55.0, "qg_red_reason": "bandit"}},
            ]
        }
        factory, *_ = _make_firestore_module_factory([])
        result = resolve_codebase_health_map(manifest, firestore_module_factory=factory)
        assert result["repo-a"] == {"coverage_pct": 90.0}
        assert result["repo-b"] == {"coverage_pct": 55.0, "qg_red_reason": "bandit"}

    def test_non_dict_codebase_health_in_manifest_excluded(self) -> None:
        """A non-dict codebase_health value in the manifest is silently skipped."""
        manifest: dict[str, object] = {
            "repositories": {
                "repo-a": {"codebase_health": "bad-value"},  # not a dict
                "repo-b": {"codebase_health": {"coverage_pct": 82.0}},
            }
        }
        factory, *_ = _make_firestore_module_factory([])
        result = resolve_codebase_health_map(manifest, firestore_module_factory=factory)
        assert "repo-a" not in result
        assert result["repo-b"] == {"coverage_pct": 82.0}
