"""Unit tests for _ci_status_firestore_store — fixture-injected fake Firestore module."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from deployment_api.routes._ci_status_firestore_store import (
    COLLECTION,
    RELEASE_COLLECTION,
    resolve_ci_status_map,
    resolve_release_version_map,
)

# ── Fake Firestore plumbing ─────────────────────────────────────────────────────


def _make_doc(doc_id: str, data: dict[str, object]) -> MagicMock:
    doc = MagicMock()
    doc.id = doc_id
    doc.to_dict.return_value = data
    return doc


def _make_firestore_module_factory(docs: list[MagicMock]):
    """Build a fake firestore_module_factory that returns the given collection docs."""
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


class TestResolveWithFirestore:
    def test_firestore_overlay_overwrites_manifest(self) -> None:
        manifest: dict[str, object] = {
            "repositories": {
                "repo-a": {"ci_status": "STAGING_GREEN"},
                "repo-b": {"ci_status": "FEATURE_GREEN"},
            }
        }
        docs = [
            _make_doc("repo-a", {"status": "MAIN_GREEN", "rank": 4}),
        ]
        factory, *_ = _make_firestore_module_factory(docs)
        result = resolve_ci_status_map(manifest, firestore_module_factory=factory)
        # Firestore wins for repo-a; manifest value kept for repo-b
        assert result["repo-a"] == "MAIN_GREEN"
        assert result["repo-b"] == "FEATURE_GREEN"

    def test_firestore_adds_repo_absent_from_manifest(self) -> None:
        manifest: dict[str, object] = {"repositories": {"repo-a": {"ci_status": "MAIN_GREEN"}}}
        docs = [
            _make_doc("repo-a", {"status": "MAIN_GREEN"}),
            _make_doc("repo-z", {"status": "STAGING_GREEN"}),
        ]
        factory, *_ = _make_firestore_module_factory(docs)
        result = resolve_ci_status_map(manifest, firestore_module_factory=factory)
        assert result["repo-z"] == "STAGING_GREEN"

    def test_empty_firestore_returns_manifest_map(self) -> None:
        manifest: dict[str, object] = {"repositories": {"repo-a": {"ci_status": "MAIN_GREEN"}}}
        factory, *_ = _make_firestore_module_factory([])
        result = resolve_ci_status_map(manifest, firestore_module_factory=factory)
        assert result == {"repo-a": "MAIN_GREEN"}

    def test_firestore_doc_missing_status_field_ignored(self) -> None:
        manifest: dict[str, object] = {"repositories": {"repo-a": {"ci_status": "MAIN_GREEN"}}}
        docs = [_make_doc("repo-a", {"rank": 4})]  # no "status" key
        factory, *_ = _make_firestore_module_factory(docs)
        result = resolve_ci_status_map(manifest, firestore_module_factory=factory)
        # blank/absent status → not overlaid, manifest value kept
        assert result["repo-a"] == "MAIN_GREEN"

    def test_collection_queried_with_canonical_name(self) -> None:
        factory, module_mock, client_mock, collection_mock = _make_firestore_module_factory([])
        resolve_ci_status_map({}, firestore_module_factory=factory)
        client_mock.collection.assert_called_once_with(COLLECTION)

    def test_project_id_passed_to_client(self) -> None:
        factory, module_mock, client_mock, _ = _make_firestore_module_factory([])
        resolve_ci_status_map({}, project_id="my-project", firestore_module_factory=factory)
        module_mock.Client.assert_called_once_with(project="my-project")


class TestResolveFirestoreUnavailable:
    def test_degrades_to_manifest_on_exception(self) -> None:
        manifest: dict[str, object] = {"repositories": {"repo-a": {"ci_status": "MAIN_GREEN"}}}

        def broken_factory():
            raise RuntimeError("connection refused")

        result = resolve_ci_status_map(manifest, firestore_module_factory=broken_factory)
        assert result == {"repo-a": "MAIN_GREEN"}

    def test_warning_logged_on_firestore_error(self, caplog: pytest.LogCaptureFixture) -> None:
        def raise_os_error():
            raise OSError("timeout")

        with caplog.at_level(logging.WARNING):
            resolve_ci_status_map({}, firestore_module_factory=raise_os_error)
        assert any("Firestore read unavailable" in r.message for r in caplog.records)

    def test_empty_manifest_and_firestore_unavailable_returns_empty(self) -> None:
        def broken_factory():
            raise Exception("sdk not installed")

        result = resolve_ci_status_map({}, firestore_module_factory=broken_factory)
        assert result == {}


class TestManifestCiStatusSeed:
    def test_list_format_repositories_tolerated(self) -> None:
        """resolve_ci_status_map handles list-form repositories (the PM store supports both)."""
        manifest: dict[str, object] = {
            "repositories": [
                {"name": "repo-a", "ci_status": "MAIN_GREEN"},
                {"name": "repo-b", "ci_status": "STAGING_GREEN"},
            ]
        }
        factory, *_ = _make_firestore_module_factory([])
        result = resolve_ci_status_map(manifest, firestore_module_factory=factory)
        assert result == {"repo-a": "MAIN_GREEN", "repo-b": "STAGING_GREEN"}

    def test_blank_ci_status_in_manifest_excluded(self) -> None:
        manifest: dict[str, object] = {
            "repositories": {
                "repo-a": {"ci_status": ""},  # blank → excluded from base
                "repo-b": {"ci_status": "MAIN_GREEN"},
            }
        }
        factory, *_ = _make_firestore_module_factory([])
        result = resolve_ci_status_map(manifest, firestore_module_factory=factory)
        assert "repo-a" not in result
        assert result["repo-b"] == "MAIN_GREEN"


class TestResolveReleaseVersionMap:
    """resolve_release_version_map reads repo_state/{repo}.release_tag.version (the PM registry)."""

    def test_firestore_release_overlays_manifest(self) -> None:
        manifest: dict[str, object] = {"versions": {"repo-a": "1.2.3", "repo-b": "0.4.0"}}
        docs = [_make_doc("repo-a", {"release_tag": {"version": "1.3.0", "tag": "v1.3.0"}})]
        factory, *_ = _make_firestore_module_factory(docs)
        result = resolve_release_version_map(manifest, firestore_module_factory=factory)
        # Firestore wins for repo-a; manifest value kept for repo-b
        assert result["repo-a"] == "1.3.0"
        assert result["repo-b"] == "0.4.0"

    def test_firestore_adds_repo_absent_from_manifest(self) -> None:
        manifest: dict[str, object] = {"versions": {"repo-a": "1.0.0"}}
        docs = [
            _make_doc("repo-a", {"release_tag": {"version": "1.0.0"}}),
            _make_doc("repo-z", {"release_tag": {"version": "2.5.1"}}),
        ]
        factory, *_ = _make_firestore_module_factory(docs)
        result = resolve_release_version_map(manifest, firestore_module_factory=factory)
        assert result["repo-z"] == "2.5.1"

    def test_doc_without_release_tag_ignored(self) -> None:
        """A repo_state doc carrying only ci_failure/promotion_lag (no release_tag) is skipped."""
        manifest: dict[str, object] = {"versions": {"repo-a": "1.0.0"}}
        docs = [_make_doc("repo-a", {"ci_failure": {"count": 2}})]  # no release_tag
        factory, *_ = _make_firestore_module_factory(docs)
        result = resolve_release_version_map(manifest, firestore_module_factory=factory)
        assert result["repo-a"] == "1.0.0"  # manifest value kept

    def test_release_tag_without_version_field_ignored(self) -> None:
        manifest: dict[str, object] = {"versions": {"repo-a": "1.0.0"}}
        docs = [_make_doc("repo-a", {"release_tag": {"tag": "v?", "sha": "abc"}})]  # no version
        factory, *_ = _make_firestore_module_factory(docs)
        result = resolve_release_version_map(manifest, firestore_module_factory=factory)
        assert result["repo-a"] == "1.0.0"

    def test_empty_firestore_returns_manifest_map(self) -> None:
        manifest: dict[str, object] = {"versions": {"repo-a": "1.0.0", "repo-b": ""}}
        factory, *_ = _make_firestore_module_factory([])
        result = resolve_release_version_map(manifest, firestore_module_factory=factory)
        assert result == {"repo-a": "1.0.0"}  # blank manifest version excluded

    def test_queries_repo_state_collection(self) -> None:
        factory, _module, client_mock, _coll = _make_firestore_module_factory([])
        resolve_release_version_map({}, firestore_module_factory=factory)
        client_mock.collection.assert_called_once_with(RELEASE_COLLECTION)

    def test_degrades_to_manifest_on_exception(self) -> None:
        manifest: dict[str, object] = {"versions": {"repo-a": "1.0.0"}}

        def broken_factory():
            raise RuntimeError("connection refused")

        result = resolve_release_version_map(manifest, firestore_module_factory=broken_factory)
        assert result == {"repo-a": "1.0.0"}

    def test_warning_logged_on_firestore_error(self, caplog: pytest.LogCaptureFixture) -> None:
        def raise_os_error():
            raise OSError("timeout")

        with caplog.at_level(logging.WARNING):
            resolve_release_version_map({}, firestore_module_factory=raise_os_error)
        assert any("version_registry Firestore read unavailable" in r.message for r in caplog.records)
