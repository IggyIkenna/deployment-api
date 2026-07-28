"""Unit tests for _verdict_store_reader — fixture-injected fake Firestore module.

Mirrors test_ci_status_firestore_store.py's structure, generalized to an arbitrary collection
name (version_coherence_verdicts / change_freeze_verdicts) instead of the hardcoded ci_status one.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from deployment_api.routes._verdict_store_reader import (
    CHANGE_FREEZE_COLLECTION,
    VERSION_COHERENCE_COLLECTION,
    get_all_verdicts,
    get_all_verdicts_with_status,
    get_verdict,
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


class TestGetAllVerdicts:
    def test_reads_every_doc_in_collection(self) -> None:
        docs = [
            _make_doc("repo-a", {"verdict": "OK", "reasons": []}),
            _make_doc("repo-b", {"verdict": "VERSION_SPLIT", "reasons": ["x"]}),
        ]
        factory, *_ = _make_firestore_module_factory(docs)
        result = get_all_verdicts(VERSION_COHERENCE_COLLECTION, firestore_module_factory=factory)
        assert result["repo-a"]["verdict"] == "OK"
        assert result["repo-b"]["verdict"] == "VERSION_SPLIT"

    def test_empty_collection_returns_empty_dict(self) -> None:
        factory, *_ = _make_firestore_module_factory([])
        assert get_all_verdicts(VERSION_COHERENCE_COLLECTION, firestore_module_factory=factory) == {}

    def test_collection_queried_with_given_name(self) -> None:
        factory, _module, client_mock, _coll = _make_firestore_module_factory([])
        get_all_verdicts(CHANGE_FREEZE_COLLECTION, firestore_module_factory=factory)
        client_mock.collection.assert_called_once_with(CHANGE_FREEZE_COLLECTION)

    def test_project_id_passed_to_client(self) -> None:
        factory, module_mock, _client, _coll = _make_firestore_module_factory([])
        get_all_verdicts(VERSION_COHERENCE_COLLECTION, project_id="my-project", firestore_module_factory=factory)
        module_mock.Client.assert_called_once_with(project="my-project")

    def test_doc_with_no_data_yields_empty_dict_value(self) -> None:
        doc = MagicMock()
        doc.id = "repo-a"
        doc.to_dict.return_value = None
        factory, *_ = _make_firestore_module_factory([doc])
        result = get_all_verdicts(VERSION_COHERENCE_COLLECTION, firestore_module_factory=factory)
        assert result["repo-a"] == {}


class TestGetVerdict:
    def test_returns_matching_doc(self) -> None:
        docs = [_make_doc("PROD_DEPLOY", {"verdict": "BLOCKED", "reasons": ["freeze window"]})]
        factory, *_ = _make_firestore_module_factory(docs)
        doc = get_verdict(CHANGE_FREEZE_COLLECTION, "PROD_DEPLOY", firestore_module_factory=factory)
        assert doc["verdict"] == "BLOCKED"

    def test_missing_key_returns_empty_dict(self) -> None:
        factory, *_ = _make_firestore_module_factory([])
        assert get_verdict(CHANGE_FREEZE_COLLECTION, "AUTONOMOUS", firestore_module_factory=factory) == {}


class TestUnavailability:
    def test_get_all_verdicts_degrades_to_empty_on_exception(self) -> None:
        def broken_factory():
            raise RuntimeError("connection refused")

        assert get_all_verdicts(VERSION_COHERENCE_COLLECTION, firestore_module_factory=broken_factory) == {}

    def test_warning_logged_on_firestore_error(self, caplog: pytest.LogCaptureFixture) -> None:
        def raise_os_error():
            raise OSError("timeout")

        with caplog.at_level(logging.WARNING):
            get_all_verdicts(VERSION_COHERENCE_COLLECTION, firestore_module_factory=raise_os_error)
        assert any("verdict-store Firestore read unavailable" in r.message for r in caplog.records)

    def test_get_all_verdicts_with_status_reports_unavailable(self) -> None:
        def broken_factory():
            raise RuntimeError("boom")

        docs, available = get_all_verdicts_with_status(
            VERSION_COHERENCE_COLLECTION, firestore_module_factory=broken_factory
        )
        assert docs == {}
        assert available is False

    def test_get_all_verdicts_with_status_distinguishes_empty_from_unavailable(self) -> None:
        """The whole reason this two-value form exists: an EMPTY-but-reachable collection must not
        be conflated with an unreachable one — a plain dict-empty check on the read alone can't
        tell them apart."""
        factory, *_ = _make_firestore_module_factory([])
        docs, available = get_all_verdicts_with_status(VERSION_COHERENCE_COLLECTION, firestore_module_factory=factory)
        assert docs == {}
        assert available is True
