"""Unit tests for routes/checklist.py — helpers + route coverage."""

from __future__ import annotations

import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_trading_library import setup_events

setup_events("deployment-api", "test")

_PATCH_DISABLE_AUTH = "deployment_api.rbac.DISABLE_AUTH"


# ── _ui_status ────────────────────────────────────────────────────────────────


class TestUiStatus:
    def test_pass_maps_to_done(self) -> None:
        from deployment_api.routes.checklist import _ui_status

        assert _ui_status("pass") == "done"

    def test_fail_maps_to_pending(self) -> None:
        from deployment_api.routes.checklist import _ui_status

        assert _ui_status("fail") == "pending"

    def test_na_maps_to_na(self) -> None:
        from deployment_api.routes.checklist import _ui_status

        assert _ui_status("na") == "n/a"

    def test_partial_maps_to_partial(self) -> None:
        from deployment_api.routes.checklist import _ui_status

        assert _ui_status("partial") == "partial"

    def test_none_maps_to_pending(self) -> None:
        from deployment_api.routes.checklist import _ui_status

        assert _ui_status(None) == "pending"

    def test_unknown_maps_to_pending(self) -> None:
        from deployment_api.routes.checklist import _ui_status

        assert _ui_status("some_unknown") == "pending"


# ── _parse_stage_item ─────────────────────────────────────────────────────────


class TestParseStageItem:
    def test_pass_status_returns_done(self) -> None:
        from deployment_api.routes.checklist import _parse_stage_item

        result = _parse_stage_item("cr1_functionality", "CR1", {"status": "pass"})
        assert result is not None
        assert result["status"] == "done"
        assert result["blocking"] is False

    def test_fail_blocking_stage_sets_blocking_true(self) -> None:
        from deployment_api.routes.checklist import _parse_stage_item

        result = _parse_stage_item("cr4_quality_gate", "CR4", {"status": "fail"})
        assert result is not None
        assert result["blocking"] is True

    def test_fail_non_blocking_stage_blocking_false(self) -> None:
        from deployment_api.routes.checklist import _parse_stage_item

        result = _parse_stage_item("cr1_functionality", "CR1", {"status": "fail"})
        assert result is not None
        assert result["blocking"] is False

    def test_non_str_raw_status_defaults_to_not_assessed(self) -> None:
        from deployment_api.routes.checklist import _parse_stage_item

        result = _parse_stage_item("cr1_functionality", "CR1", {"status": 123})
        assert result is not None
        assert result["status"] == "pending"

    def test_not_a_dict_returns_none(self) -> None:
        from deployment_api.routes.checklist import _parse_stage_item

        assert _parse_stage_item("cr1_functionality", "CR1", "not-a-dict") is None

    def test_notes_from_criteria_field(self) -> None:
        from deployment_api.routes.checklist import _parse_stage_item

        result = _parse_stage_item("cr1_functionality", "CR1", {"status": "pass", "criteria": "met all"})
        assert result is not None
        assert result["notes"] == "met all"

    def test_verified_date_from_last_run_date(self) -> None:
        from deployment_api.routes.checklist import _parse_stage_item

        result = _parse_stage_item("cr1_functionality", "CR1", {"status": "pass", "last_run_date": "2024-01-01"})
        assert result is not None
        assert result["verified_date"] == "2024-01-01"


# ── _build_category ───────────────────────────────────────────────────────────


class TestBuildCategory:
    def test_all_done_returns_100_percent(self) -> None:
        from deployment_api.routes.checklist import _CR_STAGES, _build_category

        section: dict[str, object] = {s: {"status": "pass"} for s, _ in _CR_STAGES}
        cat = _build_category("code_readiness", "CR", _CR_STAGES, section)
        assert cat["percent"] == 100

    def test_partial_counts_half(self) -> None:
        from deployment_api.routes.checklist import _build_category

        stages: list[tuple[str, str]] = [("s1", "S1"), ("s2", "S2")]
        section: dict[str, object] = {"s1": {"status": "partial"}, "s2": {"status": "pass"}}
        cat = _build_category("test", "Test", stages, section)
        assert cat["percent"] == 75

    def test_na_items_excluded_from_denominator(self) -> None:
        from deployment_api.routes.checklist import _build_category

        stages: list[tuple[str, str]] = [("s1", "S1"), ("s2", "S2")]
        section: dict[str, object] = {"s1": {"status": "na"}, "s2": {"status": "pass"}}
        cat = _build_category("test", "Test", stages, section)
        assert cat["percent"] == 100

    def test_empty_stages_returns_100_percent(self) -> None:
        from deployment_api.routes.checklist import _build_category

        cat = _build_category("test", "Test", [], {})
        assert cat["percent"] == 100


# ── _parse_codex_checklist ────────────────────────────────────────────────────


class TestParseCodexChecklist:
    def _make_full_data(self) -> dict[str, object]:
        return {
            "repo": "my-service",
            "last_updated": "2024-01-01",
            "code_readiness": {"cr1_functionality": {"status": "pass"}},
            "deployment_readiness": {"batch": {"dr1_infra": {"status": "pass"}}},
            "business_readiness": {"br1_stakeholder_sign_off": {"status": "pass"}},
        }

    def test_basic_parsing_returns_service_name(self) -> None:
        from deployment_api.routes.checklist import _parse_codex_checklist

        result = _parse_codex_checklist(self._make_full_data())
        assert result["service"] == "my-service"
        assert result["schema"] == "codex-v3.0"

    def test_na_reason_in_deployment_readiness(self) -> None:
        from deployment_api.routes.checklist import _parse_codex_checklist

        data: dict[str, object] = {
            "repo": "my-service",
            "deployment_readiness": {"na_reason": "Not applicable for this service"},
        }
        result = _parse_codex_checklist(data)
        categories = result["categories"]
        assert isinstance(categories, list)
        dr_cats = [c for c in categories if c["name"] == "deployment_readiness"]
        assert len(dr_cats) == 1
        assert dr_cats[0]["percent"] == 100

    def test_blocking_items_fail_populates_blocking(self) -> None:
        from deployment_api.routes.checklist import _parse_codex_checklist

        data: dict[str, object] = {
            "code_readiness": {"cr5_quickmerge": {"status": "fail"}},
        }
        result = _parse_codex_checklist(data)
        blocking = result["blocking_items"]
        assert isinstance(blocking, list)
        assert len(blocking) == 1

    def test_readiness_percent_calculated(self) -> None:
        from deployment_api.routes.checklist import _parse_codex_checklist

        data: dict[str, object] = {
            "code_readiness": {
                "cr1_functionality": {"status": "pass"},
                "cr2_unit_tests": {"status": "pass"},
            }
        }
        result = _parse_codex_checklist(data)
        assert isinstance(result["readiness_percent"], int)


# ── _list_all_checklists ──────────────────────────────────────────────────────


class TestListAllChecklists:
    def test_nonexistent_dir_returns_empty(self) -> None:
        from deployment_api.routes.checklist import _list_all_checklists

        result = _list_all_checklists(Path("/nonexistent/path"))
        assert result == []

    def test_valid_yaml_file_included(self) -> None:
        from deployment_api.routes.checklist import _list_all_checklists

        with tempfile.TemporaryDirectory() as tmp:
            yaml_path = Path(tmp) / "my-service.yaml"
            yaml_path.write_text("repo: my-service\nlast_updated: 2024-01-01\n")
            result = _list_all_checklists(Path(tmp))
        assert len(result) == 1
        assert result[0]["service"] == "my-service"

    def test_underscore_prefixed_file_skipped(self) -> None:
        from deployment_api.routes.checklist import _list_all_checklists

        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "_internal.yaml").write_text("repo: internal\n")
            result = _list_all_checklists(Path(tmp))
        assert result == []

    def test_sorted_by_readiness_descending(self) -> None:
        from deployment_api.routes.checklist import _list_all_checklists

        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "svc-a.yaml").write_text(
                "repo: svc-a\ncode_readiness:\n  cr1_functionality:\n    status: pass\n"
            )
            (Path(tmp) / "svc-b.yaml").write_text("repo: svc-b\n")
            result = _list_all_checklists(Path(tmp))
        assert len(result) == 2
        percents = [r["readiness_percent"] for r in result]
        assert percents == sorted(percents, reverse=True)


# ── _get_warnings ─────────────────────────────────────────────────────────────


class TestGetWarnings:
    def test_pending_non_blocking_item_included(self) -> None:
        from deployment_api.routes.checklist import _get_warnings

        checklist: dict[str, object] = {
            "categories": [
                {
                    "name": "cr",
                    "items": [
                        {"id": "cr1", "description": "CR1", "status": "pending", "blocking": False}
                    ],
                }
            ]
        }
        warnings = _get_warnings(checklist)
        assert len(warnings) == 1

    def test_blocking_item_excluded_from_warnings(self) -> None:
        from deployment_api.routes.checklist import _get_warnings

        checklist: dict[str, object] = {
            "categories": [
                {
                    "name": "cr",
                    "items": [
                        {"id": "cr5", "description": "CR5", "status": "pending", "blocking": True}
                    ],
                }
            ]
        }
        warnings = _get_warnings(checklist)
        assert len(warnings) == 0

    def test_done_items_excluded(self) -> None:
        from deployment_api.routes.checklist import _get_warnings

        checklist: dict[str, object] = {
            "categories": [
                {
                    "name": "cr",
                    "items": [{"id": "cr1", "description": "CR1", "status": "done", "blocking": False}],
                }
            ]
        }
        assert _get_warnings(checklist) == []

    def test_capped_at_10(self) -> None:
        from deployment_api.routes.checklist import _get_warnings

        items = [{"id": f"s{i}", "description": f"S{i}", "status": "pending", "blocking": False} for i in range(15)]
        checklist: dict[str, object] = {"categories": [{"name": "cr", "items": items}]}
        assert len(_get_warnings(checklist)) == 10


# ── routes ────────────────────────────────────────────────────────────────────


@pytest.fixture
def client_checklist() -> TestClient:
    from deployment_api.routes.checklist import router

    app = FastAPI()
    app.include_router(router, prefix="/checklists")
    with patch(_PATCH_DISABLE_AUTH, True):
        return TestClient(app, raise_server_exceptions=False)


def test_get_checklist_no_codex_dir_returns_503(client_checklist: TestClient) -> None:
    r = client_checklist.get("/checklists/my-service/checklist")
    assert r.status_code == 503


def test_get_checklist_not_found_returns_404() -> None:
    from deployment_api.routes.checklist import router

    app = FastAPI()
    app.include_router(router, prefix="/checklists")
    with tempfile.TemporaryDirectory() as tmp:
        app.state.codex_dir = Path(tmp)
        with patch(_PATCH_DISABLE_AUTH, True):
            client = TestClient(app, raise_server_exceptions=False)
        r = client.get("/checklists/nonexistent/checklist")
    assert r.status_code == 404


def test_get_checklist_found_returns_200() -> None:
    from deployment_api.routes.checklist import router

    app = FastAPI()
    app.include_router(router, prefix="/checklists")
    with tempfile.TemporaryDirectory() as tmp:
        yaml_path = Path(tmp) / "my-service.yaml"
        yaml_path.write_text("repo: my-service\nlast_updated: 2024-01-01\n")
        app.state.codex_dir = Path(tmp)
        with patch(_PATCH_DISABLE_AUTH, True):
            client = TestClient(app, raise_server_exceptions=False)
        r = client.get("/checklists/my-service/checklist")
    assert r.status_code == 200
    assert r.json()["service"] == "my-service"


def test_validate_checklist_no_codex_dir_returns_not_ready() -> None:
    from deployment_api.routes.checklist import router

    app = FastAPI()
    app.include_router(router, prefix="/checklists")
    with patch(_PATCH_DISABLE_AUTH, True):
        client = TestClient(app, raise_server_exceptions=False)
    r = client.get("/checklists/my-service/checklist/validate")
    assert r.status_code == 200
    data = r.json()
    assert data["ready"] is False
    assert data["readiness_percent"] == 0


def test_validate_checklist_not_found_returns_ok() -> None:
    from deployment_api.routes.checklist import router

    app = FastAPI()
    app.include_router(router, prefix="/checklists")
    with tempfile.TemporaryDirectory() as tmp:
        app.state.codex_dir = Path(tmp)
        with patch(_PATCH_DISABLE_AUTH, True):
            client = TestClient(app, raise_server_exceptions=False)
        r = client.get("/checklists/missing-service/checklist/validate")
    assert r.status_code == 200
    data = r.json()
    assert data["can_proceed_with_acknowledgment"] is True


def test_validate_checklist_found_returns_result() -> None:
    from deployment_api.routes.checklist import router

    app = FastAPI()
    app.include_router(router, prefix="/checklists")
    with tempfile.TemporaryDirectory() as tmp:
        yaml_path = Path(tmp) / "my-service.yaml"
        yaml_path.write_text(
            "repo: my-service\ncode_readiness:\n  cr1_functionality:\n    status: pass\n"
        )
        app.state.codex_dir = Path(tmp)
        with patch(_PATCH_DISABLE_AUTH, True):
            client = TestClient(app, raise_server_exceptions=False)
        r = client.get("/checklists/my-service/checklist/validate")
    assert r.status_code == 200
    data = r.json()
    assert data["service"] == "my-service"
    assert isinstance(data["ready"], bool)


def test_list_checklists_no_codex_dir_returns_empty() -> None:
    from deployment_api.routes.checklist import router

    app = FastAPI()
    app.include_router(router, prefix="/checklists")
    with patch(_PATCH_DISABLE_AUTH, True):
        client = TestClient(app, raise_server_exceptions=False)
    r = client.get("/checklists")
    assert r.status_code == 200
    assert r.json() == {"checklists": [], "count": 0}


def test_list_checklists_with_yaml_returns_list() -> None:
    from deployment_api.routes.checklist import router

    app = FastAPI()
    app.include_router(router, prefix="/checklists")
    with tempfile.TemporaryDirectory() as tmp:
        (Path(tmp) / "svc-a.yaml").write_text("repo: svc-a\n")
        app.state.codex_dir = Path(tmp)
        with patch(_PATCH_DISABLE_AUTH, True):
            client = TestClient(app, raise_server_exceptions=False)
        r = client.get("/checklists")
    assert r.status_code == 200
    data = r.json()
    assert data["count"] == 1
    assert data["checklists"][0]["service"] == "svc-a"
