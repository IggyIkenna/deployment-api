"""Unit tests for routes/epics.py — helper + route coverage."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import cast
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_trading_library import setup_events

setup_events("deployment-api", "test")

_PATCH_DISABLE_AUTH = "deployment_api.rbac.DISABLE_AUTH"
_PATCH_LOAD_EPICS = "deployment_api.routes.epics._load_all_epics"
_PATCH_LOAD_REPOS = "deployment_api.routes.epics._load_all_repos"


# ── _extract_data_fields ──────────────────────────────────────────────────────


class TestExtractDataFields:
    def test_with_full_data_block(self) -> None:
        from deployment_api.routes.epics import _extract_data_fields

        ac_data: dict[str, object] = {
            "data": {
                "historical_available": True,
                "live_available": True,
                "mock_available": False,
                "testnet_available": True,
                "historical_start_date": "2023-01-01",
            }
        }
        result = _extract_data_fields(ac_data)
        assert result["historical_available"] is True
        assert result["live_available"] is True
        assert result["historical_start_date"] == "2023-01-01"

    def test_no_data_key_returns_defaults(self) -> None:
        from deployment_api.routes.epics import _extract_data_fields

        result = _extract_data_fields({})
        assert result["historical_available"] is False
        assert result["live_available"] is False
        assert result["historical_start_date"] is None

    def test_data_not_dict_returns_defaults(self) -> None:
        from deployment_api.routes.epics import _extract_data_fields

        result = _extract_data_fields({"data": "not-a-dict"})
        assert result["historical_available"] is False


# ── _cr_ordinal / _br_ordinal ────────────────────────────────────────────────


class TestOrdinals:
    def test_cr_ordinals_ordered(self) -> None:
        from deployment_api.routes.epics import _cr_ordinal

        assert _cr_ordinal("cr0") < _cr_ordinal("cr1")
        assert _cr_ordinal("cr3") < _cr_ordinal("cr5")

    def test_cr_ordinal_none_returns_zero(self) -> None:
        from deployment_api.routes.epics import _cr_ordinal

        assert _cr_ordinal(None) == 0

    def test_br_ordinals_ordered(self) -> None:
        from deployment_api.routes.epics import _br_ordinal

        assert _br_ordinal("br0") < _br_ordinal("br5")

    def test_br_ordinal_na_returns_minus_one(self) -> None:
        from deployment_api.routes.epics import _br_ordinal

        assert _br_ordinal("na") == -1

    def test_unknown_cr_returns_zero(self) -> None:
        from deployment_api.routes.epics import _cr_ordinal

        assert _cr_ordinal("cr99") == 0


# ── _get_branch_status ────────────────────────────────────────────────────────


class TestGetBranchStatus:
    def test_returns_branch_status_for_asset_group(self) -> None:
        from deployment_api.routes.epics import _get_branch_status

        repo_data: dict[str, object] = {
            "asset_group_readiness": {
                "defi": {
                    "branch_status": {"main": {"quickmerged": True}},
                }
            }
        }
        result = _get_branch_status(repo_data, "defi")
        assert result["main"] == {"quickmerged": True}

    def test_falls_back_to_core_when_missing(self) -> None:
        from deployment_api.routes.epics import _get_branch_status

        repo_data: dict[str, object] = {
            "asset_group_readiness": {
                "core": {
                    "branch_status": {"main": {"quickmerged": False}},
                }
            }
        }
        result = _get_branch_status(repo_data, "defi")
        assert isinstance(result, dict)

    def test_no_acr_returns_empty(self) -> None:
        from deployment_api.routes.epics import _get_branch_status

        result = _get_branch_status({}, "defi")
        assert result == {}

    def test_acr_not_dict_returns_empty(self) -> None:
        from deployment_api.routes.epics import _get_branch_status

        result = _get_branch_status({"asset_group_readiness": "not-a-dict"}, "defi")
        assert result == {}


# ── _get_asset_group_data ──────────────────────────────────────────────────────


class TestGetAssetGroupData:
    def test_returns_data_for_group(self) -> None:
        from deployment_api.routes.epics import _get_asset_group_data

        repo_data: dict[str, object] = {
            "asset_group_readiness": {
                "cefi": {"feature_groups": ["fg1", "fg2"]},
            }
        }
        result = _get_asset_group_data(repo_data, "cefi")
        assert result["feature_groups"] == ["fg1", "fg2"]

    def test_falls_back_to_core(self) -> None:
        from deployment_api.routes.epics import _get_asset_group_data

        repo_data: dict[str, object] = {
            "asset_group_readiness": {
                "core": {"feature_groups": ["core_fg"]},
            }
        }
        result = _get_asset_group_data(repo_data, "defi")
        assert result.get("feature_groups") == ["core_fg"]

    def test_no_acr_returns_empty(self) -> None:
        from deployment_api.routes.epics import _get_asset_group_data

        assert _get_asset_group_data({}, "defi") == {}


# ── _is_repo_complete ─────────────────────────────────────────────────────────


class TestIsRepoComplete:
    def _make_repo_data(
        self,
        cr: str = "cr5",
        br: str = "br5",
        quickmerged: bool = True,
    ) -> dict[str, object]:
        return {
            "code_readiness": {"current_stage": cr},
            "business_readiness": {"current_stage": br},
            "asset_group_readiness": {
                "core": {
                    "branch_status": {"main": {"quickmerged": quickmerged}},
                }
            },
        }

    def test_complete_repo_returns_true(self) -> None:
        from deployment_api.routes.epics import _is_repo_complete

        ok, reason = _is_repo_complete(self._make_repo_data(), "cr3", "na", "core")
        assert ok is True
        assert reason == ""

    def test_cr_too_low_returns_false(self) -> None:
        from deployment_api.routes.epics import _is_repo_complete

        ok, reason = _is_repo_complete(self._make_repo_data(cr="cr1"), "cr5", "na", "core")
        assert ok is False
        assert "CR" in reason

    def test_br_too_low_returns_false(self) -> None:
        from deployment_api.routes.epics import _is_repo_complete

        ok, reason = _is_repo_complete(self._make_repo_data(br="br1"), "cr3", "br5", "core")
        assert ok is False
        assert "BR" in reason

    def test_not_quickmerged_returns_false(self) -> None:
        from deployment_api.routes.epics import _is_repo_complete

        ok, reason = _is_repo_complete(self._make_repo_data(quickmerged=False), "cr3", "na", "core")
        assert ok is False
        assert "quickmerged" in reason

    def test_br_na_skips_br_check(self) -> None:
        from deployment_api.routes.epics import _is_repo_complete

        ok, _ = _is_repo_complete(self._make_repo_data(br="br0"), "cr3", "na", "core")
        assert ok is True


# ── _compute_epic ─────────────────────────────────────────────────────────────


class TestComputeEpic:
    def test_empty_required_repos_returns_100_pct(self) -> None:
        from deployment_api.routes.epics import _compute_epic

        epic: dict[str, object] = {
            "epic_id": "defi",
            "display_name": "DeFi",
            "mvp_priority": 1,
            "required_repos": [],
            "optional_repos": [],
        }
        result = _compute_epic(epic, {})
        assert result["epic_pct"] == 0
        assert result["epic_complete"] is False

    def test_missing_repo_adds_blocking(self) -> None:
        from deployment_api.routes.epics import _compute_epic

        epic: dict[str, object] = {
            "epic_id": "defi",
            "required_repos": [{"repo": "missing-repo", "min_stage": "cr3"}],
        }
        result = _compute_epic(epic, {})
        blocking = cast(list[dict[str, object]], result["blocking_repos"])
        assert len(blocking) == 1
        assert blocking[0]["repo"] == "missing-repo"
        assert "No per-repo YAML" in str(blocking[0]["blocking_reason"])

    def test_complete_repo_increments_count(self) -> None:
        from deployment_api.routes.epics import _compute_epic

        repo_data: dict[str, dict[str, object]] = {
            "my-service": {
                "code_readiness": {"current_stage": "cr5"},
                "business_readiness": {"current_stage": "br5"},
                "asset_group_readiness": {"core": {"branch_status": {"main": {"quickmerged": True}}}},
            }
        }
        epic: dict[str, object] = {
            "epic_id": "defi",
            "required_repos": [{"repo": "my-service", "min_stage": "cr3", "min_br_stage": "na", "asset_group": "core"}],
        }
        result = _compute_epic(epic, repo_data)
        assert result["completed"] == 1
        assert result["epic_pct"] == 100

    def test_optional_repos_included(self) -> None:
        from deployment_api.routes.epics import _compute_epic

        epic: dict[str, object] = {
            "epic_id": "defi",
            "required_repos": [],
            "optional_repos": [{"repo": "opt-service", "asset_group": "defi", "note": "nice to have"}],
        }
        result = _compute_epic(epic, {})
        optional_status = cast(list[dict[str, object]], result["optional_repos_status"])
        assert len(optional_status) == 1
        assert optional_status[0]["repo"] == "opt-service"
        assert optional_status[0]["yaml_present"] is False


# ── _sort_key ─────────────────────────────────────────────────────────────────


class TestSortKey:
    def test_defi_before_cefi(self) -> None:
        from deployment_api.routes.epics import _sort_key

        assert _sort_key({"epic_id": "defi"}) < _sort_key({"epic_id": "cefi"})

    def test_unknown_epic_returns_99(self) -> None:
        from deployment_api.routes.epics import _sort_key

        assert _sort_key({"epic_id": "unknown"}) == 99


# ── list_epics route ──────────────────────────────────────────────────────────


@pytest.fixture
def client_epics() -> TestClient:
    from deployment_api.routes.epics import router

    app = FastAPI()
    app.include_router(router, prefix="/epics")
    with patch(_PATCH_DISABLE_AUTH, True):
        return TestClient(app, raise_server_exceptions=False)


def test_list_epics_no_epics_dir_returns_503(client_epics: TestClient) -> None:
    r = client_epics.get("/epics")
    assert r.status_code == 503


def test_list_epics_with_empty_epics_returns_empty_list(client_epics: TestClient) -> None:
    from fastapi import FastAPI

    from deployment_api.routes.epics import router

    app = FastAPI()
    app.include_router(router, prefix="/epics")

    with tempfile.TemporaryDirectory() as tmp:
        app.state.epics_dir = Path(tmp)
        app.state.codex_dir = Path(tmp)

        with patch(_PATCH_DISABLE_AUTH, True):
            client = TestClient(app, raise_server_exceptions=False)
        with patch(_PATCH_LOAD_EPICS, return_value=[]):
            r = client.get("/epics")

    assert r.status_code == 200
    assert r.json() == []


def test_list_epics_with_epics_returns_summaries() -> None:
    from fastapi import FastAPI

    from deployment_api.routes.epics import router

    app = FastAPI()
    app.include_router(router, prefix="/epics")

    with tempfile.TemporaryDirectory() as tmp:
        app.state.epics_dir = Path(tmp)
        app.state.codex_dir = Path(tmp)

        mock_epic: dict[str, object] = {
            "epic_id": "defi",
            "display_name": "DeFi",
            "mvp_priority": 1,
            "required_repos": [],
            "optional_repos": [],
        }

        with patch(_PATCH_DISABLE_AUTH, True):
            client = TestClient(app, raise_server_exceptions=False)
        with (
            patch(_PATCH_LOAD_EPICS, return_value=[mock_epic]),
            patch(_PATCH_LOAD_REPOS, return_value={}),
        ):
            r = client.get("/epics")

    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["epic_id"] == "defi"


def test_get_epic_detail_no_config_returns_503() -> None:
    from fastapi import FastAPI

    from deployment_api.routes.epics import router

    app = FastAPI()
    app.include_router(router, prefix="/epics")

    with patch(_PATCH_DISABLE_AUTH, True):
        client = TestClient(app, raise_server_exceptions=False)
    r = client.get("/epics/defi")
    assert r.status_code == 503


def test_get_epic_detail_not_found_returns_404() -> None:
    from fastapi import FastAPI

    from deployment_api.routes.epics import router

    app = FastAPI()
    app.include_router(router, prefix="/epics")

    with tempfile.TemporaryDirectory() as tmp:
        app.state.epics_dir = Path(tmp)
        app.state.codex_dir = Path(tmp)

        with patch(_PATCH_DISABLE_AUTH, True):
            client = TestClient(app, raise_server_exceptions=False)
        r = client.get("/epics/nonexistent")

    assert r.status_code == 404


def test_get_epic_detail_found_returns_full_result() -> None:
    from fastapi import FastAPI

    from deployment_api.routes.epics import router

    app = FastAPI()
    app.include_router(router, prefix="/epics")

    with tempfile.TemporaryDirectory() as tmp:
        epic_path = Path(tmp) / "defi-epic.yaml"
        epic_path.write_text("epic_id: defi\ndisplay_name: DeFi\nmvp_priority: 1\n")

        app.state.epics_dir = Path(tmp)
        app.state.codex_dir = Path(tmp)

        with patch(_PATCH_DISABLE_AUTH, True):
            client = TestClient(app, raise_server_exceptions=False)
        with patch(_PATCH_LOAD_REPOS, return_value={}):
            r = client.get("/epics/defi")

    assert r.status_code == 200
    data = r.json()
    assert data["epic_id"] == "defi"
