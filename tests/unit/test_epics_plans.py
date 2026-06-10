"""Unit tests for the Epics-tab-v2 live PM epics + plan drilldown (operator add 2026-06-10).

Covers the pure parsers (frontmatter + checkbox counts), the grouping/orphan derivation,
and the mock-mode route shape (the UI/playwright contract).
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_trading_library import setup_events

setup_events("deployment-api", "test")

from deployment_api.routes._epics_plans import _count_checkboxes, _parse_frontmatter, _str_field

_PATCH_MOCK_MODE = "deployment_api.routes.epics.DeploymentApiConfig.is_mock_mode"


@pytest.fixture
def client_epics() -> TestClient:
    from deployment_api.routes.epics import router

    app = FastAPI()
    app.include_router(router, prefix="/api/epics")
    return TestClient(app, raise_server_exceptions=False)


class TestParsers:
    def test_parse_frontmatter(self) -> None:
        text = "---\nname: observability_master\ntier: L4\npriority: P0\n---\n# body\n- [ ] todo"
        fm = _parse_frontmatter(text)
        assert fm["name"] == "observability_master"
        assert _str_field(fm, "tier") == "L4"
        assert _str_field(fm, "missing") == ""

    def test_parse_frontmatter_absent(self) -> None:
        assert _parse_frontmatter("# no frontmatter\n- [ ] x") == {}

    def test_count_checkboxes(self) -> None:
        text = (
            "- [x] [CODE] P0. done one\n"
            "- [X] done two\n"
            "- [ ] [CODE] P1. open p1\n"
            "- [ ] [DOCS] P3. open p3\n"
            "  - [ ] nested open (no priority)\n"
            "not a checkbox\n"
        )
        done, open_, open_p01 = _count_checkboxes(text)
        assert done == 2
        assert open_ == 3
        assert open_p01 == 1  # only the P1 line (P3 + nested-no-priority excluded)


class TestEpicsPlansMockRoute:
    def test_plans_route_not_captured_by_epic_id(self, client_epics: TestClient) -> None:
        # `/plans` must resolve to the v2 endpoint, NOT the `/{epic_id}` path param.
        with patch(_PATCH_MOCK_MODE, return_value=True):
            resp = client_epics.get("/api/epics/plans")
        assert resp.status_code == 200
        body = resp.json()
        assert body["source"] == "mock"
        assert {"generated_at", "epics", "orphans", "orphan_count"} <= set(body)

    def test_mock_shape_cards_drilldown_orphans(self, client_epics: TestClient) -> None:
        with patch(_PATCH_MOCK_MODE, return_value=True):
            body = client_epics.get("/api/epics/plans").json()
        obs = next(e for e in body["epics"] if e["name"] == "observability_master")
        assert obs["tier"] == "L4"
        assert obs["plan_count"] == 2
        assert obs["plans"], "epic carries its active-plan drilldown"
        plan = obs["plans"][0]
        assert {"slug", "parent_epic", "done", "open", "open_p0p1", "pct", "github_url"} <= set(plan)
        # Orphans (no parent_epic) are surfaced, not dropped.
        assert body["orphan_count"] == 1
        assert body["orphans"][0]["parent_epic"] == ""
