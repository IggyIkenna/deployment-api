"""Unit tests for the Epics-tab-v2 live PM epics + plan drilldown (operator add 2026-06-10).

Covers the pure parsers (frontmatter + checkbox counts), the grouping/orphan derivation,
and the mock-mode route shape (the UI/playwright contract).
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import cast
from unittest.mock import AsyncMock, patch

import aiohttp
import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from unified_trading_library import setup_events

setup_events("deployment-api", "test")

from deployment_api.routes._epics_plans import (
    _count_checkboxes,
    _is_plan_md,
    _normalize_epic_ref,
    _parse_frontmatter,
    _str_field,
)

_PATCH_MOCK_MODE = "deployment_api.routes.epics.DeploymentApiConfig.is_mock_mode"


@pytest.fixture
def client_epics() -> TestClient:
    from deployment_api.routes.epics import router

    app = FastAPI()
    app.include_router(router, prefix="/api/epics")
    return TestClient(app, raise_server_exceptions=False)


class TestNormalizeEpicRef:
    """parent_epic is declared 3 ways across the repo; all must collapse to one slug so a
    path-form reference (e.g. asset-group canonicalisation plans → epics/mtds_mdps_master.md)
    is NOT wrongly orphaned (regression: operator-reported 0-plan/false-orphan counts 2026-06-11)."""

    def test_all_forms_collapse_to_bare_slug(self) -> None:
        assert _normalize_epic_ref("mtds_mdps_master") == "mtds_mdps_master"
        assert _normalize_epic_ref("epics/mtds_mdps_master.md") == "mtds_mdps_master"
        assert _normalize_epic_ref("plans/epics/infrastructure_master.md") == "infrastructure_master"
        # case-insensitive + whitespace-trimmed
        assert _normalize_epic_ref("  Infrastructure_Master  ") == "infrastructure_master"

    def test_distinct_epics_stay_distinct(self) -> None:
        assert _normalize_epic_ref("epics/cefi_master.md") != _normalize_epic_ref("epics/defi_master.md")


class TestIsPlanMd:
    """plans/active/ housekeeping files are NOT plans and must never reach the orphan strip
    (regression: operator-reported 2026-06-11 — INDEX/_agent_pings/task_template shown as
    review-blocking orphans)."""

    def test_housekeeping_files_excluded(self) -> None:
        for name in ("INDEX.md", "task_template.md", "README.md", "_agent_pings.md"):
            assert not _is_plan_md("file", name), name

    def test_real_plans_and_epics_included(self) -> None:
        assert _is_plan_md("file", "cefi_manifest_canonicalisation_2026_06_01.md")
        assert _is_plan_md("file", "mtds_mdps_master.md")

    def test_non_md_and_dirs_excluded(self) -> None:
        assert not _is_plan_md("dir", "issues")
        assert not _is_plan_md("file", "notes.txt")
        assert not _is_plan_md("file", None)


class TestFrontmatterRobustness:
    """A plan must never be silently orphaned because its frontmatter is invalid YAML — live PM
    plans have prettier-wrapped multi-line `title:` (\\ continuation) + plain `source:` lists with
    embedded `:`/quotes that make `yaml.safe_load` RAISE (regression: 2 plans orphaned 2026-06-11
    despite a valid parent_epic). The line-based fallback must still recover the scalar fields."""

    _MALFORMED = (
        "---\n"
        'title: "CI-status side store — move ci_status from the git \\\n'
        'manifest to Firestore (doc-per-repo + CAS-on-rank)"\n'
        "parent_epic: infrastructure_master\n"
        "status: active\n"
        "tier: L4\n"
        "source:\n"
        '  - operator design direction 2026-06-10 ("ci_status commit\\\n'
        " noise — what's a side store + how are races handled\")\n"
        "---\n# body\n- [ ] todo\n"
    )

    def test_invalid_yaml_falls_back_to_line_scalars(self) -> None:
        fm = _parse_frontmatter(self._MALFORMED)
        assert _str_field(fm, "parent_epic") == "infrastructure_master"
        assert _str_field(fm, "status") == "active"
        assert _str_field(fm, "tier") == "L4"

    def test_indented_list_lines_do_not_leak_into_scalars(self) -> None:
        fm = _parse_frontmatter(self._MALFORMED)
        assert "operator design direction" not in str(fm.get("source", ""))


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


def _blob_entry(name: str, text: str, truncated: bool = False) -> dict[str, object]:
    return {"name": name, "type": "blob", "object": {"text": text, "isTruncated": truncated}}


_EPIC_TEXT = "---\nname: mtds_mdps_master\ntitle: MTDS Master\ntier: L1\npriority: P0\nstatus: active\n---\n"
_PLAN_TEXT = "---\nparent_epic: epics/mtds_mdps_master.md\nstatus: active\nestimate_class: infra\n---\n- [x] a\n- [ ] [CODE] P1. b\n"


def _gql_fixture() -> dict[str, object]:
    return {
        "repository": {
            "epics": {"entries": [_blob_entry("mtds_mdps_master.md", _EPIC_TEXT), _blob_entry("README.md", "x")]},
            "active": {
                "entries": [
                    _blob_entry("some_plan_2026_06_01.md", _PLAN_TEXT),
                    _blob_entry("INDEX.md", "x"),
                    _blob_entry("_agent_pings.md", "x"),
                ]
            },
        }
    }


class TestQuotaBudget:
    """The 2026-06-11 rate-limit fix: ONE GraphQL call per cold load (was ~92 REST calls —
    live 503 'GitHub rate limit exhausted' reproduced), and on GitHub failure the LAST
    cached payload is served with stale=True instead of a 503 (degraded ≠ blank)."""

    @pytest.fixture(autouse=True)
    def _fresh_cache(self) -> Iterator[None]:
        import deployment_api.routes._epics_plans as mod

        mod._cache = None  # pyright: ignore[reportPrivateUsage]
        yield
        mod._cache = None  # pyright: ignore[reportPrivateUsage]

    async def test_cold_load_is_one_github_call(self) -> None:
        from deployment_api.routes import _epics_plans as mod

        gql = AsyncMock(return_value=_gql_fixture())
        with patch.object(mod, "gh_graphql", gql), patch.object(mod, "gh_raw_file", AsyncMock()) as raw:
            result = await mod.load_epics_plans(cast(aiohttp.ClientSession, None), "tok")
        assert gql.await_count == 1  # the whole cold load
        assert raw.await_count == 0  # no per-file REST fallback needed
        assert result["stale"] is False
        assert result["orphan_count"] == 0  # path-form parent groups; housekeeping excluded
        assert result["epics"][0]["plan_count"] == 1

    async def test_cached_load_is_zero_calls(self) -> None:
        from deployment_api.routes import _epics_plans as mod

        gql = AsyncMock(return_value=_gql_fixture())
        with patch.object(mod, "gh_graphql", gql):
            await mod.load_epics_plans(cast(aiohttp.ClientSession, None), "tok")
            await mod.load_epics_plans(cast(aiohttp.ClientSession, None), "tok")
        assert gql.await_count == 1  # second load served from the 300 s TTL cache

    async def test_rate_limit_serves_stale_cache_not_503(self) -> None:
        from deployment_api.routes import _epics_plans as mod

        gql = AsyncMock(return_value=_gql_fixture())
        with patch.object(mod, "gh_graphql", gql):
            first = await mod.load_epics_plans(cast(aiohttp.ClientSession, None), "tok")
        assert mod._cache is not None  # pyright: ignore[reportPrivateUsage]
        # Expire the TTL, then make GitHub rate-limited: the cached payload must come back stale.
        mod._cache = (mod._cache[0] - 10_000.0, mod._cache[1])  # pyright: ignore[reportPrivateUsage]
        gql_limited = AsyncMock(side_effect=HTTPException(status_code=503, detail="rate limited"))
        with patch.object(mod, "gh_graphql", gql_limited):
            second = await mod.load_epics_plans(cast(aiohttp.ClientSession, None), "tok")
        assert second["stale"] is True
        assert second["epics"] == first["epics"]

    async def test_cold_start_with_no_cache_propagates(self) -> None:
        from deployment_api.routes import _epics_plans as mod

        gql_limited = AsyncMock(side_effect=HTTPException(status_code=503, detail="rate limited"))
        with patch.object(mod, "gh_graphql", gql_limited), pytest.raises(HTTPException):
            await mod.load_epics_plans(cast(aiohttp.ClientSession, None), "tok")

    async def test_truncated_blob_falls_back_to_rest_for_that_file_only(self) -> None:
        from deployment_api.routes import _epics_plans as mod

        fixture = _gql_fixture()
        repo = cast(dict[str, object], fixture["repository"])
        active = cast(dict[str, object], repo["active"])
        cast(list[object], active["entries"]).append(_blob_entry("big_plan_2026_06_01.md", "", truncated=True))
        gql = AsyncMock(return_value=fixture)
        raw = AsyncMock(return_value=_PLAN_TEXT)
        with patch.object(mod, "gh_graphql", gql), patch.object(mod, "gh_raw_file", raw):
            result = await mod.load_epics_plans(cast(aiohttp.ClientSession, None), "tok")
        assert gql.await_count == 1
        assert raw.await_count == 1  # exactly the truncated file
        slugs = {p["slug"] for p in result["epics"][0]["plans"]}
        assert "big_plan_2026_06_01" in slugs


class TestEpicsPlansMockRoute:
    def test_plans_route_not_captured_by_epic_id(self, client_epics: TestClient) -> None:
        # `/plans` must resolve to the v2 endpoint, NOT the `/{epic_id}` path param.
        with patch(_PATCH_MOCK_MODE, return_value=True):
            resp = client_epics.get("/api/epics/plans")
        assert resp.status_code == 200
        body = resp.json()
        assert body["source"] == "mock"
        assert {"generated_at", "epics", "orphans", "orphan_count", "stale"} <= set(body)

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
