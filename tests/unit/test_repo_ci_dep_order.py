"""Unit tests for the dep-order promotion-HOLD computation (mirrors staging-to-main.yml STAGE 1.8).

Pure-function tests for `_compute_dep_order(rows, view)` + the two new ManifestView accessors
(`dependencies_for` / `tier_for`). The held scenario: a low-tier dep lagging at STAGING_GREEN
holds every dependent above it from main, exactly like the STAGE 1.8 dep-order gate.
"""

from __future__ import annotations

from deployment_api.routes._repo_ci_manifest import ManifestView, manifest_view_from_raw
from deployment_api.routes._repo_ci_types import (  # pyright: ignore[reportPrivateUsage]
    BranchDeltaDict,
    RepoOverviewDict,
    SitStateDict,
)
from deployment_api.routes.repo_ci import _compute_dep_order  # pyright: ignore[reportPrivateUsage]


def _sit() -> SitStateDict:
    return SitStateDict(
        in_breaking_pending=False,
        staging_locked=False,
        staging_locked_reason=None,
        last_sit_run_status="success",
        last_sit_run_age_min=10,
        stuck_in_sit=False,
    )


def _row(repo: str, ci_status: str, *, staging_main_files: int = 0) -> RepoOverviewDict:
    """A minimal overview row carrying only what _compute_dep_order reads: repo, ci_status, and the
    staging→main delta (for a root blocker's main_files_behind)."""
    deltas: list[BranchDeltaDict] = [
        BranchDeltaDict(base="main", head="staging", ahead_by=1, behind_by=0, files_changed=staging_main_files),
    ]
    return RepoOverviewDict(
        repo=repo,
        repo_type="service",
        ci_status=ci_status,
        branch_ci={},
        branches=[],
        deltas=deltas,
        open_prs=[],
        sit=_sit(),
        image={  # pyright: ignore[reportArgumentType]  # minimal stub; not read by _compute_dep_order
            "last_build_status": None,
            "last_build_sha": None,
            "last_build_time": None,
            "last_build_log_url": None,
            "last_success_sha": None,
            "last_success_time": None,
            "last_success_log_url": None,
            "deployed_version": None,
            "image_stale": None,
        },
        last_green_main=None,
        main_lag_age_min=None,
        main_unpromoted_commits=None,
        drain_stalled=False,
        tier="",
        blocked_by=[],
        blocking=[],
        codebase_health=None,
    )


def _view(repos: dict[str, dict[str, object]]) -> ManifestView:
    return manifest_view_from_raw({"repositories": repos})


def _dep(name: str) -> dict[str, object]:
    return {"name": name, "version": ">=0.1.0,<1.0.0", "required": True}


# A tier-0 dep (uac) lagging at STAGING_GREEN holds two tier-3 dependents from main.
_HELD_REPOS: dict[str, dict[str, object]] = {
    "unified-api-contracts": {"type": "library", "tier": 0, "ci_status": "STAGING_GREEN", "dependencies": []},
    "market-tick-data-service": {
        "type": "service",
        "tier": 3,
        "ci_status": "STAGING_GREEN",
        "dependencies": [_dep("unified-api-contracts")],
    },
    "strategy-service": {
        "type": "service",
        "tier": 3,
        "ci_status": "STAGING_GREEN",
        "dependencies": [_dep("unified-api-contracts")],
    },
}


class TestManifestAccessors:
    def test_dependencies_for_returns_names(self) -> None:
        view = _view(_HELD_REPOS)
        assert view.dependencies_for("market-tick-data-service") == ["unified-api-contracts"]
        assert view.dependencies_for("unified-api-contracts") == []  # no deps
        assert view.dependencies_for("nonexistent") == []  # no manifest entry → fail-open

    def test_dependencies_for_drops_self_and_blank(self) -> None:
        view = _view(
            {
                "a-svc": {
                    "dependencies": [_dep("a-svc"), _dep("b-svc"), {"version": "x"}, "c-svc", 7],
                },
                "b-svc": {},
                "c-svc": {},
            }
        )
        # self-ref dropped, blank-name dict dropped, non-str/dict dropped; bare-string dep kept.
        assert view.dependencies_for("a-svc") == ["b-svc", "c-svc"]

    def test_tier_for_stringifies(self) -> None:
        view = _view(
            {
                "a": {"tier": 0},
                "b": {"tier": "service"},
                "c": {},  # no tier
            }
        )
        assert view.tier_for("a") == "0"
        assert view.tier_for("b") == "service"
        assert view.tier_for("c") == ""
        assert view.tier_for("missing") == ""


class TestComputeDepOrder:
    def test_held_repo_gets_blocked_by(self) -> None:
        view = _view(_HELD_REPOS)
        rows = [
            _row("unified-api-contracts", "STAGING_GREEN", staging_main_files=4),
            _row("market-tick-data-service", "STAGING_GREEN"),
            _row("strategy-service", "STAGING_GREEN"),
        ]
        blocked_by, blocking, held = _compute_dep_order(rows, view)

        # Both dependents are HELD by the tier-0 dep (blocked_by non-empty ⟺ held).
        mtds_blockers = blocked_by["market-tick-data-service"]
        assert len(mtds_blockers) == 1
        assert mtds_blockers[0]["name"] == "unified-api-contracts"
        assert mtds_blockers[0]["tier"] == "0"
        assert mtds_blockers[0]["ci_status"] == "STAGING_GREEN"
        assert len(blocked_by["strategy-service"]) == 1
        # The root dep itself has no pending-dep → blocked_by empty.
        assert blocked_by["unified-api-contracts"] == []

    def test_root_dep_gets_blocking_and_appears_in_root_blockers(self) -> None:
        view = _view(_HELD_REPOS)
        rows = [
            _row("unified-api-contracts", "STAGING_GREEN", staging_main_files=4),
            _row("market-tick-data-service", "STAGING_GREEN"),
            _row("strategy-service", "STAGING_GREEN"),
        ]
        _, blocking, held = _compute_dep_order(rows, view)

        # The inverse map: the root dep blocks both dependents (sorted).
        assert blocking["unified-api-contracts"] == ["market-tick-data-service", "strategy-service"]
        # Held aggregate: both dependents held, one root blocker with blocking_count == 2.
        assert held["held_repos"] == ["market-tick-data-service", "strategy-service"]
        assert len(held["root_blockers"]) == 1
        root = held["root_blockers"][0]
        assert root["repo"] == "unified-api-contracts"
        assert root["tier"] == "0"
        assert root["ci_status"] == "STAGING_GREEN"
        assert root["blocking_count"] == 2
        assert root["main_files_behind"] == 4  # its own staging→main files_changed

    def test_fully_on_main_fleet_yields_empty_promotion_held(self) -> None:
        view = _view(
            {
                "unified-api-contracts": {"tier": 0, "ci_status": "MAIN_GREEN", "dependencies": []},
                "market-tick-data-service": {
                    "tier": 3,
                    "ci_status": "MAIN_GREEN",
                    "dependencies": [_dep("unified-api-contracts")],
                },
            }
        )
        rows = [
            _row("unified-api-contracts", "MAIN_GREEN"),
            _row("market-tick-data-service", "MAIN_GREEN"),
        ]
        blocked_by, blocking, held = _compute_dep_order(rows, view)
        assert held["held_repos"] == []
        assert held["root_blockers"] == []
        assert blocked_by["market-tick-data-service"] == []
        assert blocking["unified-api-contracts"] == []

    def test_repo_with_all_deps_on_main_is_not_held(self) -> None:
        # A dependent with a PENDING promotion (STAGING_GREEN) but whose dep is ALREADY on main
        # (MAIN_GREEN / SIT_VALIDATED) is NOT held — blocked_by empty.
        view = _view(
            {
                "unified-api-contracts": {"tier": 0, "ci_status": "MAIN_GREEN", "dependencies": []},
                "unified-trading-library": {"tier": 0, "ci_status": "SIT_VALIDATED", "dependencies": []},
                "execution-service": {
                    "tier": 3,
                    "ci_status": "STAGING_GREEN",  # itself pending, but deps are on main
                    "dependencies": [_dep("unified-api-contracts"), _dep("unified-trading-library")],
                },
            }
        )
        rows = [
            _row("unified-api-contracts", "MAIN_GREEN"),
            _row("unified-trading-library", "SIT_VALIDATED"),
            _row("execution-service", "STAGING_GREEN"),
        ]
        blocked_by, _, held = _compute_dep_order(rows, view)
        assert blocked_by["execution-service"] == []  # deps on main → not held
        assert held["held_repos"] == []

    def test_no_deps_repo_is_never_held(self) -> None:
        view = _view({"solo-svc": {"tier": 3, "ci_status": "STAGING_GREEN", "dependencies": []}})
        rows = [_row("solo-svc", "STAGING_GREEN")]
        blocked_by, _, held = _compute_dep_order(rows, view)
        assert blocked_by["solo-svc"] == []  # pending promotion but no deps → never held
        assert held["held_repos"] == []

    def test_missing_manifest_data_fails_open(self) -> None:
        # A pending repo whose dep is NOT in the manifest, plus a dep with a blank ci_status →
        # both fail-open (treated as on-main, not blockers) exactly like STAGE 1.8.
        view = _view(
            {
                "dep-blank": {"tier": 0, "dependencies": []},  # ci_status absent → fail-open
                "a-svc": {
                    "tier": 3,
                    "ci_status": "STAGING_GREEN",
                    "dependencies": [_dep("dep-blank"), _dep("not-in-manifest")],
                },
            }
        )
        rows = [
            _row("dep-blank", ""),  # blank ci_status
            _row("a-svc", "STAGING_GREEN"),
        ]
        blocked_by, _, held = _compute_dep_order(rows, view)
        assert blocked_by["a-svc"] == []  # both deps fail-open → not held
        assert held["held_repos"] == []

    def test_root_selection_prefers_bottom_of_stack(self) -> None:
        # Two-tier hold: tier-0 uac (STAGING_GREEN) holds tier-1 utl-derived (STAGING_GREEN) which
        # holds tier-3 svc. The TRUE root is uac (its own blocked_by is empty); utl-derived is held
        # too but is NOT a root (it is itself blocked). svc is held. root_blockers = [uac] only.
        view = _view(
            {
                "uac": {"tier": 0, "ci_status": "STAGING_GREEN", "dependencies": []},
                "mid-lib": {
                    "tier": 1,
                    "ci_status": "STAGING_GREEN",
                    "dependencies": [_dep("uac")],
                },
                "svc": {
                    "tier": 3,
                    "ci_status": "STAGING_GREEN",
                    "dependencies": [_dep("mid-lib")],
                },
            }
        )
        rows = [
            _row("uac", "STAGING_GREEN", staging_main_files=2),
            _row("mid-lib", "STAGING_GREEN"),
            _row("svc", "STAGING_GREEN"),
        ]
        blocked_by, _, held = _compute_dep_order(rows, view)
        # All three are held: uac holds mid-lib, mid-lib holds svc.
        assert held["held_repos"] == ["mid-lib", "svc"]  # uac itself is not held (no pending dep)
        # Only uac is a root (mid-lib is itself blocked by uac).
        assert [rb["repo"] for rb in held["root_blockers"]] == ["uac"]
        assert held["root_blockers"][0]["blocking_count"] == 1  # uac directly blocks mid-lib only

    def test_root_blockers_sorted_lowest_tier_first(self) -> None:
        # Two independent root blockers at different tiers: lowest tier first, then blocking_count
        # desc. uac (tier 0) blocks 1; a tier-1 lib blocks 2 → uac first (tier 0 < 1).
        view = _view(
            {
                "uac": {"tier": 0, "ci_status": "STAGING_GREEN", "dependencies": []},
                "lib1": {"tier": 1, "ci_status": "STAGING_GREEN", "dependencies": []},
                "svc-a": {"tier": 3, "ci_status": "STAGING_GREEN", "dependencies": [_dep("uac")]},
                "svc-b": {"tier": 3, "ci_status": "STAGING_GREEN", "dependencies": [_dep("lib1")]},
                "svc-c": {"tier": 3, "ci_status": "STAGING_GREEN", "dependencies": [_dep("lib1")]},
            }
        )
        rows = [
            _row("uac", "STAGING_GREEN"),
            _row("lib1", "STAGING_GREEN"),
            _row("svc-a", "STAGING_GREEN"),
            _row("svc-b", "STAGING_GREEN"),
            _row("svc-c", "STAGING_GREEN"),
        ]
        _, _, held = _compute_dep_order(rows, view)
        order = [(rb["repo"], rb["blocking_count"]) for rb in held["root_blockers"]]
        assert order == [("uac", 1), ("lib1", 2)]  # tier 0 before tier 1 despite lower blocking_count

    def test_empty_fleet_yields_empty_held(self) -> None:
        blocked_by, blocking, held = _compute_dep_order([], _view({}))
        assert blocked_by == {}
        assert blocking == {}
        assert held == {"held_repos": [], "root_blockers": []}
