"""
Mock fixtures for the repo-CI dashboard — drive UI dev + playwright mock-mode suite.

Extracted from repo_ci.py (commit 94e4feb) but the file was never created. These fixtures
cover every stuck class + stuck-in-SIT state so the regression spec can assert all of them.
"""

from __future__ import annotations

from deployment_api.settings import GITHUB_ORG

from ._repo_ci_alerts import AlertEntryDict, AlertsPayloadDict, derive_streams
from ._repo_ci_types import (  # pyright: ignore[reportPrivateUsage]
    BranchCommitsDict,
    BranchDeltaDict,
    BranchHeadDict,
    CommitEntryDict,
    FleetGitHealthProxyDict,
    ImageSignalDict,
    OverviewResponseDict,
    PromotionBlockedDict,
    RepoDetailResponseDict,
    RepoErrorDict,
    RepoOverviewDict,
    RepoPrDict,
    SitJobDict,
    SitLastRunDict,
    SitStateDict,
    _now_iso,
)


def _mock_sit(in_pending: bool, stuck: bool) -> SitStateDict:
    return SitStateDict(
        in_breaking_pending=in_pending,
        staging_locked=in_pending,
        staging_locked_reason="breaking cascade in flight" if in_pending else None,
        last_sit_run_status="failure" if stuck else "success",
        last_sit_run_age_min=240 if stuck else 12,
        stuck_in_sit=stuck,
    )


def _mock_image(version: str | None, stale: bool | None) -> ImageSignalDict:
    # When stale (mock FAILING repo) the latest build is red but a prior SUCCESS is still
    # surfaced — exercises the "last successful build" path in the UI.
    failing = stale is True
    return ImageSignalDict(
        last_build_status="FAILURE" if failing else "SUCCESS",
        last_build_sha="fae1ed0" if failing else "aaa1111",
        last_build_time="2026-06-11T09:15:00Z" if failing else "2026-06-11T07:30:00Z",
        last_build_log_url="https://console.cloud.google.com/cloud-build/builds/mock-latest",
        last_success_sha="aaa1111",
        last_success_time="2026-06-11T07:30:00Z",
        last_success_log_url="https://console.cloud.google.com/cloud-build/builds/mock-success",
        deployed_version=version,
        image_stale=stale,
    )


def _mock_pr(repo: str, number: int, base: str, stuck_class: str | None, state: str) -> RepoPrDict:
    return RepoPrDict(
        repo=repo,
        number=number,
        title=f"promote live-defi-rollout -> {base}",
        base=base,
        head="live-defi-rollout",
        url=f"https://github.com/{GITHUB_ORG}/{repo}/pull/{number}",
        age_min=95,
        auto_merge=True,
        merge_state=state,
        failed_check=stuck_class == "failing_check",
        v2_present=stuck_class in (None, "failing_check", "automerge_stuck"),
        stuck_class=stuck_class,  # pyright: ignore[reportArgumentType]  # fixture literal is within StuckClass
    )


def _mock_row(
    repo: str,
    repo_type: str,
    ci_status: str,
    prs: list[RepoPrDict],
    sit: SitStateDict,
) -> RepoOverviewDict:
    branches = [
        BranchHeadDict(branch="live-defi-rollout", sha="abc1234", committed_at="2026-06-10T08:00:00Z"),
        BranchHeadDict(branch="staging", sha="abc1200", committed_at="2026-06-10T07:30:00Z"),
        BranchHeadDict(branch="main", sha="abc1100", committed_at="2026-06-10T06:00:00Z"),
    ]
    deltas = [
        BranchDeltaDict(base="staging", head="live-defi-rollout", ahead_by=2, behind_by=0, files_changed=3),
        BranchDeltaDict(base="main", head="staging", ahead_by=1, behind_by=0, files_changed=1),
        BranchDeltaDict(base="main", head="live-defi-rollout", ahead_by=3, behind_by=0, files_changed=4),
    ]
    # Mock the failing branch: FAILING → main red (common "main red, LDR recovered" shape);
    # STAGING_GREEN → LDR red (actively-broken shape); else all green. Lets playwright assert
    # the branch annotation deterministically.
    if ci_status == "FAILING":
        branch_ci: dict[str, str | None] = {"live-defi-rollout": "success", "staging": "success", "main": "failure"}
    elif ci_status == "STAGING_GREEN":
        branch_ci = {"live-defi-rollout": "failure", "staging": "success", "main": "success"}
    else:
        branch_ci = {"live-defi-rollout": "success", "staging": "success", "main": "success"}
    return RepoOverviewDict(
        repo=repo,
        repo_type=repo_type,
        ci_status=ci_status,
        branch_ci=branch_ci,
        branches=branches,
        deltas=deltas,
        open_prs=prs,
        sit=sit,
        image=_mock_image("1.2.0", stale=ci_status == "FAILING"),
    )


def _mock_overview() -> OverviewResponseDict:
    rows = [
        _mock_row("unified-trading-library", "library", "MAIN_GREEN", [], _mock_sit(False, False)),
        _mock_row(
            "market-tick-data-service",
            "service",
            "STAGING_GREEN",
            [_mock_pr("market-tick-data-service", 41, "main", "conflicting", "dirty")],
            _mock_sit(False, False),
        ),
        _mock_row(
            "instruments-service",
            "service",
            "STAGING_PENDING",
            [_mock_pr("instruments-service", 17, "staging", "v2_never_reported", "blocked")],
            _mock_sit(False, False),
        ),
        _mock_row(
            "execution-service",
            "service",
            "FAILING",
            [
                _mock_pr("execution-service", 88, "main", "skip_ci_jammed", "blocked"),
                _mock_pr("execution-service", 89, "staging", "failing_check", "blocked"),
            ],
            _mock_sit(False, False),
        ),
        _mock_row(
            "strategy-service",
            "service",
            "STAGING_GREEN",
            [_mock_pr("strategy-service", 52, "main", "automerge_stuck", "blocked")],
            _mock_sit(False, False),
        ),
        _mock_row("greeks-service", "service", "STAGING_GREEN", [], _mock_sit(True, True)),
    ]
    stuck_prs = [pr for row in rows for pr in row["open_prs"] if pr.get("stuck_class")]
    stuck_in_sit = [row["repo"] for row in rows if row["sit"]["stuck_in_sit"]]
    sit_last_run = SitLastRunDict(
        url=f"https://github.com/{GITHUB_ORG}/unified-trading-pm/actions/runs/12345",
        status="in_progress",
        conclusion=None,
        age_min=18,
        jobs=[
            SitJobDict(name="sit / unified-trading-library", status="completed", conclusion="success"),
            SitJobDict(name="sit / greeks-service", status="completed", conclusion="failure"),
            SitJobDict(name="sit / execution-service", status="in_progress", conclusion=None),
        ],
    )
    return OverviewResponseDict(
        generated_at=_now_iso(),
        source="mock",
        repos=rows,
        stuck_prs=stuck_prs,
        stuck_in_sit=stuck_in_sit,
        sit_last_run=sit_last_run,
        # One sample degraded repo so the UI errors[] panel + its regression spec have data.
        errors=[RepoErrorDict(repo="alerting-service", error="HTTP 502 from GitHub compare")],
        # Two sample promotion-blocked repos (G1) so the panel + its regression spec have data:
        # one quarantined (escalated), one with a raw consecutive-fail count.
        promotion_blocked=[
            PromotionBlockedDict(
                repo="batch-live-reconciliation-service",
                failures=3,
                quarantined=True,
                since="2026-06-11T08:00:00Z",
                attempts=3,
                escalated=True,
            ),
            PromotionBlockedDict(repo="execution-service", failures=1, quarantined=False),
        ],
    )


def _mock_detail(repo: str) -> RepoDetailResponseDict:
    overview = _mock_overview()
    row = next((r for r in overview["repos"] if r["repo"] == repo), overview["repos"][0])
    history = [
        BranchCommitsDict(
            branch=branch["branch"],
            commits=[
                CommitEntryDict(
                    sha=f"{branch['sha'] or 'abc0000'}{i}",
                    message=f"feat: change {i} on {branch['branch']}",
                    author="ikennaigboaka [slot-3·laptop]",
                    committed_at="2026-06-10T07:00:00Z",
                    v2_conclusion="success" if i % 3 else "failure",
                )
                for i in range(5)
            ],
        )
        for branch in row["branches"]
    ]
    return RepoDetailResponseDict(
        repo=row["repo"],
        repo_type=row["repo_type"],
        ci_status=row["ci_status"],
        generated_at=_now_iso(),
        source="mock",
        branches=row["branches"],
        deltas=row["deltas"],
        history=history,
        open_prs=row["open_prs"],
        sit=row["sit"],
        image=row["image"],
    )


def _mock_alerts() -> AlertsPayloadDict:
    """Mock alert ledger — a lifecycle pair (FAILED -> RESOLVED) + a live CRITICAL, so the
    UI/playwright can assert current-vs-previous traceability."""
    entries: list[AlertEntryDict] = [
        AlertEntryDict(
            kind="alert",
            timestamp="2026-06-10T12:10:00Z",
            repo="unified-trading-pm",
            workflow_name="ci-status-update",
            severity="CRITICAL",
            conclusion="failure",
            message="CI REGRESSION: deployment-api is now FAILING (was MAIN_GREEN)",
            run_url="https://github.com/IggyIkenna/unified-trading-pm/actions/runs/1",
        ),
        AlertEntryDict(
            kind="alert",
            timestamp="2026-06-10T12:50:00Z",
            repo="unified-trading-pm",
            workflow_name="ci-status-update",
            severity="INFO",
            conclusion="success",
            message="RESOLVED: deployment-api recovered (FAILING -> MAIN_GREEN)",
            run_url="https://github.com/IggyIkenna/unified-trading-pm/actions/runs/2",
        ),
        AlertEntryDict(
            kind="alert",
            timestamp="2026-06-10T13:00:00Z",
            repo="unified-trading-pm",
            workflow_name="sit-unlock",
            severity="INFO",
            conclusion="success",
            message="SIT PASSED — staging UNLOCKED, breaking_pending cleared. Promotion queue flowing.",
            run_url="https://github.com/IggyIkenna/unified-trading-pm/actions/runs/3",
        ),
        AlertEntryDict(
            kind="alert",
            timestamp="2026-06-10T13:05:00Z",
            repo="execution-service",
            workflow_name="quality-gates-v2",
            severity="CRITICAL",
            conclusion="failure",
            message="quality-gates-v2 FAILED on main",
            run_url="https://github.com/IggyIkenna/execution-service/actions/runs/4",
        ),
    ]
    ordered = sorted(entries, key=lambda e: e["timestamp"], reverse=True)
    return AlertsPayloadDict(generated_at=_now_iso(), source="mock", alerts=ordered, streams=derive_streams(entries))


def _mock_fleet_git_health() -> FleetGitHealthProxyDict:
    """Mock fleet git-health — one laptop host with a clean + a drift-violating slot, so the
    UI/playwright can assert the proxied-data path AND the orchestrator deep-link both render."""
    return {
        "available": True,
        "reason": "",
        "orchestrator_url": "https://api.agent-orchestrator.odum-research.com",
        "data": {
            "generated_at": "2026-06-10T13:00:00Z",
            "scope": "fleet",
            "summary": {
                "hosts": 1,
                "slots": 2,
                "repos_total": 3,
                "dirty": 1,
                "behind": 1,
                "ahead": 1,
                "diverged": 0,
                "clean": 1,
                "drift_violations": 1,
                "reporter_stale_slots": 0,
                "ff_cron_stale_slots": 0,
            },
            "hosts": [
                {
                    "host": "laptop",
                    "vm_id": None,
                    "slots": [
                        {
                            "slot_id": 1,
                            "host": "laptop",
                            "reported_at": "2026-06-10T12:59:00Z",
                            "reporter_stale": False,
                            "ff_pull_last_run": "2026-06-10T12:58:00Z",
                            "ff_pull_last_result": "ok",
                            "ff_cron_stale": False,
                            "repos": [
                                {
                                    "name": "unified-trading-pm",
                                    "state": "clean",
                                    "dirty_files": 0,
                                    "ahead": 0,
                                    "behind": 0,
                                    "local_sha": "abc1234",
                                    "not_clean_since": None,
                                    "unpushed_plans": [],
                                    "drift_violation": False,
                                }
                            ],
                        },
                        {
                            "slot_id": 3,
                            "host": "laptop",
                            "reported_at": "2026-06-10T12:59:00Z",
                            "reporter_stale": False,
                            "ff_pull_last_run": "2026-06-10T12:58:00Z",
                            "ff_pull_last_result": "skip:dirty",
                            "ff_cron_stale": False,
                            "repos": [
                                {
                                    "name": "mtds",
                                    "state": "dirty",
                                    "dirty_files": 3,
                                    "ahead": 0,
                                    "behind": 1,
                                    "local_sha": "def5678",
                                    "not_clean_since": "2026-06-10T12:30:00Z",
                                    "unpushed_plans": [],
                                    "drift_violation": False,
                                },
                                {
                                    "name": "execution-service",
                                    "state": "ahead",
                                    "dirty_files": 0,
                                    "ahead": 2,
                                    "behind": 0,
                                    "local_sha": "fed9876",
                                    "not_clean_since": "2026-06-10T12:40:00Z",
                                    "unpushed_plans": [],
                                    "drift_violation": True,
                                },
                            ],
                        },
                    ],
                }
            ],
            "drift_violations": [
                {
                    "host": "laptop",
                    "slot": "3",
                    "repo": "execution-service",
                    "state": "ahead",
                    "ahead": "2",
                    "behind": "0",
                }
            ],
            "vm_errors": [],
        },
    }
