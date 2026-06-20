"""
Mock fixtures for the repo-CI dashboard — drive UI dev + playwright mock-mode suite.

Extracted from repo_ci.py (commit 94e4feb) but the file was never created. These fixtures
cover every stuck class + stuck-in-SIT state so the regression spec can assert all of them.
"""

from __future__ import annotations

from deployment_api.settings import GITHUB_ORG

from ._repo_ci_alerts import AlertEntryDict, AlertsPayloadDict, derive_streams
from ._repo_ci_types import (  # pyright: ignore[reportPrivateUsage]
    BlockingCheckDict,
    BranchCommitsDict,
    BranchDeltaDict,
    BranchHeadDict,
    CommitEntryDict,
    DepBlockerDict,
    FleetGitHealthProxyDict,
    ImageSignalDict,
    LastGreenDict,
    OverviewResponseDict,
    PromoteRunDict,
    PromotionBlockedDict,
    PromotionDrainDict,
    PromotionHeldDict,
    RepoDetailResponseDict,
    RepoErrorDict,
    RepoOverviewDict,
    RepoPrDict,
    RootBlockerDict,
    SemverHealthDict,
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
        content_identical=False,  # fixture models a real (content-bearing) stuck PR, not squash-noise
        stuck_class=stuck_class,  # pyright: ignore[reportArgumentType]  # fixture literal is within StuckClass
        # A failing-check PR carries the human reason (the AWS-CodeBuild PR-approval gate
        # that stranded two drain PRs invisibly on 2026-06-15).
        blocking_checks=(
            [
                BlockingCheckDict(
                    name=f"AWS CodeBuild ap-northeast-1 ({repo})",
                    state="failure",
                    description="Pull request approval required for starting a build",
                )
            ]
            if stuck_class == "failing_check"
            else []
        ),
    )


def _mock_row(
    repo: str,
    repo_type: str,
    ci_status: str,
    prs: list[RepoPrDict],
    sit: SitStateDict,
    *,
    tier: str = "3",
    blocked_by: list[DepBlockerDict] | None = None,
    blocking: list[str] | None = None,
) -> RepoOverviewDict:
    branches = [
        BranchHeadDict(
            branch="live-defi-rollout", sha="abc1234", committed_at="2026-06-10T08:00:00Z", tree_sha="tree_ldr"
        ),
        BranchHeadDict(branch="staging", sha="abc1200", committed_at="2026-06-10T07:30:00Z", tree_sha="tree_stg"),
        BranchHeadDict(branch="main", sha="abc1100", committed_at="2026-06-10T06:00:00Z", tree_sha="tree_main"),
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
    # N2: last-green main. When main is red (FAILING) the last green ≠ the head (an EARLIER green
    # sha); otherwise the head IS green so last-green = the main head.
    if ci_status == "FAILING":
        last_green_main = LastGreenDict(sha="ab09999", at="2026-06-09T20:00:00Z")
    else:
        last_green_main = LastGreenDict(sha="abc1100", at="2026-06-10T06:00:00Z")
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
        last_green_main=last_green_main,
        # G6: every mock row is LDR-ahead-of-main (delta ahead_by=3) so it has a lag; FAILING
        # repos sit longer (the drain is stuck). >60min so the lag-chip renders prominently.
        main_lag_age_min=185 if ci_status == "FAILING" else 95,
        # promotion-drain follow-up: drain-stalled = content ahead (every mock row is) AND a
        # BLOCKING stuck PR (conflicting / failing_check / skip_ci_jammed). Derived from prs so the
        # mock can't drift from the real contract. (execution-service: skip_ci_jammed + failing_check;
        # market-tick-data-service: conflicting → both stalled. v2_never_reported / automerge_stuck
        # are auto-recoverable → NOT stalled.)
        drain_stalled=any(pr.get("stuck_class") in {"conflicting", "failing_check", "skip_ci_jammed"} for pr in prs),
        # Dep-order HOLD (STAGE 1.8 mirror). tier from the manifest layer; blocked_by/blocking
        # default empty (clear) and are seeded explicitly in _mock_overview for the held scenario.
        tier=tier,
        blocked_by=blocked_by if blocked_by is not None else [],
        blocking=blocking if blocking is not None else [],
    )


def _mock_overview() -> OverviewResponseDict:
    # Dep-order HOLD scenario (STAGE 1.8 mirror): the tier-0 dep `unified-api-contracts` lags at
    # STAGING_GREEN (NOT on main), so every dependent service above it is HELD. Two dependents
    # (market-tick-data-service + strategy-service) point at it via blocked_by, and it carries them
    # in `blocking` — so root_blockers has 1 entry (blocking_count == 2) and ≥2 rows are held.
    _uac_blocker = DepBlockerDict(name="unified-api-contracts", tier="0", ci_status="STAGING_GREEN")
    rows = [
        _mock_row("unified-trading-library", "library", "MAIN_GREEN", [], _mock_sit(False, False), tier="0"),
        # The tier-0 root blocker itself: at STAGING_GREEN (not on main), holding the two dependents.
        _mock_row(
            "unified-api-contracts",
            "library",
            "STAGING_GREEN",
            [],
            _mock_sit(False, False),
            tier="0",
            blocking=["market-tick-data-service", "strategy-service"],
        ),
        _mock_row(
            "market-tick-data-service",
            "service",
            "STAGING_GREEN",
            [_mock_pr("market-tick-data-service", 41, "main", "conflicting", "dirty")],
            _mock_sit(False, False),
            blocked_by=[_uac_blocker],
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
            blocked_by=[_uac_blocker],
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
        # Routine promote drain (PM-central, every 15 min) — both legs green + recent so the panel
        # renders the healthy case; distinct from the breaking cascade/SIT panel above.
        promotion_drain=PromotionDrainDict(
            ldr_to_staging=PromoteRunDict(
                status="completed",
                conclusion="success",
                age_min=8,
                url=f"https://github.com/{GITHUB_ORG}/unified-trading-pm/actions/runs/55501",
            ),
            ldr_to_main=PromoteRunDict(
                status="completed",
                conclusion="success",
                age_min=5,
                url=f"https://github.com/{GITHUB_ORG}/unified-trading-pm/actions/runs/55502",
            ),
        ),
        # Semver-agent standing health (G2) — breaker ARMED (3 pending bumps ≥ threshold) so the
        # panel + its regression spec exercise the alert state, not just the healthy one.
        semver_health=SemverHealthDict(
            last_run_status="completed",
            last_run_conclusion="success",
            last_run_age_min=12,
            last_run_url=f"https://github.com/{GITHUB_ORG}/unified-trading-pm/actions/runs/55503",
            pending_bump_count=3,
            pending_bump_repos=["execution-service", "mtds", "alerting-service"],
            breaker_armed=True,
            breaker_threshold=3,
        ),
        # Dep-order HOLD aggregate (STAGE 1.8 mirror) — the tier-0 unified-api-contracts at
        # STAGING_GREEN holds 2 dependents from main. Consistent with the per-row blocked_by/blocking
        # above: held_repos == the 2 dependents; root_blockers has the single tier-0 cause
        # (blocking_count == 2, main_files_behind == 1 from its staging→main delta).
        promotion_held=PromotionHeldDict(
            held_repos=["market-tick-data-service", "strategy-service"],
            root_blockers=[
                RootBlockerDict(
                    repo="unified-api-contracts",
                    tier="0",
                    ci_status="STAGING_GREEN",
                    blocking_count=2,
                    main_files_behind=1,
                )
            ],
        ),
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
        # N2-followup: per-branch last-green. LDR + main green at their heads; staging's last
        # green is an EARLIER sha (its head is red/pending) — so the drilldown shows the three
        # axes can differ.
        last_green={
            "live-defi-rollout": LastGreenDict(
                sha=f"{row['branches'][0]['sha'] or 'abc0000'}", at="2026-06-10T07:00:00Z"
            ),
            "staging": LastGreenDict(sha="ab09111", at="2026-06-09T22:00:00Z"),
            # the row's last_green_main is already LastGreenDict | None — pass it through directly.
            "main": row.get("last_green_main"),
        },
    )


def _mock_alerts() -> AlertsPayloadDict:
    """Mock alert ledger — CI lifecycle pair + live CRITICAL + non-CI ops kinds,
    so the UI/playwright can assert current-vs-previous traceability AND
    that non-CI kinds (git_health, worker_liveness, vm_down) render correctly."""
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
        # Non-CI ops alert kinds (INFRA P1: unified ledger across domains).
        AlertEntryDict(
            kind="git_health",
            timestamp="2026-06-10T13:10:00Z",
            repo="agent-orchestrator",
            workflow_name="ops-git_health",
            severity="WARNING",
            conclusion=None,
            message="Slot 3 git-health RED: dirty 35m, 2 commits behind",
            run_url=None,
        ),
        AlertEntryDict(
            kind="worker_liveness",
            timestamp="2026-06-10T13:15:00Z",
            repo="agent-orchestrator",
            workflow_name="ops-worker_liveness",
            severity="WARNING",
            conclusion=None,
            message="Slot 2 watchdog-killed: heartbeat_silent_900s",
            run_url=None,
        ),
        AlertEntryDict(
            kind="vm_down",
            timestamp="2026-06-10T13:20:00Z",
            repo="vm-watchdog",
            workflow_name="ops-vm_down",
            severity="CRITICAL",
            conclusion=None,
            message="VM agent-orchestrator-vm-1 DOWN: health-check timed out after 60s",
            run_url=None,
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
