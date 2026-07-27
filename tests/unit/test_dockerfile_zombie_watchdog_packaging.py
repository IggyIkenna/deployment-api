"""
Regression guard for heartbeat_stall_watcher_autokill_never_works_in_production_2026_07_27.md:
`vm_zombie_watchdog.py` (a top-level `deployment-service/scripts/` file, never part of the
`deployment_service` wheel) was structurally absent from every build of this image — the
`--no-deps` package install can't carry it, and nothing ever COPY'd it in from the vendored
`_deployment-service/` sibling source. `_zombie_watchdog()`
(`deployment_service/data_pipeline_monitors/cli.py`) silently returned None on every sweep,
so the stalled-VM auto-kill actuator never fired in production. The same root cause also gated
the entire Layer-0 recovery-actuator family (`scripts/recovery/`, e.g. `RelaunchStalledVm`) via
`escalation.py`'s `_ACTUATORS_AVAILABLE` probe. This test parses the Dockerfile text directly (a
real `docker build` is too expensive for the unit suite) and asserts both fixes land in the `api`
stage specifically — the production stage, not just `api-dev` (whose own `COPY scripts/
./scripts/` copies deployment-api's OWN unrelated scripts/ dir, not either of these).
"""

from pathlib import Path

_DOCKERFILE_PATH = Path(__file__).parent.parent.parent / "Dockerfile"


def _read_stage(stage_from_marker: str, next_stage_marker: str | None) -> str:
    """Slice out one build stage's text between its `FROM ... AS <x>` line and the next."""
    text = _DOCKERFILE_PATH.read_text()
    start = text.index(stage_from_marker)
    end = text.index(next_stage_marker, start) if next_stage_marker else len(text)
    return text[start:end]


class TestVmZombieWatchdogPackaging:
    def test_dockerfile_exists(self) -> None:
        assert _DOCKERFILE_PATH.is_file()

    def test_api_stage_copies_vm_zombie_watchdog_from_vendored_deployment_service(self) -> None:
        api_stage = _read_stage("FROM base AS api", "FROM api AS api-dev")
        assert "_deployment-service/scripts/vm/vm_zombie_watchdog.py" in api_stage, (
            "The production `api` stage must COPY vm_zombie_watchdog.py from the vendored "
            "_deployment-service/ source — a --no-deps package install never carries a "
            "top-level scripts/ file, so this needs an explicit COPY, not just the pip install."
        )

    def test_api_stage_copy_targets_the_namespace_package_path(self) -> None:
        api_stage = _read_stage("FROM base AS api", "FROM api AS api-dev")
        assert "./scripts/vm/vm_zombie_watchdog.py" in api_stage, (
            "Must land at ./scripts/vm/vm_zombie_watchdog.py so `scripts.vm.vm_zombie_watchdog` "
            "resolves as a PEP 420 namespace package under /app (already on sys.path via "
            "gunicorn's app-loader, the same mechanism that resolves deployment_api.main:app)."
        )

    def test_fix_precedes_deployment_service_tmp_dir_cleanup(self) -> None:
        """The fix COPYs directly from the build-context sibling source, not from the
        install step's /tmp/deployment-service — so it must not depend on `rm -rf` ordering.
        Guard against a future edit accidentally routing it through the doomed /tmp copy."""
        api_stage = _read_stage("FROM base AS api", "FROM api AS api-dev")
        copy_line = next(
            line for line in api_stage.splitlines() if "_deployment-service/scripts/vm/vm_zombie_watchdog.py" in line
        )
        assert copy_line.strip().startswith("COPY _deployment-service/"), (
            "Must COPY from the build-context source (_deployment-service/scripts/...), "
            "not from /tmp/deployment-service (which is rm -rf'd after the pip install)."
        )

    def test_api_stage_copies_recovery_actuator_package(self) -> None:
        api_stage = _read_stage("FROM base AS api", "FROM api AS api-dev")
        assert "_deployment-service/scripts/recovery/" in api_stage, (
            "The production `api` stage must also COPY the whole scripts/recovery/ package — "
            "escalation.py's _ACTUATORS_AVAILABLE probe needs scripts.recovery.relaunch_consolidator "
            "importable, or RelaunchStalledVm (and every other Layer-0 actuator) stays dead even "
            "after vm_zombie_watchdog itself is fixed."
        )
