"""
Regression guard for two chained packaging incidents, both rooted in the same structural gap:
`deployment-service/scripts/{vm,recovery}/` are top-level dirs, never part of the
`deployment_service` wheel (a `--no-deps` package install can't carry them), so anything under
them needs an explicit `COPY` from the vendored `_deployment-service/` sibling source or it is
silently absent at runtime.

1. heartbeat_stall_watcher_autokill_never_works_in_production_2026_07_27.md:
   `vm_zombie_watchdog.py` was missing entirely — `_zombie_watchdog()`
   (`deployment_service/data_pipeline_monitors/cli.py`) silently returned None on every sweep,
   so the stalled-VM auto-kill actuator never fired. The same root cause also gated the entire
   Layer-0 recovery-actuator family (`scripts/recovery/`, e.g. `RelaunchStalledVm`) via
   `escalation.py`'s `_ACTUATORS_AVAILABLE` probe.

2. data_pipeline_self_healing_completion_residual_2026_07_24.md P1 (closed): the first fix's
   `COPY` only carried the ONE file (`vm_zombie_watchdog.py`) — it did not ship the
   `scripts/vm/launch-*.sh` launchers themselves. `RelaunchBackfillVm`/`RelaunchStalledVm`
   (`scripts/recovery/relaunch_{backfill,stalled}_vm.py`) subprocess-exec those launchers by
   FILE path at actuation time (not a Python import), so `_ACTUATORS_AVAILABLE` going True
   never caught their absence — every real `auto_recover` relaunch attempt hit
   `FileNotFoundError` inside the launcher subprocess call (caught internally as
   `status=FAILED`, so it never crashed the sweep — it just never actually relaunched
   anything). The fix COPYs the WHOLE `scripts/vm/` directory, not one file, so a
   newly-added launcher is picked up automatically with no future Dockerfile edit.

This test parses the Dockerfile text directly (a real `docker build` is too expensive for the
unit suite) and asserts every fix lands in the `api` stage specifically — the production stage,
not just `api-dev` (whose own `COPY scripts/ ./scripts/` copies deployment-api's OWN unrelated
scripts/ dir, not either of these).
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

    def test_api_stage_copies_the_whole_vm_dir_not_a_single_file(self) -> None:
        """Regression guard for the P1 residual gap: the COPY source must be the `scripts/vm/`
        DIRECTORY (trailing slash — copies its full contents), not a single filename. A
        single-file copy (e.g. `.../scripts/vm/vm_zombie_watchdog.py`) silently drops every
        `launch-*.sh` launcher the relaunch actuators subprocess-exec at actuation time."""
        api_stage = _read_stage("FROM base AS api", "FROM api AS api-dev")
        assert "COPY _deployment-service/scripts/vm/ ./scripts/vm/" in api_stage, (
            "The production `api` stage must COPY the whole _deployment-service/scripts/vm/ "
            "directory (not a single file) so every launch-*.sh launcher (+ its lib/-sourced "
            "helpers and templates/) is present for RelaunchBackfillVm/RelaunchStalledVm to "
            "subprocess-exec at actuation time."
        )
        assert "_deployment-service/scripts/vm/vm_zombie_watchdog.py" not in api_stage, (
            "Must not regress to the old single-file COPY — that drops every launch-*.sh "
            "launcher again (data_pipeline_self_healing_completion_residual_2026_07_24.md P1)."
        )

    def test_api_stage_copy_targets_the_namespace_package_path(self) -> None:
        api_stage = _read_stage("FROM base AS api", "FROM api AS api-dev")
        assert "./scripts/vm/" in api_stage, (
            "Must land at ./scripts/vm/ so `scripts.vm.*` resolves as a PEP 420 namespace "
            "package under /app (already on sys.path via gunicorn's app-loader, the same "
            "mechanism that resolves deployment_api.main:app)."
        )

    def test_fix_precedes_deployment_service_tmp_dir_cleanup(self) -> None:
        """The fix COPYs directly from the build-context sibling source, not from the
        install step's /tmp/deployment-service — so it must not depend on `rm -rf` ordering.
        Guard against a future edit accidentally routing it through the doomed /tmp copy."""
        api_stage = _read_stage("FROM base AS api", "FROM api AS api-dev")
        copy_line = next(
            line for line in api_stage.splitlines() if "_deployment-service/scripts/vm/" in line and "COPY" in line
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

    def test_vm_dir_copy_precedes_recovery_dir_copy(self) -> None:
        """Both are independent (order doesn't matter for correctness), but keeping the vm/
        directory copy adjacent to (and documented alongside) the recovery/ copy is what makes
        the shared root-cause comment block coherent — guard against them drifting apart."""
        api_stage = _read_stage("FROM base AS api", "FROM api AS api-dev")
        vm_idx = api_stage.index("COPY _deployment-service/scripts/vm/")
        recovery_idx = api_stage.index("COPY _deployment-service/scripts/recovery/")
        assert vm_idx < recovery_idx, "Expected the scripts/vm/ COPY to precede the scripts/recovery/ COPY."
