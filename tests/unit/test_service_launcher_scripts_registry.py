"""Pre-flight test: every script registered in `_SERVICE_LAUNCHER_SCRIPTS`
exists on disk under `deployment-service/scripts/vm/`.

Plan: ``launcher_scripts_consolidation_into_deployment_service_2026_05_07.plan.md``
§ Phase 2 (deployment-api unit-test coverage).

Why this test exists
--------------------
Reference incident (Tab 11 audit, 2026-05-08): three entries registered
in `_SERVICE_LAUNCHER_SCRIPTS` did NOT exist on disk under
`deployment-service/scripts/vm/`. Deploy-Missing UI button was silently
broken for those services — the operator clicks Deploy-Missing, the API
resolves the path (deploy_missing.py builds the command happily), the
copy-to-clipboard widget produces a `bash deployment-service/scripts/vm/launch-X.sh ...`
invocation, the operator runs it, and the shell errors with "No such
file or directory."

Tab 11 + Tab 5 follow-up filled all three (`launch-mtds-backfill-vm.sh`,
`launch-instruments-backfill-vm.sh`, `launch-features-onchain-backfill-vm.sh`).
This test ensures any future registry edit catches the typo at CI time
instead of at panic-time deploy-missing click.

The test resolves the workspace root from the test file's location
(``.../deployment-api/tests/unit/<this>.py`` → workspace root is three
levels up from the deployment-api root). It does NOT depend on
``WORKSPACE_ROOT`` env var or any external state, so it runs identically
locally + in Cloud Build CI.
"""

from __future__ import annotations

from pathlib import Path

from deployment_api.services import deploy_missing

# Resolve workspace root from this test file's path:
#   <workspace>/deployment-api/tests/unit/test_service_launcher_scripts_registry.py
#                                  ^----- 4 parents up = workspace root
_WORKSPACE_ROOT: Path = Path(__file__).resolve().parents[3]
_VM_SCRIPT_DIR: Path = _WORKSPACE_ROOT / "deployment-service" / "scripts" / "vm"


class TestServiceLauncherScriptsRegistryOnDisk:
    """Every registered launcher must exist on disk + be a regular file.

    Catches the (a) typo case (registered name has a misplaced char) +
    (b) missing-on-disk case (registered name was never authored or got
    deleted in a refactor).
    """

    def test_vm_script_dir_exists(self) -> None:
        """Sanity check the path resolution itself before per-entry checks."""
        assert _VM_SCRIPT_DIR.is_dir(), (
            f"Resolved VM script dir does not exist: {_VM_SCRIPT_DIR}. "
            f"Workspace root resolution from test file path is broken; "
            f"check parents[3] depth in this test file."
        )

    def test_every_registered_launcher_exists_on_disk(self) -> None:
        """Walk `_SERVICE_LAUNCHER_SCRIPTS` + assert each path resolves
        to a real file under `deployment-service/scripts/vm/`.

        On failure: name the missing entries explicitly so a human reading
        the CI log knows exactly which dict edit needs a launcher author.
        """
        registry: dict[str, str] = deploy_missing._SERVICE_LAUNCHER_SCRIPTS
        # Defensive: registry must be non-empty (catches accidental
        # mass-deletion that would silently make this test pass).
        assert registry, "_SERVICE_LAUNCHER_SCRIPTS is empty — registry was wiped?"

        missing: list[tuple[str, str]] = []
        for service_slug, registered_path in registry.items():
            # Registered path is workspace-relative
            # ("deployment-service/scripts/vm/launch-X.sh"); resolve
            # against workspace root.
            on_disk = _WORKSPACE_ROOT / registered_path
            if not on_disk.is_file():
                missing.append((service_slug, registered_path))

        assert not missing, (
            "Registered launcher scripts do not exist on disk:\n"
            + "\n".join(f"  - {slug:40s} -> {path}" for slug, path in missing)
            + f"\nFix: author the missing launcher under {_VM_SCRIPT_DIR.relative_to(_WORKSPACE_ROOT)}/, "
            "OR remove the registry entry from "
            "`deployment_api/services/deploy_missing.py:_SERVICE_LAUNCHER_SCRIPTS`."
        )

    def test_registered_paths_are_under_canonical_vm_dir(self) -> None:
        """Workspace SSOT (CLAUDE.md "VM launcher script SSOT"): every
        launcher script registered for Deploy-Missing MUST live under
        `deployment-service/scripts/vm/`. No exceptions — scattered
        launchers under `e2e-testing/scripts/` etc. break the registry
        contract.
        """
        registry: dict[str, str] = deploy_missing._SERVICE_LAUNCHER_SCRIPTS
        bad_path: list[tuple[str, str]] = []
        canonical_prefix = "deployment-service/scripts/vm/"
        for service_slug, registered_path in registry.items():
            if not registered_path.startswith(canonical_prefix):
                bad_path.append((service_slug, registered_path))

        assert not bad_path, (
            "Launcher scripts must live under "
            f"`{canonical_prefix}` per workspace SSOT; offending entries:\n"
            + "\n".join(f"  - {slug:40s} -> {path}" for slug, path in bad_path)
        )
