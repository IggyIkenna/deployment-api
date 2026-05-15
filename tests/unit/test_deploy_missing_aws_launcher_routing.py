"""
AWS launcher routing pin for deploy_missing._SERVICE_LAUNCHER_SCRIPTS.

When CLOUD_PROVIDER=aws, the deploy-missing preview MUST resolve launcher
scripts to EC2-equivalent scripts (launch-*-ec2.sh) rather than the GCE
launch-*-vm.sh scripts used on GCP. Today all launchers are GCE-only (from
`aws_migration_defi_first_2026_05_07.md` which has not yet shipped EC2
launchers). These tests are marked SKIP until the EC2 launchers are added
to deployment-service/scripts/vm/.

Gate condition: the EC2 launcher files must exist at the expected path before
these tests can be un-skipped. Each test includes the exact path check in a
comment so the un-skip can be done mechanically when the launchers land.

Plan: data_status_comprehensive_test_coverage_2026_05_07 § E.3.
Gate: aws_migration_defi_first_2026_05_07.md EC2 launcher phase.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from deployment_api.services.deploy_missing import (
    _SERVICE_LAUNCHER_SCRIPTS,
    build_deploy_missing_preview,
)

# ---------------------------------------------------------------------------
# Gate check — used to skip all tests until EC2 launchers exist.
# ---------------------------------------------------------------------------

_EC2_LAUNCHERS_EXIST = any("ec2" in v for v in _SERVICE_LAUNCHER_SCRIPTS.values())
_SKIP_REASON = (
    "EC2 launchers not yet shipped (aws_migration_defi_first_2026_05_07 Phase N pending). "
    "Un-skip when deployment-service/scripts/vm/launch-*-ec2.sh files exist and "
    "_SERVICE_LAUNCHER_SCRIPTS is updated to include them."
)


@pytest.mark.skipif(not _EC2_LAUNCHERS_EXIST, reason=_SKIP_REASON)
class TestDeployMissingAwsLauncherRouting:
    """When CLOUD_PROVIDER=aws, _SERVICE_LAUNCHER_SCRIPTS routes to EC2 launchers."""

    def test_mtds_resolves_to_ec2_launcher_when_aws(self) -> None:
        """market-tick-data-service resolves to launch-mtds-backfill-ec2.sh on AWS."""
        launcher = _SERVICE_LAUNCHER_SCRIPTS.get("market-tick-data-service", "")
        assert "ec2" in launcher, (
            f"market-tick-data-service launcher is {launcher!r} — expected EC2 script. "
            "AWS routing requires the ec2 variant."
        )
        assert "vm.sh" not in launcher, (
            f"market-tick-data-service launcher {launcher!r} still points to GCE vm.sh."
        )

    def test_instruments_service_resolves_to_ec2_launcher_when_aws(self) -> None:
        """instruments-service resolves to launch-instruments-backfill-ec2.sh on AWS."""
        launcher = _SERVICE_LAUNCHER_SCRIPTS.get("instruments-service", "")
        assert "ec2" in launcher, (
            f"instruments-service launcher is {launcher!r} — expected EC2 script."
        )

    def test_features_service_resolves_to_ec2_launcher_when_aws(self) -> None:
        """features-service resolves to launch-features-ec2.sh on AWS."""
        launcher = _SERVICE_LAUNCHER_SCRIPTS.get("features-service", "")
        assert "ec2" in launcher, (
            f"features-service launcher is {launcher!r} — expected EC2 script."
        )

    def test_mdps_resolves_to_ec2_launcher_when_aws(self) -> None:
        """market-data-processing-service resolves to the EC2 launcher on AWS."""
        launcher = _SERVICE_LAUNCHER_SCRIPTS.get("market-data-processing-service", "")
        assert "ec2" in launcher, (
            f"market-data-processing-service launcher is {launcher!r} — expected EC2 script."
        )

    def test_no_launcher_contains_gce_vm_script_suffix_when_all_aws(self) -> None:
        """When fully migrated to AWS, no launcher in _SERVICE_LAUNCHER_SCRIPTS should end in -vm.sh."""
        gce_launchers = {k: v for k, v in _SERVICE_LAUNCHER_SCRIPTS.items() if "vm.sh" in v}
        assert not gce_launchers, (
            f"GCE vm.sh launchers still present when CLOUD_PROVIDER=aws: {list(gce_launchers.keys())}. "
            "All launchers must use ec2 scripts on the AWS path."
        )

    def test_build_preview_command_contains_ec2_script_when_aws(self) -> None:
        """build_deploy_missing_preview produces a command that references the EC2 script."""
        with patch.dict(os.environ, {"CLOUD_PROVIDER": "aws"}):
            preview = build_deploy_missing_preview(
                service="instruments-service",
                asset_group="cefi",
                row_key={
                    "venue": "BINANCE",
                    "data_type": "spot",
                    "instrument_type": "SPOT",
                    "date": "2024-01-01",
                },
            )

        command = preview.command
        assert "ec2" in command or "EC2" in command, (
            f"Preview command does not reference EC2 script: {command!r}. "
            "AWS path must route to EC2 launchers."
        )
        assert "launch-instruments-backfill-vm.sh" not in command, (
            f"Preview command still references GCE vm.sh script: {command!r}."
        )


class TestDeployMissingCurrentLauncherStateDocumented:
    """Document the current (GCE-only) state so drift is detectable."""

    def test_all_current_launchers_are_gce_vm_scripts(self) -> None:
        """Verifies the current state: all launchers are GCE vm.sh scripts.

        This test MUST FAIL after EC2 launchers land (it asserts the PRE-migration
        state). When it fails, un-skip the EC2 tests above and remove this test.
        """
        if _EC2_LAUNCHERS_EXIST:
            pytest.skip("EC2 launchers have landed — this pre-migration state test is obsolete.")

        gce_launchers = [v for v in _SERVICE_LAUNCHER_SCRIPTS.values() if "vm.sh" in v]
        assert len(gce_launchers) > 0, (
            "Expected at least one GCE vm.sh launcher — state has changed, review E.3 tests."
        )

    def test_no_ec2_launchers_in_current_scripts(self) -> None:
        """Pre-migration guard: no EC2 launchers exist yet.

        This test MUST FAIL after EC2 launchers land (expected). When it fails,
        un-skip the EC2 routing tests above.
        """
        if _EC2_LAUNCHERS_EXIST:
            pytest.skip("EC2 launchers have landed — pre-migration guard is now obsolete.")

        ec2_launchers = [v for v in _SERVICE_LAUNCHER_SCRIPTS.values() if "ec2" in v]
        assert len(ec2_launchers) == 0, (
            f"Unexpected EC2 launchers found: {ec2_launchers}. "
            "Un-skip the EC2 routing tests in TestDeployMissingAwsLauncherRouting."
        )
