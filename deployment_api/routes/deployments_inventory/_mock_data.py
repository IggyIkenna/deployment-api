"""Mock-mode deployment inventory fixture (no GCP / GCS access).

Split from ``routes/deployments_inventory.py`` (pure code motion; plan:
``deployment_api_qg_size_gate_debt_2026_07_30.md``). No patched-seam collaborators — a pure
static fixture.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from unified_api_contracts import DeploymentUmbrella

from deployment_api.routes.deployments_inventory import DeploymentItem

__all__ = ["_mock_inventory"]


def _mock_inventory(now: datetime) -> list[DeploymentItem]:
    """A representative mock inventory (mock mode — no GCP / GCS access).

    ``last_run_at`` is derived from the caller's ``now`` (not a hardcoded date) so this
    fixture never goes stale again — a frozen absolute date silently trains the UI on an
    old shape (see
    unified-trading-pm/plans/active/issues/deployment_api_live_mock_parity_2026_07_17.md).
    Spacing between items is preserved from the original fixture, just re-anchored to now
    (the newest original timestamp, the AWS backfill VM, maps to ``now`` itself).
    """

    def _run_at(delta: timedelta) -> str:
        return (now - delta).strftime("%Y-%m-%dT%H:%M:%SZ")

    return [
        DeploymentItem(
            name="cefi-binance-spot-20260622-014158",
            kind="VM",
            umbrella="BATCH",
            cloud="GCP",
            service="cefi-binance-spot-20260622-014158",
            asset_group="cefi",
            status="running",
            last_run_at=_run_at(timedelta(hours=9, minutes=48, seconds=2)),
            exit_code=None,
            heartbeat_age_seconds=42,
            captured_progress=11_987,
            run_log_uri="",
        ),
        DeploymentItem(
            name="defi-backfill-20260622-OOM",
            kind="VM",
            umbrella="BATCH",
            cloud="GCP",
            service="defi-backfill",
            asset_group="defi",
            status="failed",
            last_run_at=_run_at(timedelta(hours=8, minutes=30)),
            exit_code=137,
            heartbeat_age_seconds=3_600,
            captured_progress=0,
            run_log_uri="",
        ),
        DeploymentItem(
            name="strategy-live-cefi-20260620",
            kind="VM",
            umbrella="LIVE",
            cloud="GCP",
            service="strategy-live",
            asset_group="cefi",
            status="running",
            last_run_at=_run_at(timedelta(days=2, hours=11, minutes=30)),
            exit_code=None,
            heartbeat_age_seconds=30,
            captured_progress=0,
            run_log_uri="",
        ),
        DeploymentItem(
            name="defi-paper-trading-20260622",
            kind="VM",
            umbrella="PAPER",
            cloud="GCP",
            service="defi-paper-trading",
            asset_group="defi",
            status="running",
            last_run_at=_run_at(timedelta(hours=11, minutes=30)),
            exit_code=None,
            heartbeat_age_seconds=15,
            captured_progress=0,
            run_log_uri="",
        ),
        DeploymentItem(
            name="manifest-consolidator",
            kind="CLOUD_RUN_JOB",
            umbrella="BATCH",
            cloud="GCP",
            service="manifest-consolidator",
            asset_group="cefi",
            status="succeeded",
            last_run_at=_run_at(timedelta(hours=5, minutes=30)),
            exit_code=0,
            heartbeat_age_seconds=None,
            captured_progress=None,
            run_log_uri="",
        ),
        # AWS estate (Phase 5 parity) — an EC2 backfill VM + a Batch Fargate job,
        # cloud=AWS, classified under the SAME umbrellas as the GCP items.
        DeploymentItem(
            name="mtds-backfill-cefi-20260622-aws",
            kind="VM",
            umbrella="BATCH",
            cloud="AWS",
            service="mtds-backfill",
            asset_group="cefi",
            status="running",
            last_run_at=_run_at(timedelta(0)),
            exit_code=None,
            heartbeat_age_seconds=None,
            captured_progress=None,
            run_log_uri="",
        ),
        DeploymentItem(
            name="manifest-consolidator-aws",
            kind="CLOUD_RUN_JOB",
            umbrella="BATCH",
            cloud="AWS",
            service="manifest-consolidator",
            asset_group="cefi",
            status="succeeded",
            last_run_at=_run_at(timedelta(hours=5, minutes=25)),
            exit_code=0,
            heartbeat_age_seconds=None,
            captured_progress=None,
            run_log_uri="",
        ),
        # Always-on Cloud Run service (WS-B kinds census) — no live/batch/paper
        # phase, so umbrella is DeploymentUmbrella.NONE (Open-Q1); Kind carries
        # the fact that this is a service.
        DeploymentItem(
            name="deployment-api",
            kind="CLOUD_RUN_SERVICE",
            umbrella=DeploymentUmbrella.NONE.value,
            cloud="GCP",
            service="deployment-api",
            asset_group="",
            status="running",
            last_run_at=None,
            exit_code=None,
            heartbeat_age_seconds=None,
            captured_progress=None,
            run_log_uri="",
            revision="deployment-api-00042-xyz",
            region="asia-northeast1",
        ),
    ]
