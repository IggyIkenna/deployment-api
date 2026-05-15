"""Honest-coverage VM launcher — Cloud Run Job entrypoint.

Runs daily via Cloud Scheduler cron (terraform module:
``deployment-service/terraform/gcp/honest_coverage_scheduler.tf``).

Launches a GCE VM that runs
``instruments-service/scripts/measure_honest_coverage.py`` and
writes the result to::

    gs://{project_id}-honest-coverage/{date}/coverage.json

The ``deployment-api`` endpoint ``GET /api/data-status/honest-coverage``
reads that file; it returns 404 until this job has run for the day.

Singleton lock:
    Refuses to launch if a ``measure-honest-coverage-*`` VM is already
    running in the target zone (prevents JSON-race from concurrent runs).
    On skip, exits 0 so Cloud Scheduler marks the run as success.

Plan:
    ``unified-trading-pm/plans/active/issues/honest_coverage_cron_vm_scheduling_2026_05_14.md``
"""

from __future__ import annotations

import argparse
import datetime
import logging
import sys
from typing import NamedTuple, cast

from google.auth.transport.requests import AuthorizedSession

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

_VM_PREFIX = "measure-honest-coverage-"
_STARTUP_SCRIPT_TEMPLATE = "gs://deployment-scripts-{project}/vm/setup-data-pipeline-vm.sh"
_MEASURE_SCRIPT = (
    "python /home/ikennaigboaka/workspace/instruments/scripts/measure_honest_coverage.py"
)
_DISK_IMAGE = "projects/ubuntu-os-cloud/global/images/family/ubuntu-2404-lts-amd64"
_COMPUTE_BASE = "https://compute.googleapis.com/compute/v1"
_ZONE = "asia-northeast1-c"


class _Args(NamedTuple):
    project: str
    asset_group: str
    env: str


def _authed_session() -> AuthorizedSession:
    import google.auth

    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return AuthorizedSession(creds)


def _list_running_vms(session: AuthorizedSession, project: str) -> list[str]:
    """Return names of running VMs matching the honest-coverage prefix."""
    url = f"{_COMPUTE_BASE}/projects/{project}/zones/{_ZONE}/instances"
    params: dict[str, str] = {
        "filter": f'name = "{_VM_PREFIX}*" AND status = "RUNNING"',
        "fields": "items/name",
    }
    resp = session.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = cast(dict[str, list[dict[str, str]]], resp.json())
    return [item["name"] for item in data.get("items", [])]


def _create_vm(
    session: AuthorizedSession,
    project: str,
    vm_name: str,
    asset_group: str,
    deployment_env: str,
) -> str:
    """Call GCE instances.insert; return operation name."""
    url = f"{_COMPUTE_BASE}/projects/{project}/zones/{_ZONE}/instances"
    startup_url = _STARTUP_SCRIPT_TEMPLATE.format(project=project)
    measure_cmd = f"{_MEASURE_SCRIPT} --asset-group {asset_group}"
    run_ts = datetime.datetime.now(tz=datetime.UTC).strftime("%Y%m%d")

    metadata_items: list[dict[str, str]] = [
        {"key": "startup-script-url", "value": startup_url},
        {"key": "VM_TASK", "value": "measure-honest-coverage"},
        {"key": "VM_SERVICE", "value": "instruments_service"},
        {"key": "VM_BACKFILL_CMD", "value": measure_cmd},
        {"key": "VM_ASSET_GROUP", "value": asset_group},
        {"key": "VM_NAME", "value": vm_name},
        {"key": "DEPLOYMENT_ENV", "value": deployment_env},
        {"key": "VM_SHUTDOWN_ON_COMPLETION", "value": "true"},
    ]

    body: dict[str, object] = {
        "name": vm_name,
        "machineType": f"zones/{_ZONE}/machineTypes/e2-standard-2",
        "disks": [
            {
                "boot": True,
                "autoDelete": True,
                "initializeParams": {
                    "sourceImage": _DISK_IMAGE,
                    "diskSizeGb": "50",
                },
            }
        ],
        "networkInterfaces": [
            {
                "network": f"projects/{project}/global/networks/default",
                "accessConfigs": [{"type": "ONE_TO_ONE_NAT", "name": "External NAT"}],
            }
        ],
        "serviceAccounts": [
            {
                "email": "default",
                "scopes": ["https://www.googleapis.com/auth/cloud-platform"],
            }
        ],
        "metadata": {"items": metadata_items},
        "labels": {
            "purpose": "measure-honest-coverage",
            "asset-group": asset_group,
            "env": deployment_env,
            "run-ts": run_ts,
        },
    }

    resp = session.post(url, json=body, timeout=30)
    resp.raise_for_status()
    op = cast(dict[str, str], resp.json())
    return op.get("name", "<unknown>")


def _parse_args() -> _Args:
    parser = argparse.ArgumentParser(description="Launch honest-coverage measurement VM")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--asset-group", default="all", dest="asset_group")
    parser.add_argument("--env", default="prod")
    ns = parser.parse_args()
    return _Args(
        project=cast(str, ns.project),
        asset_group=cast(str, ns.asset_group),
        env=cast(str, ns.env),
    )


def main() -> None:
    args = _parse_args()

    logger.info("Checking for running honest-coverage VMs in zone %s", _ZONE)
    session = _authed_session()

    running = _list_running_vms(session, args.project)
    if running:
        logger.info(
            "SKIP: honest-coverage VM already running: %s. Concurrent runs race on GCS output.",
            running[0],
        )
        sys.exit(0)

    today = datetime.datetime.now(tz=datetime.UTC).strftime("%Y%m%d-%H%M%S")
    vm_name = f"{_VM_PREFIX}{today}"

    logger.info(
        "Launching %s: asset_group=%s env=%s",
        vm_name,
        args.asset_group,
        args.env,
    )
    op_name = _create_vm(session, args.project, vm_name, args.asset_group, args.env)
    logger.info("VM creation operation: %s", op_name)
    output_date = datetime.datetime.now(tz=datetime.UTC).strftime("%Y-%m-%d")
    output_bucket = f"{args.project}-honest-coverage"
    logger.info(
        "Output (when complete): gsutil cat gs://%s/%s/coverage.json",
        output_bucket,
        output_date,
    )
    logger.info("Done — VM launched, job complete.")


if __name__ == "__main__":
    main()
