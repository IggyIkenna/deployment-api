# Epic: observability_master
# Lifecycle: permanent
"""Unit tests for the AWS parity of the unified deployment inventory (Phase 5).

Credential-free / ``--block-network`` safe. Two layers:

* **moto** (``@mock_aws``, skipped when moto is absent) — stands up real EC2 instances
  + an AWS Batch job and asserts ``list_ec2_census`` / ``list_batch_census`` discover
  + tag them; this proves the boto3 census seam against the real AWS API surface.
* **pure** (always runs) — ``build_aws_inventory`` over census dataclasses + a fake S3
  ``StorageClient`` proves the classification → ``cloud=AWS`` mapping, the umbrella
  derivation, and the exit-137 EXIT_STATUS → ``status=failed exit_code=137`` path
  without needing moto. Plus a route test that the GCP path is unchanged (no AWS
  regression on the GCP items).

Pins: an EC2 backfill VM + a Batch job appear as classified ``cloud=AWS`` items with the
correct umbrella + status; a terminated exit-137 instance → ``status=failed
exit_code=137`` (S3 EXIT_STATUS mocked); the GCP inventory still returns its items.
"""

from __future__ import annotations

import importlib
import os
from datetime import UTC, datetime

import pytest


def _repair_deployment_service_namespace() -> None:
    """Ensure ``deployment_service`` sub-packages import under pytest's collector.

    The editable ``deployment_service`` is a path-``.pth`` install; pytest's
    ``prepend`` import mode can bind ``deployment_service`` as a namespace package with
    an empty ``__path__`` at collection time, so its sub-packages (``backends`` /
    ``data_pipeline_monitors``) then fail to import. We repair the namespace ``__path__``
    to the real on-disk package dir — a TEST-HARNESS fix only (production uvicorn loads
    the real package normally). No-op when ``__path__`` is already populated.
    """
    ds = importlib.import_module("deployment_service")
    ds_file = getattr(ds, "__file__", None)
    paths = list(getattr(ds, "__path__", []))
    if ds_file and paths:
        return
    # The editable .pth points at the deployment-service repo root.
    for entry in __import__("sys").path:
        candidate = os.path.join(entry, "deployment_service")
        if os.path.isfile(os.path.join(candidate, "__init__.py")) and candidate not in paths:
            ds.__path__.append(candidate)  # pyright: ignore[reportAttributeAccessIssue]
    importlib.invalidate_caches()


_repair_deployment_service_namespace()

os.environ.setdefault("CLOUD_MOCK_MODE", "false")
os.environ.setdefault("CLOUD_PROVIDER", "local")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("DISABLE_AUTH", "true")
os.environ.setdefault("MOCK_STATE_MODE", "deterministic")
os.environ.setdefault("AWS_DEFAULT_REGION", "ap-northeast-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")

pytestmark = [pytest.mark.timeout(60)]

_REGION = "ap-northeast-1"
_ACCOUNT = "427895769566"
_LOG_BUCKET = f"unified-trading-deployment-scripts-{_ACCOUNT}"


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeStorageClient:
    """Minimal in-memory S3 ``StorageClient`` for the durable EXIT_STATUS read.

    Implements only the two methods ``read_terminal_exit_code`` touches
    (``blob_exists`` / ``download_bytes``) keyed by ``(bucket, path)``.
    """

    def __init__(self, blobs: dict[tuple[str, str], bytes]) -> None:
        self._blobs = blobs

    def blob_exists(self, bucket: str, blob_path: str) -> bool:
        return (bucket, blob_path) in self._blobs

    def download_bytes(self, bucket: str, blob_path: str) -> bytes:
        return self._blobs[(bucket, blob_path)]


def _lifecycle_for_name(name: str) -> str:
    """Reuse the inventory route's curated prefix→lifecycle resolver (GCP-parity)."""
    from deployment_api.routes.deployments_inventory import _vm_lifecycle_class  # pyright: ignore[reportPrivateUsage]

    return _vm_lifecycle_class(name)


# ---------------------------------------------------------------------------
# Pure classification + exit-code mapping (always runs — no moto)
# ---------------------------------------------------------------------------


def test_build_aws_inventory_classifies_ec2_and_batch() -> None:
    from deployment_service.backends.aws_census import AwsBatchJobCensus, AwsInstanceCensus

    from deployment_api.routes._aws_deployments import build_aws_inventory

    instances = [
        # Running backfill EC2 → BATCH umbrella (mtds-backfill- prefix → EPHEMERAL_BATCH).
        AwsInstanceCensus(
            name="mtds-backfill-cefi-20260622-aws",
            instance_id="i-0aaa",
            state="running",
            asset_group="cefi",
            launch_time=datetime(2026, 6, 22, 11, 30, tzinfo=UTC),
        ),
        # Long-lived live strategy EC2 → LIVE (strat-live- → LONG_LIVED_LIVE)... but the
        # GCP registry uses strategy-live-; AWS launchers use strat-live-. The route
        # resolver is GCP-shaped, so use a name the shared registry classifies: a
        # strategy-live- live VM (LONG_LIVED_LIVE).
        AwsInstanceCensus(
            name="strategy-live-cefi-aws-20260620",
            instance_id="i-0bbb",
            state="running",
            asset_group="cefi",
            launch_time=datetime(2026, 6, 20, tzinfo=UTC),
        ),
    ]
    batch_jobs = [
        AwsBatchJobCensus(
            name="mtds-backfill-defi-20260622-aws",
            job_id="job-1",
            status="SUCCEEDED",
            asset_group="defi",
            created_at=datetime(2026, 6, 22, 5, 0, tzinfo=UTC),
            stopped_at=datetime(2026, 6, 22, 6, 0, tzinfo=UTC),
            exit_code=0,
            status_reason="",
        )
    ]

    items = build_aws_inventory(instances, batch_jobs, _lifecycle_for_name, None, _LOG_BUCKET)
    by_name = {i["name"]: i for i in items}

    # Every item is AWS and classified into exactly one umbrella.
    assert all(i["cloud"] == "AWS" for i in items)
    assert all(i["umbrella"] in {"LIVE", "BATCH", "PAPER", "EXPERIMENT"} for i in items)

    ec2 = by_name["mtds-backfill-cefi-20260622-aws"]
    assert ec2["kind"] == "VM"
    assert ec2["umbrella"] == "BATCH"
    assert ec2["status"] == "running"
    assert ec2["asset_group"] == "cefi"

    live = by_name["strategy-live-cefi-aws-20260620"]
    assert live["umbrella"] == "LIVE"

    job = by_name["mtds-backfill-defi-20260622-aws"]
    assert job["kind"] == "CLOUD_RUN_JOB"  # AWS Batch = the Cloud-Run analogue
    assert job["umbrella"] == "BATCH"
    assert job["status"] == "succeeded"
    assert job["exit_code"] == 0


def test_build_aws_inventory_terminated_exit_137_is_failed() -> None:
    """A terminated EC2 backfill with a durable EXIT_STATUS=137 blob → failed/137."""
    from deployment_service.backends.aws_census import AwsInstanceCensus
    from deployment_service.data_pipeline_monitors._gcs import EXIT_STATUS_BLOB

    from deployment_api.routes._aws_deployments import build_aws_inventory

    inst = AwsInstanceCensus(
        name="mtds-backfill-defi-20260622-oom",
        instance_id="i-0ccc",
        state="terminated",
        asset_group="defi",
        launch_time=datetime(2026, 6, 22, 3, 0, tzinfo=UTC),
    )
    storage = _FakeStorageClient({(_LOG_BUCKET, EXIT_STATUS_BLOB.format(vm=inst.name)): b"137\n"})
    items = build_aws_inventory([inst], [], _lifecycle_for_name, storage, _LOG_BUCKET)  # type: ignore[arg-type]
    assert len(items) == 1
    item = items[0]
    assert item["cloud"] == "AWS"
    assert item["status"] == "failed"
    assert item["exit_code"] == 137
    assert item["umbrella"] == "BATCH"


def test_build_aws_inventory_terminated_no_exit_blob_is_stopped() -> None:
    """A terminated instance with no durable code → stopped/None (never fabricated 0)."""
    from deployment_service.backends.aws_census import AwsInstanceCensus

    from deployment_api.routes._aws_deployments import build_aws_inventory

    inst = AwsInstanceCensus(
        name="mtds-backfill-cefi-gone",
        instance_id="i-0ddd",
        state="terminated",
        asset_group="cefi",
        launch_time=datetime(2026, 6, 22, tzinfo=UTC),
    )
    storage = _FakeStorageClient({})  # no EXIT_STATUS blob
    items = build_aws_inventory([inst], [], _lifecycle_for_name, storage, _LOG_BUCKET)  # type: ignore[arg-type]
    assert items[0]["status"] == "stopped"
    assert items[0]["exit_code"] is None


def test_build_aws_inventory_batch_failed_synthesises_nonzero() -> None:
    from deployment_service.backends.aws_census import AwsBatchJobCensus

    from deployment_api.routes._aws_deployments import build_aws_inventory

    job = AwsBatchJobCensus(
        name="mtds-backfill-tradfi-failjob",
        job_id="job-2",
        status="FAILED",
        asset_group="tradfi",
        created_at=datetime(2026, 6, 22, tzinfo=UTC),
        stopped_at=datetime(2026, 6, 22, 1, 0, tzinfo=UTC),
        exit_code=None,  # infra failure → no container rc
        status_reason="Essential container in task exited",
    )
    items = build_aws_inventory([], [job], _lifecycle_for_name, None, _LOG_BUCKET)
    assert items[0]["status"] == "failed"
    assert items[0]["exit_code"] == 1  # synthesised non-zero so the UI shows it red


# ---------------------------------------------------------------------------
# moto census (skipped when moto is absent — runs in the [aws]-extra CI env)
# ---------------------------------------------------------------------------


def test_list_ec2_census_discovers_tagged_backfill_instances() -> None:
    moto = pytest.importorskip("moto")
    import boto3

    with moto.mock_aws():
        ec2 = boto3.client("ec2", region_name=_REGION)
        # moto's default VPC/subnet exists; launch one tagged backfill instance.
        reservation = ec2.run_instances(
            ImageId="ami-12345678",
            InstanceType="t3.medium",
            MinCount=1,
            MaxCount=1,
            TagSpecifications=[
                {
                    "ResourceType": "instance",
                    "Tags": [
                        {"Key": "Name", "Value": "mtds-backfill-cefi-20260622-aws"},
                        {"Key": "asset-group", "Value": "cefi"},
                        {"Key": "cloud", "Value": "aws"},
                    ],
                }
            ],
        )
        instance_id = reservation["Instances"][0]["InstanceId"]

        from deployment_service.backends.aws_census import list_ec2_census

        census = list_ec2_census(region=_REGION)
        names = {c.name for c in census}
        assert "mtds-backfill-cefi-20260622-aws" in names
        found = next(c for c in census if c.name == "mtds-backfill-cefi-20260622-aws")
        assert found.asset_group == "cefi"
        assert found.instance_id == instance_id
        assert found.state in {"running", "pending"}


def test_list_batch_census_discovers_submitted_job() -> None:
    moto = pytest.importorskip("moto")
    import boto3

    with moto.mock_aws():
        batch = boto3.client("batch", region_name=_REGION)
        ec2 = boto3.client("ec2", region_name=_REGION)
        iam = boto3.client("iam", region_name=_REGION)

        # Minimal Batch setup (Fargate compute env + queue + job def).
        vpcs = ec2.describe_vpcs()["Vpcs"]
        subnets = ec2.describe_subnets()["Subnets"]
        sg = ec2.describe_security_groups()["SecurityGroups"][0]["GroupId"]
        role = iam.create_role(RoleName="batch-role", AssumeRolePolicyDocument="{}")["Role"]["Arn"]

        compute_env = batch.create_compute_environment(
            computeEnvironmentName="ce-test",
            type="MANAGED",
            computeResources={
                "type": "FARGATE",
                "maxvCpus": 4,
                "subnets": [s["SubnetId"] for s in subnets],
                "securityGroupIds": [sg],
            },
            serviceRole=role,
        )["computeEnvironmentArn"]
        batch.create_job_queue(
            jobQueueName="unified-trading-job-queue",
            state="ENABLED",
            priority=1,
            computeEnvironmentOrder=[{"order": 1, "computeEnvironment": compute_env}],
        )
        job_def = batch.register_job_definition(
            jobDefinitionName="jd-test",
            type="container",
            platformCapabilities=["FARGATE"],
            containerProperties={
                "image": "busybox",
                "resourceRequirements": [
                    {"type": "VCPU", "value": "0.25"},
                    {"type": "MEMORY", "value": "512"},
                ],
                "executionRoleArn": role,
            },
        )["jobDefinitionArn"]
        batch.submit_job(
            jobName="mtds-backfill-defi-20260622-aws",
            jobQueue="unified-trading-job-queue",
            jobDefinition=job_def,
            tags={"asset-group": "defi"},
        )
        assert vpcs  # default VPC present

        from deployment_service.backends.aws_census import list_batch_census

        census = list_batch_census(region=_REGION, job_queue="unified-trading-job-queue")
        names = {c.name for c in census}
        assert "mtds-backfill-defi-20260622-aws" in names


# ---------------------------------------------------------------------------
# Route — GCP unchanged when AWS census is empty (no regression)
# ---------------------------------------------------------------------------


def test_inventory_route_gcp_unchanged_with_empty_aws() -> None:
    """The live path: AWS census empty (boto3 degrade) → GCP items still returned."""
    from unittest.mock import patch

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from deployment_api.routes import deployments_inventory as mod

    app = FastAPI()
    app.include_router(mod.router, prefix="/api")
    client = TestClient(app, raise_server_exceptions=False)

    class _FakeEntry:
        def __init__(self, vm_name: str) -> None:
            self.vm_name = vm_name
            self.asset_group = "cefi"
            self.status = "running"
            self.started_at = "2026-06-22T11:00:00Z"
            self.last_heartbeat_at = "2026-06-22T11:59:30Z"
            self.completed_at = None
            self.exit_code = None
            self.rows_in = 0
            self.rows_out = 0
            self.rows_error = 0
            self.events_emitted = 0

    gcp_entry = _FakeEntry("cefi-binance-spot-20260622-gcp")
    mod._inventory_cache.clear()  # pyright: ignore[reportPrivateUsage]  # isolate the short-TTL cache

    with (
        patch.object(mod, "_cfg") as mock_cfg,
        # The GCP VM census is read via the parallel loader seam — patch it directly.
        patch.object(mod, "_load_gcp_vm_entries", return_value=([gcp_entry], {})),
        patch.object(mod, "latest_execution_by_job", return_value={}),
        # AWS census degrades to empty (no creds / boto3) — returns no AWS items.
        patch.object(mod, "load_aws_inventory", return_value=[]),
    ):
        mock_cfg.is_mock_mode.return_value = False
        mock_cfg.require_gcp_project_id.return_value = "test-project"
        mock_cfg.aws_codebuild_region = _REGION
        mock_cfg.aws_account_id = _ACCOUNT
        resp = client.get("/api/deployments/inventory")
    assert resp.status_code == 200
    names = {i["name"] for i in resp.json()["items"]}
    assert "cefi-binance-spot-20260622-gcp" in names  # GCP unchanged
    assert all(i["cloud"] == "GCP" for i in resp.json()["items"])


def test_inventory_route_includes_aws_items() -> None:
    """An unset cloud filter includes the AWS items the census yields."""
    from unittest.mock import patch

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from deployment_api.routes import deployments_inventory as mod

    app = FastAPI()
    app.include_router(mod.router, prefix="/api")
    client = TestClient(app, raise_server_exceptions=False)

    aws_items = [
        {
            "name": "mtds-backfill-cefi-20260622-aws",
            "kind": "VM",
            "umbrella": "BATCH",
            "cloud": "AWS",
            "service": "mtds-backfill",
            "asset_group": "cefi",
            "status": "running",
            "last_run_at": "2026-06-22T11:30:00Z",
            "exit_code": None,
            "heartbeat_age_seconds": None,
            "captured_progress": None,
            "run_log_uri": "",
        }
    ]

    mod._inventory_cache.clear()  # pyright: ignore[reportPrivateUsage]  # isolate the short-TTL cache

    with (
        patch.object(mod, "_cfg") as mock_cfg,
        # GCP VM census empty (no running VMs) — read via the parallel loader seam.
        patch.object(mod, "_load_gcp_vm_entries", return_value=([], {})),
        patch.object(mod, "latest_execution_by_job", return_value={}),
        patch.object(mod, "load_aws_inventory", return_value=aws_items),
    ):
        mock_cfg.is_mock_mode.return_value = False
        mock_cfg.require_gcp_project_id.return_value = "test-project"
        mock_cfg.aws_codebuild_region = _REGION
        mock_cfg.aws_account_id = _ACCOUNT
        # Default (no cloud filter) → AWS items present.
        resp = client.get("/api/deployments/inventory")
        # cloud=aws filter → only AWS items.
        resp_aws = client.get("/api/deployments/inventory", params={"cloud": "aws"})
        # cloud=gcp filter → no AWS census even attempted (GCP-only).
        resp_gcp = client.get("/api/deployments/inventory", params={"cloud": "gcp"})

    body = resp.json()
    assert any(i["name"] == "mtds-backfill-cefi-20260622-aws" and i["cloud"] == "AWS" for i in body["items"])

    aws_body = resp_aws.json()
    assert aws_body["total"] == 1
    assert all(i["cloud"] == "AWS" for i in aws_body["items"])

    gcp_body = resp_gcp.json()
    assert all(i["cloud"] == "GCP" for i in gcp_body["items"])
