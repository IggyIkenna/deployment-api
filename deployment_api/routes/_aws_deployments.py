# Epic: observability_master
# Lifecycle: permanent
"""AWS compute census → classified deployment-inventory items (Phase 5 parity).

The unified deployment inventory (``GET /api/deployments/inventory``) shows GCP VMs +
Cloud Run jobs today; this helper adds the AWS estate under the SAME live/batch/paper
umbrellas with ``cloud=AWS`` so the ``/deployments`` UI renders GCP **and** AWS
uniformly. AWS rides the identical ``DeploymentTarget`` / ``classify_deployment_target``
contract — GCP behaviour is untouched.

Four compute kinds, mirroring the GCP shape:

* **EC2 backfill VMs** (``deployment_service.backends.aws_census.list_ec2_census``) →
  ``DeploymentItem(kind="VM", cloud="AWS")``. Status from the EC2 instance state;
  ``exit_code`` from the durable ``s3://unified-trading-deployment-scripts-{account}/
  vm-logs/{name}/EXIT_STATUS`` blob the AWS launchers stream (survives the VM's
  self-delete — the AWS twin of the GCP ``read_terminal_exit_code``).
* **AWS Batch Fargate jobs** (``list_batch_census``, the Cloud-Run analogue) →
  ``DeploymentItem(kind="CLOUD_RUN_JOB", cloud="AWS")``. Status + exit_code from the
  Batch job description directly (Batch reports a container exit code).
* **AWS ECS/Fargate services** (``list_ecs_census``, across the prod clusters) →
  ``DeploymentItem(kind="ECS_SERVICE", cloud="AWS", umbrella="NONE")``. Always-on —
  no live/batch/paper/experiment phase, so umbrella is always ``NONE`` (never derived
  via ``classify_deployment_target``, same as a Cloud-Run job's ``lifecycle_class=""``
  precedent in ``cloud_run_job_registry.py``). Always emitted, even at 0 running
  tasks — an intentional scale-to-zero must stay visible, not vanish (WS-B Open-Q7).
  Status is a conservative desired-vs-running placeholder; the full ``serving``/
  ``scaled-to-zero``/``dead``/``degraded`` sub-taxonomy is a separate WS-D task.
* **Lambda functions** (``list_lambda_census``) → ``DeploymentItem(kind="LAMBDA",
  cloud="AWS")``. Existence + config only (``Runtime``/``MemorySize``/``State`` from a
  single paginated ``list_functions`` call) — no invocation/error stats, those are
  CloudWatch-only (no host/cgroup to sample at the edge on Lambda).

Cloud-agnostic boundaries: the AWS control plane is reached ONLY through the sanctioned
``deployment_service.backends.aws_census`` seam (deferred boto3) — never an inline
``import boto3`` here; the durable EXIT_STATUS blob is read through the cloud-agnostic
``unified_trading_library.get_storage_client(provider="aws")`` S3 client — never
``boto3.client("s3")`` directly. Honest degradation: a census / S3 read failure is
logged and yields no items / ``exit_code=None`` so the inventory never crashes and the
GCP path is unaffected.

SSOT: ``plans/active/deployment_observability_parity_live_batch_paper_2026_06_22.md``
Phase 5 + ``codex/05-infrastructure/deployment-observability.md``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

from deployment_service.deployment_classification import (
    UnclassifiedDeploymentError,
    classify_deployment_target,
)
from unified_api_contracts import (
    DeploymentCloud,
    DeploymentKind,
    DeploymentTarget,
    DeploymentUmbrella,
)
from unified_trading_library import StorageClient, get_storage_client

# Leaf module (no deployments_inventory import) — safe to import here despite
# deployments_inventory importing THIS module (the circular-import note below).
from deployment_api.routes._service_health import ecs_service_health_status

if TYPE_CHECKING:
    from deployment_service.backends.aws_census import (
        AwsBatchJobCensus,
        AwsEcsServiceCensus,
        AwsInstanceCensus,
        AwsLambdaFunctionCensus,
    )

logger = logging.getLogger(__name__)

# The ``deployment_service.backends`` + ``data_pipeline_monitors`` sub-packages are
# imported LAZILY inside ``load_aws_inventory`` (the same deferred pattern
# ``_cloud_run_executions`` uses for ``_gcp_sdk``): they pull boto3 / sub-package
# machinery that need only exist at call time, so a top-level import is avoided —
# this also keeps the route importable in environments that resolve
# ``deployment_service`` as a namespace package at collection time.

# A VM/job name → ``lifecycle_class`` string resolver (the inventory route passes its
# curated prefix-registry resolver so AWS reuses the SAME umbrella derivation as GCP).
LifecycleResolver = Callable[[str], str]

# One classified inventory item as a plain dict (the route turns these into
# ``DeploymentItem`` models — keeping this module free of the route's pydantic model
# avoids a circular import: deployments_inventory imports this module).
DeploymentItemDict = dict[str, object]

# The durable VM log/EXIT_STATUS bucket on AWS (mirrors the launcher trap block
# ``lc_aws_log_upload_trap_block`` in aws_ec2_launch_lib.sh:
# s3://unified-trading-deployment-scripts-{account}/vm-logs/{vm}/EXIT_STATUS).
_AWS_LOG_BUCKET_TPL = "unified-trading-deployment-scripts-{account}"

# EC2 instance-state → inventory wire status. ``terminated``/``stopped`` are resolved
# against the durable EXIT_STATUS blob (0 → succeeded, non-zero → failed); a terminal
# instance with no durable code reads as ``stopped`` (gone, exit unknown — never a
# fabricated success).
_EC2_RUNNING_STATES = frozenset({"pending", "running", "shutting-down", "stopping"})
_EC2_TERMINAL_STATES = frozenset({"terminated", "stopped"})

# AWS Batch status → inventory wire status (the Cloud-Run-job analogue).
_BATCH_STATUS_MAP: dict[str, str] = {
    "SUBMITTED": "pending",
    "PENDING": "pending",
    "RUNNABLE": "pending",
    "STARTING": "running",
    "RUNNING": "running",
    "SUCCEEDED": "succeeded",
    "FAILED": "failed",
}

# Lambda function State → inventory wire status. Existence-only census: "running"
# here means "deployed and ready to invoke", not "currently executing" (Lambda has
# no persistent running process to observe) — the honest existence-only reading the
# plan scopes this census to (no CloudWatch invocation/error signal).
_LAMBDA_STATE_MAP: dict[str, str] = {
    "Active": "running",
    "Pending": "pending",
    "Failed": "failed",
    "Inactive": "stopped",
}


def _aws_storage_client(region: str) -> StorageClient | None:
    """An S3 ``StorageClient`` for the durable EXIT_STATUS reads, or ``None`` on failure.

    Cloud-agnostic: ``get_storage_client(provider="aws")`` — never ``boto3.client("s3")``.
    A construction failure (no creds / boto3 absent) degrades to ``None`` so exit codes
    read as unknown rather than crashing the inventory.
    """
    try:
        return get_storage_client(provider="aws", region_name=region)
    except Exception as exc:
        logger.warning("AWS storage client unavailable (exit codes degrade to unknown): %s", exc)
        return None


def _ec2_status_and_exit(
    inst: AwsInstanceCensus,
    storage_client: StorageClient | None,
    log_bucket: str,
) -> tuple[str, int | None]:
    """Resolve the wire status + exit_code for one EC2 instance.

    Running states map straight through. A terminal state (``terminated``/``stopped``)
    reads the durable EXIT_STATUS blob: 0 → ``succeeded``, non-zero (incl. 137 OOM) →
    ``failed``; absent → ``stopped`` with ``exit_code=None`` (gone, never a fabricated
    success — the same honesty the GCP fleet monitor uses).
    """
    state = inst.state
    if state in _EC2_RUNNING_STATES:
        return ("running" if state in ("running", "shutting-down") else "pending"), None
    if state in _EC2_TERMINAL_STATES:
        exit_code: int | None = None
        if storage_client is not None:
            try:
                from deployment_service.data_pipeline_monitors._gcs import read_terminal_exit_code

                exit_code = read_terminal_exit_code(storage_client, log_bucket, inst.name)
            except Exception as exc:
                logger.warning("EXIT_STATUS read failed for %s: %s", inst.name, exc)
        if exit_code is None:
            return "stopped", None
        return ("succeeded" if exit_code == 0 else "failed"), exit_code
    return state or "unknown", None


def _ec2_item(
    inst: AwsInstanceCensus,
    storage_client: StorageClient | None,
    log_bucket: str,
    lifecycle_for_name: LifecycleResolver,
) -> DeploymentItemDict | None:
    """Classify one EC2 instance into an inventory item dict, or ``None`` if unclassifiable."""
    try:
        target: DeploymentTarget = classify_deployment_target(
            inst.name,
            lifecycle_class=lifecycle_for_name(inst.name),
            cloud=DeploymentCloud.AWS,
            kind=DeploymentKind.VM,
            asset_group=inst.asset_group or None,
        )
    except UnclassifiedDeploymentError as exc:
        logger.warning("AWS inventory: skipping unclassifiable EC2 instance %r: %s", inst.name, exc)
        return None
    status, exit_code = _ec2_status_and_exit(inst, storage_client, log_bucket)
    return {
        "name": inst.name,
        # Provenance (WS-D). The AWS estate has no deployment registry / managed-by tag yet (the
        # DEVOPS launcher-label todo), so we cannot honestly claim "deployment-api" or "adhoc" for
        # an EC2 backfill VM — degrade to "unknown" (never a fabricated deployment-api). Wire values
        # mirror deployments_inventory.LAUNCHED_BY_* (literals here to avoid a route↔helper cycle).
        "launched_by": "unknown",
        "kind": target.kind.value,
        "umbrella": target.umbrella.value,
        "cloud": target.cloud.value,
        "service": target.service,
        "asset_group": target.asset_group,
        "status": status,
        "last_run_at": inst.launch_time.isoformat() if inst.launch_time else None,
        "exit_code": exit_code,
        "heartbeat_age_seconds": None,
        "captured_progress": None,
        "run_log_uri": "",
    }


def _batch_item(
    job: AwsBatchJobCensus,
    lifecycle_for_name: LifecycleResolver,
) -> DeploymentItemDict | None:
    """Classify one AWS Batch (Fargate) job into an inventory item dict (Cloud-Run analogue)."""
    try:
        target: DeploymentTarget = classify_deployment_target(
            job.name,
            lifecycle_class=lifecycle_for_name(job.name),
            cloud=DeploymentCloud.AWS,
            kind=DeploymentKind.CLOUD_RUN_JOB,
            asset_group=job.asset_group or None,
        )
    except UnclassifiedDeploymentError as exc:
        logger.warning("AWS inventory: skipping unclassifiable Batch job %r: %s", job.name, exc)
        return None
    status = _BATCH_STATUS_MAP.get(job.status, "unknown")
    last_run = job.stopped_at or job.created_at
    exit_code = job.exit_code
    # A FAILED Batch job with no container exit code (e.g. infra/timeout failure) still
    # reports failed — synthesise a non-zero so the UI shows it red (mirrors the GCP
    # Cloud-Run 0/1 synthesis from succeeded/failed counts).
    if status == "failed" and exit_code is None:
        exit_code = 1
    if status == "succeeded" and exit_code is None:
        exit_code = 0
    return {
        "name": job.name,
        "launched_by": "unknown",  # no AWS job registry/tag signal yet — honest "unknown" (see _ec2_item)
        "kind": target.kind.value,
        "umbrella": target.umbrella.value,
        "cloud": target.cloud.value,
        "service": target.service,
        "asset_group": target.asset_group,
        "status": status,
        "last_run_at": last_run.isoformat() if last_run else None,
        "exit_code": exit_code,
        "heartbeat_age_seconds": None,
        "captured_progress": None,
        "run_log_uri": "",
    }


def _ecs_status(census: AwsEcsServiceCensus) -> str:
    """A conservative desired-vs-running status (the full service sub-taxonomy is a
    separate WS-D task — this never fabricates a ``serving``/``dead`` verdict this
    data alone can't support).

    ``running_count>0`` → ``running``; ``desired_count==0`` → ``stopped`` (an
    explicit scale-to-zero, off on purpose); otherwise ``unknown`` (desired>0 but
    nothing running — exactly the ambiguous case the sub-taxonomy task resolves).
    """
    if census.running_count > 0:
        return "running"
    if census.desired_count == 0:
        return "stopped"
    return "unknown"


def _ecs_service_item(census: AwsEcsServiceCensus) -> DeploymentItemDict:
    """Classify one ECS service into an inventory item dict.

    Services have no live/batch/paper/experiment phase (Open-Q1) — umbrella is
    always ``DeploymentUmbrella.NONE``, constructed directly rather than via
    ``classify_deployment_target`` (which requires a VM ``lifecycle_class``), the
    same precedent ``cloud_run_job_registry.py`` sets for kinds with no natural
    lifecycle_class. The caller never filters these out (Open-Q7: always emitted,
    even at 0 running tasks). ``asset_group`` stays ``""`` (cross-asset infra) —
    ECS service creation carries no asset-group tag today (see
    ``deployment-service/scripts/aws/deploy-ecs-fargate.sh``).
    """
    last_run = census.updated_at or census.created_at
    return {
        "name": census.name,
        "launched_by": "control-plane",  # always-on platform service (the AWS twin of a Cloud Run service)
        "kind": DeploymentKind.ECS_SERVICE.value,
        "umbrella": DeploymentUmbrella.NONE.value,
        "cloud": DeploymentCloud.AWS.value,
        "service": census.name,
        "asset_group": "",
        "status": _ecs_status(census),
        # D.3 service sub-taxonomy (serving/scaled-to-zero/dead/degraded) — the composite
        # health chip, derived from desired-vs-running. desired==0 is an intentional
        # scale-to-zero (neutral), desired>0 & running==0 is dead, partial is degraded.
        "composite_health_status": ecs_service_health_status(census.desired_count, census.running_count),
        "last_run_at": last_run.isoformat() if last_run else None,
        "exit_code": None,
        "heartbeat_age_seconds": None,
        "captured_progress": None,
        "run_log_uri": "",
        "cluster": census.cluster,
        "desired_count": census.desired_count,
        "running_count": census.running_count,
        "task_definition_revision": census.task_definition_revision,
    }


def _lambda_item(
    fn: AwsLambdaFunctionCensus,
    lifecycle_for_name: LifecycleResolver,
) -> DeploymentItemDict | None:
    """Classify one Lambda function into an inventory item dict — existence + config only.

    No ``exit_code`` / ``heartbeat_age_seconds`` / ``captured_progress`` (Lambda has no
    single "run" with an exit code, no heartbeat, no manifest write-progress in this
    census — invocation-level signal is the scoped CloudWatch exception, not added
    here per the plan's "default to existence-only" instruction).
    """
    try:
        target: DeploymentTarget = classify_deployment_target(
            fn.name,
            lifecycle_class=lifecycle_for_name(fn.name),
            cloud=DeploymentCloud.AWS,
            kind=DeploymentKind.LAMBDA,
        )
    except UnclassifiedDeploymentError as exc:
        logger.warning("AWS inventory: skipping unclassifiable Lambda function %r: %s", fn.name, exc)
        return None
    return {
        "name": fn.name,
        "launched_by": "control-plane",  # platform function (the AWS twin of a Cloud Function)
        "kind": target.kind.value,
        "umbrella": target.umbrella.value,
        "cloud": target.cloud.value,
        "service": target.service,
        "asset_group": target.asset_group,
        "status": _LAMBDA_STATE_MAP.get(fn.state, "unknown"),
        # Lambda has NO honest last-INVOKE time without the paid CloudWatch metric (deliberately
        # avoided — the reason we lean on Athena/BigQuery). last_run_at stays None (honest-absent),
        # never the last-DEPLOY time mislabelled as a run; the deploy time surfaces as
        # last_modified_at, which the UI renders as "last modified" + a tooltip.
        "last_run_at": None,
        "last_modified_at": fn.last_modified.isoformat() if fn.last_modified else None,
        "exit_code": None,
        "heartbeat_age_seconds": None,
        "captured_progress": None,
        "run_log_uri": "",
        "runtime": fn.runtime,
        "memory_size_mb": fn.memory_size_mb,
        "package_type": fn.package_type,
    }


def build_aws_inventory(
    instances: list[AwsInstanceCensus],
    batch_jobs: list[AwsBatchJobCensus],
    ecs_services: list[AwsEcsServiceCensus],
    lifecycle_for_name: LifecycleResolver,
    storage_client: StorageClient | None,
    log_bucket: str,
    lambda_functions: list[AwsLambdaFunctionCensus] | None = None,
) -> list[DeploymentItemDict]:
    """Assemble AWS inventory item dicts (EC2 VMs + Batch jobs + ECS services + Lambda
    functions), classified by umbrella.

    Pure over its inputs (the census lists + a lifecycle resolver + the EXIT_STATUS
    storage client) so it is moto-testable without a live route. An unclassifiable
    VM/job/Lambda name is logged + skipped per-row (never crashes the inventory); ECS
    services always classify (no lifecycle_class needed — see ``_ecs_service_item``).
    """
    items: list[DeploymentItemDict] = []
    for inst in instances:
        item = _ec2_item(inst, storage_client, log_bucket, lifecycle_for_name)
        if item is not None:
            items.append(item)
    for job in batch_jobs:
        item = _batch_item(job, lifecycle_for_name)
        if item is not None:
            items.append(item)
    for svc in ecs_services:
        items.append(_ecs_service_item(svc))
    for fn in lambda_functions or []:
        lambda_deployment_item = _lambda_item(fn, lifecycle_for_name)
        if lambda_deployment_item is not None:
            items.append(lambda_deployment_item)
    return items


def load_aws_inventory(
    *,
    region: str,
    aws_account_id: str,
    lifecycle_for_name: LifecycleResolver,
    instance_id_by_name: dict[str, str] | None = None,
) -> list[DeploymentItemDict]:
    """Census + classify the live AWS estate into inventory item dicts.

    Calls the sanctioned ``aws_census`` seam (deferred boto3) for the EC2 + Batch +
    Lambda census, resolves the durable-log S3 bucket from the account id, and builds
    the classified items. Each census kind degrades to an empty list independently on
    any AWS error (no creds / boto3 absent / API down / unsupported region) — a Lambda
    census failure never blocks EC2/Batch and vice versa (shard-level failure isolation
    across kinds).

    ``instance_id_by_name``, when passed, is UPDATED in place with this region's
    ``{instance_id: Name-tag}`` census pairs — the billing join key (an AWS CUR row's
    ``line_item_resource_id`` is an ARN/instance-id, never the friendly Name tag) needs
    this mapping, and this is the one place the raw ``AwsInstanceCensus`` objects (which
    carry both fields) exist before they're flattened into item dicts that drop
    ``instance_id``. Optional + additive so existing callers (``fleet_reconciliation``,
    tests) are unaffected.

    The census import is deferred (not a top-level import) to match the
    ``_cloud_run_executions`` ``_gcp_sdk`` pattern: ``deployment_service.backends`` pulls
    boto3 / sub-package machinery only needed at call time. ``find_spec`` gates the
    import (no try/except-ImportError shim) — an absent seam degrades to no AWS items.
    """
    import importlib
    import importlib.util

    # find_spec RAISES ModuleNotFoundError (not returns None) when an intermediate parent
    # package is itself absent (e.g. deployment_service stubbed without a real backends
    # sub-package) — that is exactly "seam unavailable", so degrade rather than 500.
    try:
        census_spec = importlib.util.find_spec("deployment_service.backends.aws_census")
    except ModuleNotFoundError:
        census_spec = None
    if census_spec is None:
        logger.warning("AWS census seam unavailable (degrading to no AWS items)")
        return []
    from deployment_service.backends.aws_census import (
        list_batch_census,
        list_ec2_census,
        list_ecs_census,
        list_lambda_census,
    )

    instances = list_ec2_census(region=region)
    if instance_id_by_name is not None:
        instance_id_by_name.update({inst.instance_id: inst.name for inst in instances if inst.instance_id})
    batch_jobs = list_batch_census(region=region)
    ecs_services = list_ecs_census(region=region)
    lambda_functions = list_lambda_census(region=region)
    if not instances and not batch_jobs and not ecs_services and not lambda_functions:
        return []
    log_bucket = _AWS_LOG_BUCKET_TPL.format(account=aws_account_id)
    # Only construct an S3 client when there is a terminal instance whose exit code we
    # actually need (running-only census needs no EXIT_STATUS read).
    needs_exit_read = any(i.state in _EC2_TERMINAL_STATES for i in instances)
    storage_client = _aws_storage_client(region) if needs_exit_read else None
    return build_aws_inventory(
        instances, batch_jobs, ecs_services, lifecycle_for_name, storage_client, log_bucket, lambda_functions
    )


__all__ = [
    "DeploymentItemDict",
    "LifecycleResolver",
    "build_aws_inventory",
    "load_aws_inventory",
]
