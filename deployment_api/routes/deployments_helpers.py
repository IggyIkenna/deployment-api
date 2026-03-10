"""
Helper functions for deployments routes.

Contains utility functions for log analysis, state management, and other common operations.
"""

import logging
import time
from typing import cast

from deployment_api.deployment_api_config import DeploymentApiConfig

logger = logging.getLogger(__name__)

# Deployment configuration (public alias for cross-module access)
_deployment_config = DeploymentApiConfig()
deployment_config: DeploymentApiConfig = _deployment_config

# Cache for verification results
_verification_cache: dict[str, dict[str, object]] = {}


def _set_verification_cache(deployment_id: str, data: dict[str, object]) -> None:
    """
    Set verification cache for a deployment.
    """
    _verification_cache[deployment_id] = {"data": data, "timestamp": time.time()}


def set_verification_cache(deployment_id: str, data: dict[str, object]) -> None:
    """Public alias for _set_verification_cache."""
    _set_verification_cache(deployment_id, data)


def build_deploy_env_vars(
    service: str,
    project_id: str,
    deployment_id: str,
    max_concurrent: int,
    deployment_mode: str = "vm",  # or "cloud-run"
    enable_direct_gcs: bool = False,
    shard_id: str | None = None,
    # Runtime topology env vars (Stream 3 — operational mode injection)
    deploy_mode: str = "batch",  # "batch" | "live"
    operational_mode: str = "",  # service-specific: "train_phase1", "execute", etc.
    cloud_provider: str = "gcp",  # "gcp" | "aws" | "local"
) -> dict[str, str]:
    """
    Build standardized environment variables for deployment.

    These are passed to the actual deployment containers and must match
    what the services expect. See: deployment env var standardization.

    Args:
        service: Service name (e.g. "ml-training-service").
        project_id: Cloud project / account ID.
        deployment_id: Unique deployment ID for correlation.
        max_concurrent: Maximum concurrent jobs / VMs.
        deployment_mode: Compute substrate ("vm", "cloud_run", "batch", "ec2").
        enable_direct_gcs: Pass ENABLE_DIRECT_GCS=true for high-throughput services.
        shard_id: Optional shard identifier for per-shard containers.
        deploy_mode: Runtime mode — "batch" (GCS transport) or "live" (PubSub/SQS).
        operational_mode: Service-specific operational sub-mode injected as
            OPERATIONAL_MODE (e.g. "train_phase1", "execute", "instrument").
            Empty string omits the var so services use their own default.
        cloud_provider: Cloud provider routing key ("gcp", "aws", "local").
    """
    env_vars = {
        "SERVICE_NAME": service,
        "PROJECT_ID": project_id,
        "DEPLOYMENT_ID": deployment_id,
        "MAX_CONCURRENT": str(max_concurrent),
        "DEPLOYMENT_MODE": deployment_mode,
        # Runtime topology env vars — consumed by services via UCI/UnifiedCloudConfig
        "RUNTIME_MODE": deploy_mode,
        "CLOUD_PROVIDER": cloud_provider,
    }

    # Inject OPERATIONAL_MODE only when non-empty so services can detect it was set
    if operational_mode:
        env_vars["OPERATIONAL_MODE"] = operational_mode

    # Optional shard-specific environment
    if shard_id:
        env_vars["SHARD_ID"] = shard_id

    # GCS direct access for performance-critical services
    if enable_direct_gcs:
        env_vars["ENABLE_DIRECT_GCS"] = "true"

    return env_vars


def maybe_add_direct_gcs(
    service: str, env_vars: dict[str, str], deployment_config: dict[str, object] | None = None
) -> dict[str, str]:
    """
    Conditionally add direct GCS environment variables for high-throughput services.

    Services like market-tick-data-handler benefit from direct GCS access
    instead of going through the API server.
    """
    # Services that benefit from direct GCS access
    direct_gcs_services = {
        "market-tick-data-handler",
        "market-data-processing-service",
        "features-delta-one-service",
    }

    if service in direct_gcs_services:
        env_vars["ENABLE_DIRECT_GCS"] = "true"
        if deployment_config:
            # Add any service-specific GCS configuration
            gcs_config_raw = deployment_config.get("gcs_config")
            gcs_config: dict[str, object] = (
                cast(dict[str, object], gcs_config_raw) if isinstance(gcs_config_raw, dict) else {}
            )
            if gcs_config:
                env_vars.update({f"GCS_{str(k).upper()}": str(v) for k, v in gcs_config.items()})

    return env_vars


# Backward-compatible alias (tests import the private name)
_maybe_add_direct_gcs = maybe_add_direct_gcs


def find_duplicate_running_shards(  # noqa: C901
    state_manager: object, service: str, deployment_id: str, shard_args_list: list[list[str]]
) -> list[dict[str, object]]:
    """
    Find any currently running deployments that would conflict with the new shards.

    Returns list of conflicts with details about the overlapping deployments.
    This is critical for preventing data corruption in services that process
    by date ranges or other dimensions.
    """
    conflicts: list[dict[str, object]] = []

    try:
        # Get all active deployments for this service
        list_fn = getattr(state_manager, "list_deployments", None)
        if not callable(list_fn):
            return conflicts
        active_deployments_raw: object = list_fn(
            service=service, status=["running", "pending", "resuming"]
        )
        active_deployments: list[dict[str, object]] = (
            cast(list[dict[str, object]], active_deployments_raw)
            if isinstance(active_deployments_raw, list)
            else []
        )

        # Convert new shard args to comparable format
        new_shard_signatures: set[str] = set()
        for shard_args in shard_args_list:
            # Extract key identifying parameters (service-specific logic)
            signature = _extract_shard_signature(service, shard_args)
            if signature:
                new_shard_signatures.add(signature)

        # Check each active deployment for overlaps
        for active_deployment in active_deployments:
            dep_id_raw = active_deployment.get("deployment_id")
            if not isinstance(dep_id_raw, str):
                continue
            if dep_id_raw == deployment_id:
                continue  # Skip self

            get_shards_fn = getattr(state_manager, "get_deployment_shards", None)
            if not callable(get_shards_fn):
                continue
            active_shards_raw: object = get_shards_fn(dep_id_raw)
            active_shards: list[dict[str, object]] = (
                cast(list[dict[str, object]], active_shards_raw)
                if isinstance(active_shards_raw, list)
                else []
            )

            for active_shard in active_shards:
                args_raw = active_shard.get("args")
                args_list: list[str] = (
                    cast(list[str], args_raw) if isinstance(args_raw, list) else []
                )
                active_signature = _extract_shard_signature(service, args_list)

                if active_signature and active_signature in new_shard_signatures:
                    conflicts.append(
                        {
                            "deployment_id": dep_id_raw,
                            "shard_id": active_shard.get("shard_id"),
                            "signature": active_signature,
                            "status": active_deployment.get("status"),
                            "started_at": active_deployment.get("started_at"),
                        }
                    )

    except (OSError, ValueError, RuntimeError) as e:
        logger.warning("Error checking for duplicate shards: %s", e)
        # Don't fail deployment due to conflict check failure

    return conflicts


def _extract_shard_signature(service: str, shard_args: list[str]) -> str | None:
    """
    Extract a signature from shard arguments that uniquely identifies the work.

    This is used to detect overlapping shards across deployments.
    Service-specific logic for what constitutes "overlapping work".
    """
    if not shard_args:
        return None

    signature_parts = [service]

    # Extract date range and category for most services
    for i, arg in enumerate(shard_args):
        if arg == "--start-date" and i + 1 < len(shard_args):
            signature_parts.append(f"start:{shard_args[i + 1]}")
        elif arg == "--end-date" and i + 1 < len(shard_args):
            signature_parts.append(f"end:{shard_args[i + 1]}")
        elif arg == "--category" and i + 1 < len(shard_args):
            signature_parts.append(f"cat:{shard_args[i + 1]}")
        elif arg == "--venue" and i + 1 < len(shard_args):
            signature_parts.append(f"venue:{shard_args[i + 1]}")

    return "|".join(signature_parts) if len(signature_parts) > 1 else None
