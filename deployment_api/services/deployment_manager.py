"""
Core deployment management operations.

This module handles the business logic for deployment operations
including creation, validation, quota checking, and execution.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable
from datetime import date as _date
from typing import TYPE_CHECKING, cast

from deployment_service.config_loader import ConfigLoader, _substitute_env_vars
from deployment_service.deployment.orchestrator import DeploymentOrchestrator
from deployment_service.shard_calculator import ShardCalculator
from unified_events_interface import log_event

from deployment_api import settings as _settings
from deployment_api.routes.deployment_validation import (
    _resolve_deploy_dates,
    generate_deployment_report,
    validate_deployment_request,
    validate_image_availability,
    validate_quota_requirements,
    validate_shard_configuration,
)
from deployment_api.routes.deployments_helpers import _build_deploy_env_vars
from deployment_api.utils.quota_requirements import (
    VmQuotaShape,
    multiply_resources,
    vm_quota_shape_from_compute_config,
)

if TYPE_CHECKING:
    from deployment_api.routes.deployments import DeployRequest

logger = logging.getLogger(__name__)

# Events system is initialized at application startup (main.py), not at import time


class DeploymentManager:
    """Manages deployment operations and business logic."""

    def __init__(self) -> None:
        """Initialize the deployment manager."""
        self.default_region = _settings.GCS_REGION or "asia-northeast1"
        self.default_project_id = _settings.GCP_PROJECT_ID
        self.default_max_concurrent = 100

    def validate_deployment_request(self, deploy_request: DeployRequest) -> dict[str, object] | None:
        """
        Validate deployment request parameters.

        Returns:
            Error dict if validation fails, None if valid
        """
        return cast(dict[str, object] | None, validate_deployment_request(deploy_request))

    def calculate_quota_requirements(self, deploy_request: DeployRequest, config_dir: str = "configs") -> dict[str, object]:
        """
        Calculate quota requirements for a deployment.

        Args:
            deploy_request: Deployment request object
            config_dir: Configuration directory path

        Returns:
            Dict containing quota requirements and recommendations
        """
        loader = ConfigLoader(config_dir)
        loader.load_service_config(deploy_request.service)

        # Calculate shards
        calculator: ShardCalculator = ShardCalculator(config_dir)

        try:
            _start_d = _date.fromisoformat(deploy_request.start_date) if deploy_request.start_date else None
            _end_d = _date.fromisoformat(deploy_request.end_date) if deploy_request.end_date else None
            shards = calculator.calculate_shards(
                service=deploy_request.service,
                start_date=_start_d,
                end_date=_end_d,
                max_shards=deploy_request.max_shards or 10000,
                cloud_config_path=deploy_request.cloud_config_path,
                respect_start_dates=not deploy_request.ignore_start_dates,
                skip_existing=deploy_request.deploy_missing,
                skip_dimensions=deploy_request.skip_dimensions or [],
                date_granularity_override=deploy_request.date_granularity,
                **deploy_request.filters,
            )
        except (ValueError, KeyError, FileNotFoundError) as e:
            log_event(
                "deployment.quota_calculation.failed",
                details={
                    "error_type": "shard_calculation_error",
                    "service": deploy_request.service,
                    "error_message": str(e),
                    "error_category": "configuration",
                },
            )
            raise ValueError(f"Failed to calculate shards: {e}") from e
        except OSError as e:
            log_event(
                "deployment.quota_calculation.failed",
                details={
                    "error_type": "configuration_access_error",
                    "service": deploy_request.service,
                    "error_message": str(e),
                    "error_category": "file_system",
                },
            )
            raise ValueError(f"Configuration access failed: {e}") from e

        total_shards: int = len(shards)
        if total_shards == 0:
            return {
                "total_shards": 0,
                "quota_ok": True,
                "message": "No shards to deploy",
                "quota_details": {},
                "recommendations": {},
            }

        # Get compute configuration
        compute_config: dict[str, object] = loader.get_scaled_compute_config(
            deploy_request.service,
            deploy_request.compute,
            deploy_request.max_workers,
            "venue" in (deploy_request.skip_dimensions or []),
        )

        # Calculate resource requirements
        max_concurrent: int = deploy_request.max_concurrent or self.default_max_concurrent
        if deploy_request.compute == "vm":
            vm_shape: VmQuotaShape = vm_quota_shape_from_compute_config(compute_config)
            single_vm_shape: dict[str, float] = vm_shape.per_shard()
            total_shape: dict[str, float] = multiply_resources(single_vm_shape, max_concurrent)
            recommended_concurrent: int = max_concurrent  # Simplified: no headroom data available here
        else:
            # Cloud Run quota calculation
            cpu_raw = compute_config.get("cpu", 2)
            memory_raw = compute_config.get("memory", "4Gi")
            cpu_val: int = int(cpu_raw) if isinstance(cpu_raw, (int, float)) else 2
            memory_str: str = str(memory_raw) if memory_raw is not None else "4Gi"
            single_vm_shape = {
                "cpu": float(cpu_val),
                "memory_gb": float(int(memory_str.replace("Gi", "")) if "Gi" in memory_str else 4),
            }
            total_shape = multiply_resources(single_vm_shape, max_concurrent)
            recommended_concurrent = max_concurrent

        return {
            "total_shards": total_shards,
            "max_concurrent": max_concurrent,
            "compute_config": compute_config,
            "resource_requirements": {
                "single_shard": single_vm_shape,
                "max_concurrent_total": total_shape,
            },
            "recommendations": {
                "recommended_max_concurrent": recommended_concurrent,
                "estimated_duration_minutes": max(5, total_shards / max_concurrent * 3),
            },
            "quota_ok": True,  # Simplified for now
        }

    def create_deployment(
        self,
        deploy_request: DeployRequest,
        config_dir: str = "configs",
        background_task_func: Callable[..., None] | None = None,
    ) -> dict[str, object]:
        """
        Create a new deployment.

        Args:
            deploy_request: Deployment request object
            config_dir: Configuration directory path
            background_task_func: Function to run background deployment

        Returns:
            Dict containing deployment info and shard list
        """
        # Generate deployment ID
        deployment_id: str = str(uuid.uuid4())

        log_event(
            "deployment.creation.started",
            details={
                "deployment_id": deployment_id,
                "service": deploy_request.service,
                "compute_type": deploy_request.compute,
                "region": deploy_request.region or self.default_region,
            },
        )

        # Validate request
        validation_error: dict[str, object] | None = self.validate_deployment_request(deploy_request)
        if validation_error:
            raise ValueError(str(validation_error))

        # Validate shard configuration
        loader_for_validation = ConfigLoader(config_dir)
        service_cfg: dict[str, object] = cast(dict[str, object], loader_for_validation.load_service_config(deploy_request.service))
        shard_error: dict[str, object] | None = cast(
            dict[str, object] | None,
            validate_shard_configuration(service_cfg, deploy_request),
        )
        if shard_error:
            raise ValueError(str(shard_error))

        # Validate quota requirements (simplified: no shape/count data here)
        # Note: full quota validation done in calculate_quota_requirements
        quota_error: dict[str, object] | None = cast(
            dict[str, object] | None,
            validate_quota_requirements({}, 0),
        )
        if quota_error:
            raise ValueError(str(quota_error))

        # Validate image availability
        compute_cfg_for_validation: dict[str, object] = cast(
            dict[str, object],
            loader_for_validation.get_compute_recommendation(deploy_request.service, deploy_request.compute),
        )
        region_for_validation: str = deploy_request.region or self.default_region
        raw_docker_image = service_cfg.get("docker_image")
        docker_image_for_validation: str = (
            _substitute_env_vars(str(raw_docker_image))
            if raw_docker_image
            else f"{region_for_validation}-docker.pkg.dev/{self.default_project_id}/{deploy_request.service}/{deploy_request.service}:latest"
        )
        image_error: dict[str, object] | None = cast(
            dict[str, object] | None,
            validate_image_availability(docker_image_for_validation, region_for_validation),
        )
        if image_error:
            raise ValueError(str(image_error))

        # Calculate shards
        calculator: ShardCalculator = ShardCalculator(config_dir)
        _start_d2 = _date.fromisoformat(deploy_request.start_date) if deploy_request.start_date else None
        _end_d2 = _date.fromisoformat(deploy_request.end_date) if deploy_request.end_date else None
        shards = calculator.calculate_shards(
            service=deploy_request.service,
            start_date=_start_d2,
            end_date=_end_d2,
            max_shards=deploy_request.max_shards or 10000,
            cloud_config_path=deploy_request.cloud_config_path,
            respect_start_dates=not deploy_request.ignore_start_dates,
            skip_existing=deploy_request.deploy_missing,
            skip_dimensions=deploy_request.skip_dimensions or [],
            date_granularity_override=deploy_request.date_granularity,
            **deploy_request.filters,
        )

        if not shards:
            raise ValueError("No shards to deploy after filtering")

        # Build shard list for API response
        shard_list: list[dict[str, object]] = []
        for shard in shards:
            shard_dict: dict[str, object] = {
                "shard_id": f"{shard.service}-{shard.shard_index}",
                "shard_index": shard.shard_index,
                "total_shards": shard.total_shards,
                "dimensions": shard.dimensions,
                "cli_args": shard.cli_command.split()[1:],  # Remove 'python -m service' part
            }
            shard_list.append(shard_dict)

        # Generate CLI command for the deployment
        cli_parts: list[str] = [
            "python",
            "-m",
            "deployment",
            "deploy",
            "--service",
            deploy_request.service,
            "--compute",
            deploy_request.compute,
            "--max-concurrent",
            str(deploy_request.max_concurrent or self.default_max_concurrent),
            "--region",
            deploy_request.region or self.default_region,
            "--deployment-id",
            deployment_id,
        ]

        if deploy_request.start_date:
            start_date_val = deploy_request.start_date
            start_str: str = start_date_val.isoformat() if isinstance(start_date_val, _date) else str(start_date_val)
            cli_parts.extend(["--start-date", start_str])
        if deploy_request.end_date:
            end_date_val = deploy_request.end_date
            end_str: str = end_date_val.isoformat() if isinstance(end_date_val, _date) else str(end_date_val)
            cli_parts.extend(["--end-date", end_str])
        if deploy_request.tag:
            cli_parts.extend(["--tag", deploy_request.tag])
        if deploy_request.vm_zone:
            cli_parts.extend(["--vm-zone", deploy_request.vm_zone])

        cli_command: str = " ".join(cli_parts)

        # Start background deployment if function provided
        if background_task_func:
            background_task_func(deploy_request, config_dir, shard_list, cli_command, deployment_id)

        log_event(
            "deployment.creation.completed",
            details={
                "deployment_id": deployment_id,
                "service": deploy_request.service,
                "total_shards": len(shards),
                "max_concurrent": deploy_request.max_concurrent or self.default_max_concurrent,
                "background_execution": background_task_func is not None,
            },
        )

        return {
            "deployment_id": deployment_id,
            "service": deploy_request.service,
            "region": deploy_request.region or self.default_region,
            "compute": deploy_request.compute,
            "total_shards": len(shards),
            "max_concurrent": deploy_request.max_concurrent or self.default_max_concurrent,
            "cli_command": cli_command,
            "shard_list": shard_list,
            "status": "pending",
        }

    def run_deployment_background(
        self,
        deploy_request: DeployRequest,
        config_dir: str,
        shard_list: list[dict[str, object]],
        cli_command: str,
        deployment_id: str,
    ) -> None:
        """Execute deployment in the background."""
        try:
            # Resolve effective dates
            _eff_start, _eff_end = _resolve_deploy_dates(deploy_request, config_dir)

            # Use region from request or default
            deployment_region: str = deploy_request.region or self.default_region

            if deployment_region != _settings.GCS_REGION and _settings.WARN_CROSS_REGION_EGRESS:
                log_event(
                    "deployment.cross_region_warning",
                    details={
                        "deployment_region": deployment_region,
                        "gcs_region": _settings.GCS_REGION,
                        "deployment_id": deployment_id,
                        "service": deploy_request.service,
                        "cost_impact": "egress_charges",
                    },
                )

            loader: ConfigLoader = ConfigLoader(config_dir)
            service_config: dict[str, object] = cast(dict[str, object], loader.load_service_config(deploy_request.service))
            compute_config: dict[str, object] = cast(dict[str, object], loader.get_compute_recommendation(
                deploy_request.service, deploy_request.compute
            ))

            raw_docker = service_config.get("docker_image")
            docker_image: str = (
                _substitute_env_vars(str(raw_docker))
                if raw_docker
                else f"{deployment_region}-docker.pkg.dev/{self.default_project_id}/{deploy_request.service}/{deploy_request.service}:latest"
            )
            job_name: str = cast(str, service_config.get("cloud_run_job_name", deploy_request.service))

            # Build orchestrator shards
            orchestrator_shards: list[dict[str, object]] = [
                {
                    "shard_id": s["shard_id"],
                    "dimensions": s["dimensions"],
                    "args": s["cli_args"],
                }
                for s in shard_list
            ]

            # Create orchestrator
            orchestrator: DeploymentOrchestrator = DeploymentOrchestrator(
                project_id=self.default_project_id,
                region=deployment_region,
                compute_type=deploy_request.compute,
                deployment_mode=deploy_request.mode,
                deployment_id=deployment_id,
                service=deploy_request.service,
                docker_image=docker_image,
                job_name=job_name,
                compute_config=compute_config,
                env_vars=_build_deploy_env_vars(
                    deployment_region=deployment_region,
                    compute_type=deploy_request.compute,
                    compute_config=compute_config,
                ),
                max_concurrent=deploy_request.max_concurrent or self.default_max_concurrent,
                vm_zone=deploy_request.vm_zone,
            )

            # Run deployment
            orchestrator.deploy(orchestrator_shards, tag=deploy_request.tag)
            log_event(
                "deployment.completed",
                details={
                    "deployment_id": deployment_id,
                    "service": deploy_request.service,
                    "region": deployment_region,
                    "compute_type": deploy_request.compute,
                    "total_shards": len(orchestrator_shards),
                    "tag": deploy_request.tag,
                },
            )

        except ValueError as e:
            log_event(
                "deployment.failed",
                severity="ERROR",
                details={
                    "deployment_id": deployment_id,
                    "service": deploy_request.service,
                    "error_type": "validation_error",
                    "error_message": str(e),
                    "error_category": "validation",
                },
            )
        except KeyError as e:
            log_event(
                "deployment.failed",
                severity="ERROR",
                details={
                    "deployment_id": deployment_id,
                    "service": deploy_request.service,
                    "error_type": "configuration_error",
                    "error_message": str(e),
                    "error_category": "configuration",
                },
            )
        except ConnectionError as e:
            log_event(
                "deployment.failed",
                severity="ERROR",
                details={
                    "deployment_id": deployment_id,
                    "service": deploy_request.service,
                    "error_type": "connection_error",
                    "error_message": str(e),
                    "error_category": "network",
                },
            )
        except OSError as e:
            log_event(
                "deployment.failed",
                severity="ERROR",
                details={
                    "deployment_id": deployment_id,
                    "service": deploy_request.service,
                    "error_type": "file_system_error",
                    "error_message": str(e),
                    "error_category": "file_system",
                },
            )
        except RuntimeError as e:
            log_event(
                "deployment.failed",
                severity="ERROR",
                details={
                    "deployment_id": deployment_id,
                    "service": deploy_request.service,
                    "error_type": "unexpected_error",
                    "error_message": str(e),
                    "error_category": "unexpected",
                },
            )

    def get_deployment_report(self, deployment_id: str) -> dict[str, object]:
        """
        Generate a detailed deployment report.

        Args:
            deployment_id: Deployment ID to generate report for

        Returns:
            Dict containing deployment report
        """
        # Load state for deployment_id and generate report
        from deployment_api.services.deployment_state import DeploymentStateService

        state_service = DeploymentStateService()
        state = state_service.get_deployment_state(deployment_id)
        if not state:
            return {"error": f"Deployment {deployment_id} not found"}
        return cast(dict[str, object], generate_deployment_report(state, None, None))
