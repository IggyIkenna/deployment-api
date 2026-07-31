"""Chain base: CLI wrapper + last-updated / completeness operations.

Split out of the 6,663-line ``data_status_service.py`` god-module
(codex ratchet plan 2026-06-10). The facade module re-exports every
public + legacy-underscore name, so callers keep importing from
``deployment_api.services.data_status_service``.
"""

import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import cast

from unified_api_contracts.internal import MarketCategory

import deployment_api.services.data_status_service as _dss
from deployment_api.services.data_status_drilldown import (
    SERVICE_TO_KIND,
)
from deployment_api.services.data_status_drilldown import (
    build_bucket_name as _drilldown_build_bucket_name,
)

logger = logging.getLogger(__name__)


def _resolve_cli_config_dir() -> str | None:
    """Resolve the ``deployment_service`` CLI ``--config-dir`` for the subprocess.

    The CLI is invoked as ``python -m deployment_service ...`` from an installed
    wheel, so its own ``get_config_dir()`` walk (relative to the package source /
    CWD) fails inside the Cloud Run image — ``configs/`` lives at the
    deployment-service REPO ROOT, is not package data, and ``pip install --no-deps``
    drops it (the source 500: "Could not find configs directory"). The deployment-api
    image, however, bundles the byte-identical mirror as ``pm-configs/`` (Dockerfile
    "COPY pm-configs/ ./pm-configs/"; SSOT ``unified-trading-pm/configs/``), so we
    point the CLI at it explicitly: ``<deployment_api repo/app root>/pm-configs``.
    Returns ``None`` when it doesn't exist, so ``_build_cli_cmd`` omits the flag and
    lets the CLI fall back to its own discovery (e.g. a dev run from deployment-service).
    """
    # cli.py == deployment_api/services/data_status/cli.py → parents[3] == repo/app root.
    pm_configs = Path(__file__).resolve().parents[3] / "pm-configs"
    if pm_configs.is_dir():
        return str(pm_configs)
    return None


class DataStatusCliMixin:
    """Chain base — CLI subprocess wrapper + cross-service status ops.

    The data_status mixins form a single linear inheritance chain
    (cli -> defi -> sports -> breakdowns_domain -> breakdowns_core ->
    venue_resolution -> coverage -> missing_shards -> manifest_category_builder ->
    manifest) so that
    every cross-group ``self._method`` reference resolves statically
    under basedpyright strict. ``DataStatusService`` composes the top of
    the chain and is the ONLY public entry point — import it from
    ``deployment_api.services.data_status_service`` (the facade).
    """

    project_id: str
    deployment_env_short: str

    def _build_cli_cmd(
        self,
        service: str,
        start_date: str,
        end_date: str,
        asset_groups: list[str] | None,
        venues: list[str] | None,
        show_missing: bool,
        check_venues: bool,
        check_data_types: bool,
        check_feature_groups: bool,
        check_timeframes: bool,
        mode: str,
    ) -> list[str]:
        """Build the data-status CLI command list."""
        cmd = [sys.executable, "-m", "deployment_service"]
        # ``--config-dir`` is a GROUP-level option (must precede the subcommand) —
        # points the in-image CLI at the bundled pm-configs mirror (see _resolve_cli_config_dir).
        _config_dir = _resolve_cli_config_dir()
        if _config_dir is not None:
            cmd.extend(["--config-dir", _config_dir])
        cmd += [
            "data-status",
            "-s",
            service,
            "--start-date",
            start_date,
            "--end-date",
            end_date,
            "--output",
            "json",
            "--mode",
            mode,
        ]
        self._append_cli_filter_flags(
            cmd,
            service,
            asset_groups,
            venues,
            show_missing,
            check_venues,
            check_data_types,
            check_feature_groups,
            check_timeframes,
        )
        return cmd

    @staticmethod
    def _append_cli_filter_flags(
        cmd: list[str],
        service: str,
        asset_groups: list[str] | None,
        venues: list[str] | None,
        show_missing: bool,
        check_venues: bool,
        check_data_types: bool,
        check_feature_groups: bool,
        check_timeframes: bool,
    ) -> None:
        """Append the ``-c``/``-v`` filters + boolean check-flags to ``cmd``, in place."""
        for ag in asset_groups or []:
            # The deployment-service CLI still accepts ``-c`` for the
            # asset_group filter (legacy short flag preserved during the
            # asset_group canonical-vocabulary rollout per CLAUDE.md SSOT).
            cmd.extend(["-c", ag])
        for venue in venues or []:
            cmd.extend(["-v", venue])
        if show_missing:
            cmd.append("--show-missing")
        if check_venues:
            cmd.append("--check-venues")
        elif check_feature_groups:
            cmd.append("--check-feature-groups")
        elif check_timeframes:
            cmd.append("--check-timeframes")
        elif service in ["market-tick-data-handler", "market-data-processing-service"]:
            cmd.append("--fast")
        if check_data_types:
            cmd.append("--check-data-types")

    async def run_data_status_cli(
        self,
        service: str,
        start_date: str,
        end_date: str,
        asset_groups: list[str] | None = None,
        venues: list[str] | None = None,
        show_missing: bool = False,
        check_venues: bool = False,
        check_data_types: bool = False,
        check_feature_groups: bool = False,
        check_timeframes: bool = False,
        mode: str = "batch",
    ) -> dict[str, object]:
        """Run data-status CLI command and return parsed JSON output."""
        cmd = self._build_cli_cmd(
            service,
            start_date,
            end_date,
            asset_groups,
            venues,
            show_missing,
            check_venues,
            check_data_types,
            check_feature_groups,
            check_timeframes,
            mode,
        )
        logger.info("Running CLI: %s", " ".join(cmd))
        return await self._execute_cli_subprocess(cmd)

    @staticmethod
    async def _execute_cli_subprocess(cmd: list[str]) -> dict[str, object]:
        """Run ``cmd`` as a subprocess and parse stdout as JSON; an error dict on any failure."""
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=None,
            )
            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                error_msg = f"CLI command failed with code {process.returncode}: {stderr.decode()}"
                logger.error(error_msg)
                return {"error": error_msg, "stderr": stderr.decode()}

            try:
                return cast(dict[str, object], json.loads(stdout.decode()))
            except json.JSONDecodeError as e:
                logger.error("Failed to parse CLI JSON output: %s", e)
                return {"error": f"Invalid JSON output: {e}", "raw_output": stdout.decode()}

        except (OSError, ValueError, RuntimeError) as e:
            logger.error("Error running CLI command: %s", e)
            return {"error": str(e)}

    async def get_last_updated_info(
        self,
        service: str,
        asset_groups: list[str] | None = None,
    ) -> dict[str, object]:
        """Get last updated information for a service (per asset_group bucket-activity check)."""
        if service not in SERVICE_TO_KIND and service != "features-commodity-service":
            return {"error": f"Unknown service: {service}"}

        # Default asset_groups if none specified
        if not asset_groups:
            asset_groups = [cat.value.lower() for cat in MarketCategory]

        asset_groups_info: dict[str, object] = {
            category: self._last_updated_for_category(service, category) for category in asset_groups
        }
        return {
            "service": service,
            "asset_groups": asset_groups_info,
            "overall_last_updated": None,
        }

    @staticmethod
    def _last_updated_for_category(service: str, category: str) -> dict[str, object]:
        """Bucket-activity proxy for one (service, asset_group)'s last-updated status."""
        try:
            bucket_name = _drilldown_build_bucket_name(service, category)
            # Use the most recent object in the bucket as proxy for recent activity.
            objects = _dss.list_objects(bucket_name, "", max_results=10)
            if objects:
                return {
                    "status": "active",
                    "object_count": len(objects),
                    "sample_paths": objects[:5],  # First 5 as examples
                }
            return {"status": "empty", "object_count": 0}
        except (OSError, ValueError, RuntimeError) as e:
            logger.debug("Error checking category %s: %s", category, e)
            return {"status": "error", "error": str(e)}

    async def validate_data_completeness(
        self,
        service: str,
        date: str,
        asset_groups: list[str] | None = None,
        venues: list[str] | None = None,
    ) -> dict[str, object]:
        """Validate data completeness for a specific date."""
        result = await self.run_data_status_cli(
            service=service,
            start_date=date,
            end_date=date,
            asset_groups=asset_groups,
            venues=venues,
            show_missing=True,
        )
        if "error" in result:
            return result
        return self._build_completeness_validation(service, date, result)

    @staticmethod
    def _build_completeness_validation(service: str, date: str, result: dict[str, object]) -> dict[str, object]:
        """Analyze a single-date ``run_data_status_cli`` result into a completeness summary."""
        missing_venues: list[str] = []
        validation_errors: list[object] = []
        is_complete = True
        completed_venues = 0

        venues_list = DataStatusCliMixin._extract_venues_list(result)
        total_venues = len(venues_list)
        for venue_info_raw in venues_list:
            if not isinstance(venue_info_raw, dict):
                continue
            venue_info = cast(dict[str, object], venue_info_raw)
            vname_raw: object = venue_info.get("venue", "unknown")
            venue_name = vname_raw if isinstance(vname_raw, str) else "unknown"
            status_raw: object = venue_info.get("status")
            status = status_raw if isinstance(status_raw, str) else ""

            if status == "missing":
                is_complete = False
                missing_venues.append(venue_name)
            elif status == "error":
                err_raw: object = venue_info.get("error", "Unknown error")
                validation_errors.append(
                    {"venue": venue_name, "error": err_raw if isinstance(err_raw, str) else "Unknown error"}
                )
            else:
                completed_venues += 1

        completion_rate = (completed_venues / total_venues * 100) if total_venues > 0 else 0.0
        return {
            "service": service,
            "date": date,
            "is_complete": is_complete,
            "total_venues": total_venues,
            "completed_venues": completed_venues,
            "missing_venues": missing_venues,
            "errors": validation_errors,
            "completion_rate": completion_rate,
        }

    @staticmethod
    def _extract_venues_list(result: dict[str, object]) -> list[object]:
        """The single date entry's ``venues`` list from a ``run_data_status_cli`` result, or ``[]``."""
        dates_val: object = result.get("dates")
        if not (dates_val and isinstance(dates_val, list)):
            return []
        dates_list = cast(list[object], dates_val)
        if not (dates_list and isinstance(dates_list[0], dict)):
            return []
        date_data = cast(dict[str, object], dates_list[0])  # Single date
        venues_val: object = date_data.get("venues")
        if venues_val and isinstance(venues_val, list):
            return cast(list[object], venues_val)
        return []
