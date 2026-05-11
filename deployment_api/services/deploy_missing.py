"""Deploy-Missing surgical-recovery preview helper.

Plan: ``data_status_drilldown_shard_atom_alignment_2026_05_07.plan`` Phase 3.

The deployment-ui Data Status panel's Deploy-Missing button on a single
leaf shard needs to fire a backfill VM scoped to ONE shard via the
``--shard-key=...`` MTDS CLI flag (Phase 4). Two ways to wire this:

1. **Auto-launch** — the API directly invokes
   ``gcloud compute instances create ...`` against the operator's GCP
   project. Requires the deployment-api service account to have
   ``roles/compute.instanceAdmin.v1`` + the right network / subnet /
   image perms, plus a complete tarball-refresh step
   (``deployment-service/scripts/vm/create-code-tarballs.sh``) before
   the new VM picks up the latest code. Out of scope for Phase 3 ship
   without an operations review of the security boundary.
2. **Preview + copy** (this module) — the API takes the leaf
   ``row_key`` from the drill-down, builds the surgical CLI invocation
   the operator should run, and returns it as a structured response.
   The UI renders it in a copy-to-clipboard widget. The operator runs
   the command from their authenticated terminal -- same security
   boundary as today's manual backfills, no new perms needed.

Phase 3 ships option 2; option 1 is a follow-up plan after the security
review.
"""

from __future__ import annotations

import logging
import shlex
from dataclasses import dataclass, field
from typing import cast

logger = logging.getLogger(__name__)


# Supported launch modes for the Deploy-Missing flow.
# * ``preview`` — return the surgical bash invocation the operator copies
#   + runs from their authenticated terminal. The VM boots from the
#   tarball that's currently in
#   ``gs://deployment-scripts-${PID}/code/`` (refreshed via
#   ``deployment-service/scripts/vm/create-code-tarballs.sh`` by the
#   operator). Same security boundary as today's manual backfills; safe
#   to run from staging / prod.
# * ``tarball-from-local`` — pair the launcher invocation with a
#   ``create-code-tarballs.sh --all`` step that copies the OPERATOR'S
#   LOCAL working tree into the GCS tarball bucket BEFORE the VM
#   launches. Lets a developer test a code change end-to-end without
#   pushing + waiting for CI. **Only works from the operator's local
#   workstation** (the script reads the local repo paths via the
#   workspace manifest); never works from the deployment-api Cloud Run
#   container. The UI must surface a strong warning so operators don't
#   confuse this with the prod path.
SUPPORTED_LAUNCH_MODES: frozenset[str] = frozenset({"preview", "tarball-from-local"})


# Service slug -> launch-script name in
# ``deployment-service/scripts/vm/``. Operators copy + run the produced
# command from a workstation that has gcloud + the workspace cloned.
_VM_SCRIPT_DIR = "deployment-service/scripts/vm"
_SERVICE_LAUNCHER_SCRIPTS: dict[str, str] = {
    "market-tick-data-service": f"{_VM_SCRIPT_DIR}/launch-mtds-backfill-vm.sh",
    "market-data-processing-service": f"{_VM_SCRIPT_DIR}/launch-mdps-backfill-vm.sh",
    "instruments-service": f"{_VM_SCRIPT_DIR}/launch-instruments-backfill-vm.sh",
    # Consolidated features-service (Phase 8A of
    # `unified-trading-pm/plans/active/features_repo_consolidation_2026_05_08.md`).
    # The single `launch-features-vm.sh --feature-family <name>` entry-point
    # supersedes the 8 per-family launchers. Each `features-<family>-service`
    # legacy slug points at the same launcher; callers pass
    # `--feature-family <family>` to dispatch.
    "features-service": f"{_VM_SCRIPT_DIR}/launch-features-vm.sh",
    "features-onchain-service": f"{_VM_SCRIPT_DIR}/launch-features-vm.sh",
    "features-delta-one-service": f"{_VM_SCRIPT_DIR}/launch-features-vm.sh",
    "features-volatility-service": f"{_VM_SCRIPT_DIR}/launch-features-vm.sh",
    "features-cross-instrument-service": f"{_VM_SCRIPT_DIR}/launch-features-vm.sh",
    "features-sports-service": f"{_VM_SCRIPT_DIR}/launch-features-vm.sh",
    "features-calendar-service": f"{_VM_SCRIPT_DIR}/launch-features-vm.sh",
    "features-commodity-service": f"{_VM_SCRIPT_DIR}/launch-features-vm.sh",
    "features-multi-timeframe-service": f"{_VM_SCRIPT_DIR}/launch-features-vm.sh",
}


# Live-cluster launcher registry (Phase 11.2 of
# `unified-trading-pm/plans/active/live_pipeline_mtds_mdps_features_2026_05_08.md`).
# These are NOT keyed by service slug like ``_SERVICE_LAUNCHER_SCRIPTS`` above
# (which serves per-shard Deploy-Missing); they're keyed by **live-cluster role**
# because the live pipeline deploys as a fixed set of VM types per
# ``unified-trading-pm/codex/05-infrastructure/live-pipeline-architecture.md``
# § "VM topology + launchers":
#   - 5 per-asset_group MTDS producers (one per cefi/defi/tradfi/sports/prediction)
#   - 5 per-asset_group MDPS+features consumers
#   - 1 singleton features-cross-cutting consumer
#   - 1 singleton replay-cascade producer (one window at a time)
#
# The deployment-UI consumes this registry for the per-asset_group "Deploy live
# cluster" action (Phase 11.4 — UI button). Operators pick (a) cluster role
# from the closed set + (b) asset_group (where applicable) + (c) ``--env``;
# UI renders the resulting launcher command for tarball-from-local or
# preview-only execution.
#
# Codex SSOT for the topology:
# ``unified-trading-pm/codex/05-infrastructure/runtime-tiers-and-deployment.md``
# § "Live-pipeline VM topology (2026-05-08 cutover)".
_LIVE_CLUSTER_LAUNCHER_SCRIPTS: dict[str, str] = {
    # Per-asset_group launchers — caller appends ``--asset-group <ag> --env <env>``.
    "mtds-live": f"{_VM_SCRIPT_DIR}/launch-mtds-live.sh",
    "mdps-features-live": f"{_VM_SCRIPT_DIR}/launch-mdps-features-live.sh",
    # Singletons — caller appends ``--env <env>`` only; no asset_group axis.
    "features-cross-cutting": f"{_VM_SCRIPT_DIR}/launch-features-cross-cutting.sh",
    # Replay-cascade also takes ``--start``/``--end``/``--shard-key`` per invocation.
    "replay-cascade": f"{_VM_SCRIPT_DIR}/launch-replay-cascade.sh",
}


# Required keys in the ``row_key`` payload depending on whether the leaf
# is bundled (per-root parquet) or per-instrument (one parquet per
# instrument). The 5th shard-key field varies by routing (--root vs
# --instrument-ids).
_BUNDLED_DATA_TYPES: frozenset[str] = frozenset(
    {
        "options_chain",
        "futures_chain",
    }
)


@dataclass
class DeployMissingPreview:
    """Structured response for the UI's Deploy-Missing copy-to-clipboard
    widget.

    ``shard_key`` is the canonical 6-field pipe-delimited form the MTDS
    CLI accepts; ``command`` is the full bash invocation the operator
    should run; ``mode`` records which launch path (preview / tarball-
    from-local) the command targets so the UI can render the right
    warning copy.
    """

    service: str
    asset_group: str
    row_key: dict[str, str]
    shard_key: str
    launcher_script: str
    command: str
    notes: list[str]
    mode: str = "preview"
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {
            "service": self.service,
            "asset_group": self.asset_group,
            "row_key": dict(self.row_key),
            "shard_key": self.shard_key,
            "launcher_script": self.launcher_script,
            "command": self.command,
            "notes": list(self.notes),
            "mode": self.mode,
            "warnings": list(self.warnings),
        }


class DeployMissingError(ValueError):
    """Raised when the deploy-missing preview cannot be assembled."""


def _build_shard_key(asset_group: str, row_key: dict[str, str]) -> str:
    """Compose the canonical 6-field pipe-delimited shard key.

    Empty fields are preserved as empty strings; the MTDS-side decomposer
    (``market_tick_data_service.cli.shard_key.decompose_shard_key``)
    skips empties so default behaviour applies.
    """
    venue = row_key.get("venue", "")
    data_type = row_key.get("data_type", "")
    instrument_type = row_key.get("instrument_type", "")
    # Bundled shards put their root in either ``instrument_id`` (less
    # common) or a dedicated ``root`` column in the manifest. Read both;
    # ``root`` wins if both populated.
    inst_or_root = row_key.get("root", "") or row_key.get("instrument_id", "")
    day = row_key.get("day", "") or row_key.get("date", "")
    return "|".join([asset_group, venue, data_type, instrument_type, inst_or_root, day])


def build_deploy_missing_preview(
    *,
    service: str,
    asset_group: str,
    row_key: dict[str, str],
    mode: str = "preview",
) -> DeployMissingPreview:
    """Assemble the surgical re-run command for ONE leaf shard.

    Args:
        service: Target service slug (e.g. ``"market-tick-data-service"``).
        asset_group: Lowercase asset_group (cefi / defi / tradfi / sports / prediction).
        row_key: Leaf node's structured shard atom, as emitted by the
            hierarchical drill-down endpoint. Must include enough fields
            to populate the canonical 6-field pipe form -- typically
            ``venue``, ``data_type``, ``instrument_type`` (optional),
            ``instrument_id`` or ``root``, and ``day`` or ``date``.
        mode: ``"preview"`` (default) returns the launcher invocation
            against whatever tarball is currently in
            ``gs://deployment-scripts-${PID}/code/``;
            ``"tarball-from-local"`` prepends a ``create-code-tarballs.sh
            --all`` call so the VM boots the OPERATOR'S LOCAL working
            tree (only works from the operator's workstation; useless
            from the deployment-api Cloud Run pod). Caller surfaces the
            mode-appropriate warning to the operator.

    Raises:
        DeployMissingError: if the service has no registered launcher
            script, the row_key lacks required fields, or ``mode`` is
            not in ``SUPPORTED_LAUNCH_MODES``.
    """
    if mode not in SUPPORTED_LAUNCH_MODES:
        raise DeployMissingError(
            f"Unsupported deploy-missing mode {mode!r}. Supported: {sorted(SUPPORTED_LAUNCH_MODES)}"
        )

    launcher = _SERVICE_LAUNCHER_SCRIPTS.get(service)
    if launcher is None:
        raise DeployMissingError(
            f"No launcher script registered for service {service!r}. "
            f"Add to _SERVICE_LAUNCHER_SCRIPTS in "
            f"deployment_api/services/deploy_missing.py."
        )

    if not row_key.get("venue") or not row_key.get("data_type"):
        raise DeployMissingError(
            f"row_key must include at least 'venue' and 'data_type'; got {row_key!r}"
        )

    day = row_key.get("day") or row_key.get("date")
    if not day:
        raise DeployMissingError("row_key must include 'day' (or 'date') for surgical recovery")

    shard_key = _build_shard_key(asset_group, row_key)

    # Build the bash invocation. Quote the shard-key value because pipe
    # is a shell metacharacter; use shlex.quote for paranoia-grade
    # escaping. In tarball-from-local mode, prepend a refresh step so
    # the VM boots the operator's current local working tree rather
    # than the latest CI-pushed tarball.
    quoted_shard_key = shlex.quote(shard_key)
    launcher_invocation = f"bash {launcher} --shard-key={quoted_shard_key}"

    notes: list[str] = []
    warnings_out: list[str] = []
    data_type = row_key.get("data_type", "").lower()
    if data_type in _BUNDLED_DATA_TYPES:
        notes.append(
            "Bundled data_type: the 5th field is the cluster ROOT (e.g. ES.OPT). "
            "MTDS will re-run the entire bundle for that root + day."
        )
    else:
        notes.append(
            "Per-instrument data_type: the 5th field is the specific instrument_id. "
            "MTDS will re-run only this instrument's parquet for that day."
        )

    if mode == "tarball-from-local":
        # Tarball-from-local: the operator's local working tree gets
        # bundled + uploaded to GCS BEFORE the VM launches, so the VM
        # boots whatever code is checked out on the workstation
        # (uncommitted changes, branch toggles, hot-fixes). The combo
        # command chains the refresh + the launcher with ``&&`` so a
        # tarball-build failure aborts before launching the VM.
        command = (
            f"bash deployment-service/scripts/vm/create-code-tarballs.sh --all "
            f"&& {launcher_invocation}"
        )
        warnings_out.append(
            "LOCAL-ONLY: this mode copies code from your LOCAL working tree "
            "into the GCS tarball bucket. It only works when run from your "
            "own workstation -- it does NOT work from the deployment-api "
            "Cloud Run pod, CI runners, or any other server. Do not paste "
            "this command into a shared / remote shell."
        )
        warnings_out.append(
            "UNCOMMITTED CHANGES: the tarball captures whatever's on disk, "
            "including uncommitted edits. The launched VM will run that "
            "tree. Commit + push your changes first if you want the run "
            "to be reproducible."
        )
        notes.append(
            "Tarball refresh is paired with the launcher via ``&&`` so a "
            "tarball-build failure aborts before any VM launches."
        )
    else:
        # Preview mode: the VM boots from whatever tarball is currently
        # in GCS. Operators must remember to refresh manually if they
        # changed code recently.
        command = launcher_invocation
        notes.append(
            "Tarball refresh: if you changed code on this branch since the last "
            "VM launch, run "
            "``bash deployment-service/scripts/vm/create-code-tarballs.sh --all`` "
            "first so the new VM picks up your edits. Or pick the "
            "``tarball-from-local`` mode in the UI to chain that step automatically."
        )

    notes.append(
        "Per-VM shard isolation: the launcher script sets "
        "``MANIFEST_PER_VM_SHARDS=true`` + ``VM_NAME=<unique-tag>`` "
        "automatically -- safe to run multiple deploy-missing VMs "
        "concurrently."
    )

    logger.info(
        "deploy-missing preview built service=%s asset_group=%s mode=%s shard_key=%s",
        service,
        asset_group,
        mode,
        shard_key,
    )
    return DeployMissingPreview(
        service=service,
        asset_group=asset_group,
        row_key=dict(row_key),
        shard_key=shard_key,
        launcher_script=launcher,
        command=command,
        notes=notes,
        mode=mode,
        warnings=warnings_out,
    )


def list_supported_services() -> list[str]:
    """Enumerate services with a registered launcher script.

    UI consumers call this to decide which leaves render the
    Deploy-Missing button vs. a "manual recovery" placeholder.
    """
    return sorted(_SERVICE_LAUNCHER_SCRIPTS.keys())


__all__ = [
    "DeployMissingError",
    "DeployMissingPreview",
    "build_deploy_missing_preview",
    "list_supported_services",
]


_ = cast  # reserved for future strict typing of the row_key payload.
