"""Deploy-Missing surgical-recovery preview helper.

Plan: ``data_status_drilldown_shard_atom_alignment_2026_05_07.plan.md`` Phase 3.

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
from dataclasses import dataclass
from typing import cast

logger = logging.getLogger(__name__)


# Service slug -> launch-script name in
# ``deployment-service/scripts/vm/``. Operators copy + run the produced
# command from a workstation that has gcloud + the workspace cloned.
_SERVICE_LAUNCHER_SCRIPTS: dict[str, str] = {
    "market-tick-data-service": "deployment-service/scripts/vm/launch-mtds-backfill-vm.sh",
    "market-data-processing-service": "deployment-service/scripts/vm/launch-mdps-backfill-vm.sh",
    "instruments-service": "deployment-service/scripts/vm/launch-instruments-backfill-vm.sh",
    "features-onchain-service": "deployment-service/scripts/vm/launch-features-onchain-backfill-vm.sh",
    "features-delta-one-service": "deployment-service/scripts/vm/launch-features-backfill-vm.sh",
    "features-volatility-service": "deployment-service/scripts/vm/launch-features-backfill-vm.sh",
    "features-cross-instrument-service": "deployment-service/scripts/vm/launch-features-backfill-vm.sh",
    "features-sports-service": "deployment-service/scripts/vm/launch-features-backfill-vm.sh",
    "features-calendar-service": "deployment-service/scripts/vm/launch-features-backfill-vm.sh",
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
    CLI accepts; ``command`` is the full bash invocation including the
    launcher script + ``--shard-key`` flag the operator should run.
    """

    service: str
    asset_group: str
    row_key: dict[str, str]
    shard_key: str
    launcher_script: str
    command: str
    notes: list[str]

    def to_dict(self) -> dict[str, object]:
        return {
            "service": self.service,
            "asset_group": self.asset_group,
            "row_key": dict(self.row_key),
            "shard_key": self.shard_key,
            "launcher_script": self.launcher_script,
            "command": self.command,
            "notes": list(self.notes),
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

    Raises:
        DeployMissingError: if the service has no registered launcher
            script or the row_key lacks fields required to build a
            valid shard key.
    """
    launcher = _SERVICE_LAUNCHER_SCRIPTS.get(service)
    if launcher is None:
        raise DeployMissingError(
            f"No launcher script registered for service {service!r}. "
            f"Add to _SERVICE_LAUNCHER_SCRIPTS in "
            f"deployment_api/services/deploy_missing.py."
        )

    if not row_key.get("venue") or not row_key.get("data_type"):
        raise DeployMissingError(
            "row_key must include at least 'venue' and 'data_type'; "
            f"got {row_key!r}"
        )

    day = row_key.get("day") or row_key.get("date")
    if not day:
        raise DeployMissingError("row_key must include 'day' (or 'date') for surgical recovery")

    shard_key = _build_shard_key(asset_group, row_key)

    # Build the full bash invocation the operator copies + runs. Quote
    # the shard-key value because pipe is a shell metacharacter; use
    # shlex.quote for paranoia-grade escaping.
    quoted_shard_key = shlex.quote(shard_key)
    command = f"bash {launcher} --shard-key={quoted_shard_key}"

    notes: list[str] = []
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
    notes.append(
        "Tarball refresh: if you changed code on this branch since the last "
        "VM launch, run "
        "``bash deployment-service/scripts/vm/create-code-tarballs.sh --all`` "
        "first so the new VM picks up your edits."
    )
    notes.append(
        "Per-VM shard isolation: the launcher script sets "
        "``MANIFEST_PER_VM_SHARDS=true`` + ``VM_NAME=<unique-tag>`` "
        "automatically -- safe to run multiple deploy-missing VMs "
        "concurrently."
    )

    logger.info(
        "deploy-missing preview built service=%s asset_group=%s shard_key=%s",
        service,
        asset_group,
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
