"""Helpers for the trading-venue axis (``asset_group`` canonical only).

v9 post-migration: ``asset_group`` is the ONLY accepted key.  The legacy
``category`` fallback is intentionally removed — a manifest row that lacks
``asset_group`` must surface as *unknown/unmigrated*, not be silently
bucketed via a legacy key (which would confuse counts and misrepresent the
migration state).  SSOT: workspace CLAUDE.md § "Asset-group vocabulary".
"""

from __future__ import annotations

import re
from typing import cast


def _norm_ag(val: str) -> str:
    return val.strip().upper()


def trading_axis_value_from_shard_dimensions(dims: object) -> str | None:
    """Return CEFI/DEFI/… from a shard ``dimensions`` mapping.

    Reads ``asset_group`` only (v9 canonical).  Returns ``None`` when the key
    is absent so callers can surface the shard as unmigrated rather than
    silently falling back to the legacy ``category`` key.
    """
    if not isinstance(dims, dict):
        return None
    d = cast(dict[str, object], dims)
    ag = d.get("asset_group")
    if isinstance(ag, str) and ag.strip():
        return _norm_ag(ag)
    # No asset_group → unmigrated / unknown shard; do NOT fall back to category.
    return None


def _scan_cli_for_axis(cli_text: str) -> str | None:
    """Parse ``--asset-group`` from a CLI string.

    Also accepts the legacy ``--category`` flag (present in old VM launch
    strings persisted in state.json) — CLI parsing reads existing strings, it
    does not write new ones, so this is safe to keep as a read-path for
    old state records during the migration window.
    """
    for pat in (r"--asset-group[= ](\S+)", r"--category[= ](\S+)"):
        m = re.search(pat, cli_text, flags=re.IGNORECASE)
        if m:
            return _norm_ag(m.group(1))
    return None


def _scan_mapping_for_axis(d: dict[str, object]) -> str | None:
    """Return CEFI/DEFI/… from ``asset_group`` key on ``d`` (v9 canonical only)."""
    v = d.get("asset_group")
    if isinstance(v, str) and v.strip():
        return _norm_ag(v)
    if isinstance(v, list) and v and isinstance(v[0], str) and v[0].strip():
        return _norm_ag(v[0])
    return None


def _scan_shards_for_axis(raw_shards: object) -> str | None:
    if not isinstance(raw_shards, list) or not raw_shards:
        return None
    s0 = raw_shards[0]  # pyright: ignore[reportUnknownVariableType]
    if not isinstance(s0, dict):
        return None
    dims = cast(dict[str, object], s0).get("dimensions")
    return trading_axis_value_from_shard_dimensions(dims)


def _scan_cli_args_for_axis(cli_args: object) -> str | None:
    if isinstance(cli_args, str) and cli_args.strip():
        return _scan_cli_for_axis(cli_args)
    if isinstance(cli_args, list) and cli_args:
        try:
            joined = " ".join(str(x) for x in cli_args)  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
        except (TypeError, ValueError, RuntimeError):
            return None
        if joined.strip():
            return _scan_cli_for_axis(joined)
    return None


def trading_axis_from_deployment_state(data: dict[str, object]) -> str | None:
    """Best-effort asset group (CEFI, DEFI, …) from persisted ``state.json``-shaped data."""
    if (ag := _scan_mapping_for_axis(data)) is not None:
        return ag

    cfg = data.get("config")
    if isinstance(cfg, dict) and (ag := _scan_mapping_for_axis(cast(dict[str, object], cfg))) is not None:
        return ag

    if (ag := _scan_shards_for_axis(data.get("shards"))) is not None:
        return ag

    cli_cmd = data.get("cli_command")
    if isinstance(cli_cmd, str) and cli_cmd.strip() and (ag := _scan_cli_for_axis(cli_cmd)) is not None:
        return ag

    return _scan_cli_args_for_axis(data.get("cli_args"))
