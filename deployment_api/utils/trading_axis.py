"""Helpers for the trading-venue axis (``asset_group`` / legacy ``category``)."""

from __future__ import annotations

import re
from typing import cast


def _norm_ag(val: str) -> str:
    return val.strip().upper()


def trading_axis_value_from_shard_dimensions(dims: object) -> str | None:
    """Return CEFI/DEFI/… from a shard ``dimensions`` mapping."""
    if not isinstance(dims, dict):
        return None
    d = cast(dict[str, object], dims)
    ag = d.get("asset_group")
    if isinstance(ag, str) and ag.strip():
        return _norm_ag(ag)
    cat = d.get("category")
    if isinstance(cat, str) and cat.strip():
        return _norm_ag(cat)
    return None


def _scan_cli_for_axis(cli_text: str) -> str | None:
    """Parse ``--asset-group`` / ``--category`` from a CLI string."""
    for pat in (r"--asset-group[= ](\S+)", r"--category[= ](\S+)"):
        m = re.search(pat, cli_text, flags=re.IGNORECASE)
        if m:
            return _norm_ag(m.group(1))
    return None


def trading_axis_from_deployment_state(data: dict[str, object]) -> str | None:
    """Best-effort asset group (CEFI, DEFI, …) from persisted ``state.json``-shaped data."""
    for key in ("asset_group", "category"):
        v = data.get(key)
        if isinstance(v, str) and v.strip():
            return _norm_ag(v)
        if isinstance(v, list) and v and isinstance(v[0], str) and v[0].strip():
            return _norm_ag(v[0])

    cfg = data.get("config")
    if isinstance(cfg, dict):
        c = cast(dict[str, object], cfg)
        for key in ("asset_group", "category"):
            cv = c.get(key)
            if isinstance(cv, str) and cv.strip():
                return _norm_ag(cv)
            if isinstance(cv, list) and cv and isinstance(cv[0], str) and cv[0].strip():
                return _norm_ag(cv[0])

    raw_shards = data.get("shards")
    if isinstance(raw_shards, list) and raw_shards:
        s0 = raw_shards[0]
        if isinstance(s0, dict):
            dims = cast(dict[str, object], s0).get("dimensions")
            ag = trading_axis_value_from_shard_dimensions(dims)
            if ag:
                return ag

    cli_cmd = data.get("cli_command")
    if isinstance(cli_cmd, str) and cli_cmd.strip():
        from_cli = _scan_cli_for_axis(cli_cmd)
        if from_cli:
            return from_cli

    cli_args = data.get("cli_args")
    if isinstance(cli_args, str) and cli_args.strip():
        from_cli = _scan_cli_for_axis(cli_args)
        if from_cli:
            return from_cli
    if isinstance(cli_args, list) and cli_args:
        # Join argv-ish tokens and scan
        try:
            joined = " ".join(str(x) for x in cli_args)
        except (TypeError, ValueError, RuntimeError):
            joined = ""
        if joined.strip():
            from_cli = _scan_cli_for_axis(joined)
            if from_cli:
                return from_cli

    return None
