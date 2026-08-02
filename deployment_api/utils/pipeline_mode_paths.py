"""Canonical ``pipeline_mode={mode}_{source}/`` GCS path helpers (data-status reads).

SINGLE SSOT for the deployment-api data-status / drilldown readers' canonical raw-tick
path shape. Post the v9 manifest + GCS object migration (2026-06-18) every raw-tick
parquet lives under the canonical
``raw_tick_data/by_date/day={D}/pipeline_mode={mode}_{source}/asset_group={ag}/...``
shape — the ``pipeline_mode={mode}_{source}/`` key sits LEFT of ``asset_group=``. The
legacy duplicate twins (bare ``asset_group=`` with no pipeline_mode, legacy ``category=``,
top-level ``day=``) still physically exist on GCS (delete is operator-gated) — so a
reader that walks the bare/legacy shape AS WELL double-counts. Readers therefore probe
ONLY these canonical pipeline_mode layers (no legacy fallback).

The pipeline_mode tokens are DERIVED from the UAC source SSOT
(``external_batch_sources_for_asset_group`` + ``pipeline_mode_for_source``) — never
hand-listed, so a newly-registered batch source auto-appears for every reader at once.
"""

from __future__ import annotations


def canonical_pipeline_mode_segments(asset_group: str) -> list[str]:
    """Return the sorted canonical ``pipeline_mode={mode}_{source}/`` path segments.

    One segment per external batch source of ``asset_group`` that owns a
    ``Mode.BATCH`` pipeline_mode (a live-only / unregistered-batch source owns no
    batch GCS object and is skipped). Each returned value is a ready-to-concatenate
    path segment, e.g. ``"pipeline_mode=batch_chainlink/"``.
    """
    from unified_api_contracts import Mode, pipeline_mode_for_source  # noqa: imports-inside-functions
    from unified_api_contracts.registry import (  # noqa: imports-inside-functions
        external_batch_sources_for_asset_group,
    )

    pmodes: set[str] = set()
    for source in external_batch_sources_for_asset_group(asset_group.lower()):
        try:
            pmodes.add(pipeline_mode_for_source(source, Mode.BATCH).value)
        except ValueError:
            # Live-only / unregistered-batch source — owns no batch GCS object.
            continue
    return [f"pipeline_mode={pmode}/" for pmode in sorted(pmodes)]
