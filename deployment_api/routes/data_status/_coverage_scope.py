"""Coverage-scope toggle + config-version surface for the venue-year-coverage route.

Extracted from ``_live_coverage.py`` (the host module crossed the 900-line cap)
to keep the route module under the size gate. Pure helpers — no route
registration here. Plan: ``mvp_scope_catalogue_tagging_2026_06_08.md`` §4 +
§ "Config versioning".
"""

from __future__ import annotations

from typing import Literal

import pandas as pd
from pydantic import BaseModel
from unified_api_contracts import (
    is_mvp,
    mvp_scope_config_descriptor,
    prediction_markets_config_descriptor,
    sports_leagues_config_descriptor,
)

# Coverage-scope toggle (``mvp_scope_catalogue_tagging_2026_06_08.md`` §4). The
# denominator is computed over the same 4-state could-exist UNION machinery for
# every scope; ``scope`` is an EXTRA predicate over the SAME cell iteration:
#
# * ``could_exist`` (DEFAULT — preserves the pre-toggle behaviour) — every cell
#   the manifest carries (the 4-state could-exist denominator already in use).
# * ``all`` — the full universe; identical to ``could_exist`` at this endpoint
#   (the manifest IS the enumerated could-exist universe), kept as a distinct
#   value so the UI can label it + so a future "all-vs-enumerated" split has a
#   stable param name.
# * ``mvp`` — restrict to cells where UAC ``is_mvp(...)`` is True (MVP readiness
#   board; the numerator + denominator both shrink to in-scope cells).
CoverageScope = Literal["could_exist", "mvp", "all"]


class ConfigVersionTriple(BaseModel):  # CORRECT-LOCAL: data-status config-version surface; TS consumer only
    """One ``(config_version, config_content_hash)`` pair for a scope config.

    Surfaced ALONGSIDE the coverage payload so a coverage delta attributes to a
    SCOPE change (a config_version/hash moved → the rules changed) vs a DATA
    change (the triple is stable → only the underlying data moved). Sourced from
    the UAC ``*_config_descriptor()`` SSOTs (``mvp_scope`` / ``sports_leagues`` /
    ``prediction_markets``). Plan: ``mvp_scope_catalogue_tagging_2026_06_08.md``
    § "Config versioning".
    """

    version: int
    content_hash: str


def config_versions() -> dict[str, ConfigVersionTriple]:
    """The per-config ``(version, content_hash)`` triples from the UAC SSOTs.

    Each ``*_config_descriptor()`` returns a ``ConfigDescriptor`` carrying a
    monotonic ``config_version`` int + a content-addressed ``config_content_hash``
    string. The three configs version INDEPENDENTLY (the MVP scope, the sports
    leagues, and the prediction markets each change on their own cadence).
    """
    mvp = mvp_scope_config_descriptor()
    leagues = sports_leagues_config_descriptor()
    markets = prediction_markets_config_descriptor()
    return {
        "mvp_scope": ConfigVersionTriple(version=mvp.config_version, content_hash=mvp.config_content_hash),
        "sports_leagues": ConfigVersionTriple(version=leagues.config_version, content_hash=leagues.config_content_hash),
        "prediction_markets": ConfigVersionTriple(
            version=markets.config_version, content_hash=markets.config_content_hash
        ),
    }


def filter_to_mvp(df: pd.DataFrame, asset_group: str) -> pd.DataFrame:
    """Keep only rows whose ``(asset_group, venue, instrument_type, data_type)`` is MVP.

    The ``is_mvp`` predicate is an extra filter over the SAME cell iteration — it
    shrinks BOTH numerator + denominator to in-scope cells (the MVP-readiness
    board). Missing ``venue`` / ``instrument_type`` / ``data_type`` cells coerce
    to ``""`` (``is_mvp`` treats absent/impossible as non-MVP → dropped).
    """
    if df.empty:
        return df

    def _is_mvp_row(row: pd.Series[object]) -> bool:  # type: ignore[type-arg]
        return is_mvp(
            asset_group,
            str(row.get("venue", "") or ""),  # noqa: qg-empty-fallback — optional manifest column
            str(row.get("instrument_type", "") or ""),  # noqa: qg-empty-fallback — optional manifest column
            str(row.get("data_type", "") or ""),  # noqa: qg-empty-fallback — optional manifest column
        )

    mask = df.apply(_is_mvp_row, axis=1)
    if mask.empty:
        return df.iloc[0:0]
    return df[mask]
