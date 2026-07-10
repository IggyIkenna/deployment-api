"""Unit tests for the data_status_drilldown UI-boundary ``data_type``
normaliser (``deployment_api.services.data_status_drilldown._schema``).

``_normalise_data_type`` / ``_DATA_TYPE_ALIASES`` had zero test references
despite being the entry point every ``get_schema_for_shard`` call passes
``data_type`` through before the UAC contract lookup — a silent typo in
``_DATA_TYPE_ALIASES`` or a regressed lowercasing fallback would make
DEFI pool-shard schema lookups quietly return ``registered: False``.
"""

from __future__ import annotations

import pytest

from deployment_api.services.data_status_drilldown._schema import (
    _DATA_TYPE_ALIASES,
    _normalise_data_type,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # Explicit alias table entries (UI-supplied uppercase legacy names).
        ("POOL_DEFINITION", "dex_pool_state"),
        ("INSTRUMENT_DEFINITION", "dex_pool_state"),
        ("POOL_SNAPSHOT", "dex_pool_state"),
        ("POOL_STATE", "dex_pool_state"),
        ("POOL_SWAPS", "dex_pool_swaps"),
        # Uppercase with no explicit alias -> lowercased.
        ("TRADES", "trades"),
        ("OHLCV_1M", "ohlcv_1m"),
        # Already-canonical snake_case -> passthrough unchanged.
        ("trades", "trades"),
        ("dex_pool_state", "dex_pool_state"),
    ],
)
def test_normalise_data_type_resolves_known_aliases(raw: str, expected: str) -> None:
    assert _normalise_data_type(raw) == expected


def test_alias_table_values_are_all_lowercase_snake_case() -> None:
    """Every alias target must already be canonical (lowercase) — an
    uppercase alias VALUE would silently defeat the point of the table."""
    for value in _DATA_TYPE_ALIASES.values():
        assert value == value.lower()
