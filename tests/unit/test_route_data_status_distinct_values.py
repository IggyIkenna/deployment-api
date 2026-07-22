"""Tests for routes/data_status/_distinct_values.py — the RAW distinct-values
enumeration (``GET /distinct-values/{asset_group}``), the SSOT-alignment /
canonical-drift panel restored 2026-07-18 (operator ask).

The endpoint reads the nightly honest-coverage ``coverage.json`` rollup and, per
asset_group, enumerates the DISTINCT ``venues`` / ``instrument_types`` /
``data_types`` / ``chains`` present (from the ``by_venue*`` / ``by_chain`` map
keys), badging each ``is_canonical`` against the UAC canonical sets. Values are
NOT collapsed — case/plural drift MUST survive (that is the whole point).

Mirrors ``test_route_data_status_axis_census.py``'s TestClient + patch pattern.
The GCS reader (``_read_honest_coverage_rollup``) is patched at the submodule path
where it is defined + called, so no real rollup blob is touched.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

_PATCH_DISABLE_AUTH = "deployment_api.rbac.DISABLE_AUTH"
_PATCH_CFG = "deployment_api.routes.data_status._cfg"
_PATCH_READ_ROLLUP = "deployment_api.routes.data_status._distinct_values._read_honest_coverage_rollup"


def _make_mock_cfg() -> MagicMock:
    cfg = MagicMock()
    cfg.is_mock_mode.return_value = False
    cfg.deployment_env = "dev"
    return cfg


def _defi_coverage_payload() -> dict[str, object]:
    """A honest-coverage ``coverage.json`` slice with deliberate canonical + drift
    values across every axis — mirrors the real defi rollup's shape (bare-protocol
    venues, mixed-case instrument_types, ``by_chain`` from the chain-enum add)."""
    return {
        "generated_at": "2026-07-18T02:00:00Z",
        "date": "2026-07-18",
        "schema_version": 2,
        # ag -> {venue: counts}
        "by_venue": {
            "defi": {
                # bare-protocol venue dupes (AAVE vs AAVE_V3) — the drift signal.
                "AAVE": {"captured": 5},
                "AAVE_V3": {"captured": 3},
                "UNISWAP_V3": {"captured": 9},
                "": {"captured": 1},  # blank sentinel — must be dropped
            }
        },
        # ag -> {venue: {instrument_type: counts}}
        "by_venue_instrument_type": {
            "defi": {
                "AAVE_V3": {"LENDING": {"captured": 3}, "lending": {"captured": 1}},
                "UNISWAP_V3": {"POOL": {"captured": 9}, "pool": {"captured": 2}},
                "KAMINO": {"SOLANA_LENDING": {"captured": 4}, "None": {"captured": 1}},
            }
        },
        # ag -> {venue: {data_type: counts}}
        "by_venue_data_type": {
            "defi": {
                "UNISWAP_V3": {"dex_pool_state": {"captured": 9}, "dex_pools": {"captured": 1}},
            }
        },
        # ag -> {chain: counts} (chain-enum add; keys uncollapsed)
        "by_chain": {
            "defi": {
                "ETHEREUM": {"captured": 12},
                "SOLANA": {"captured": 4},
                "ethereum": {"captured": 1},  # lower-case drift — must survive + flag
                "nan": {"captured": 1},  # blank sentinel — must be dropped
            }
        },
    }


@pytest.fixture
def client_distinct_values() -> TestClient:
    from deployment_api.routes.data_status import router

    app = FastAPI()
    app.include_router(router, prefix="/data-status")
    with (
        patch(_PATCH_DISABLE_AUTH, True),
        patch(_PATCH_CFG, _make_mock_cfg()),
    ):
        yield TestClient(app, raise_server_exceptions=False)  # type: ignore[misc]


class TestEnumerateDistinctValues:
    """The pure enumerator (no I/O) — badging + raw-preservation semantics."""

    def test_axes_badged_against_uac_sets(self) -> None:
        from deployment_api.routes.data_status import enumerate_distinct_values

        axes, non_canonical = enumerate_distinct_values(_defi_coverage_payload(), "defi")

        # instrument_types (defi): case-INSENSITIVE since the 2026-07-20 audit.
        # UPDATED: this previously asserted the lower-case forms were drift. That
        # encoded a stale assumption — the operator-locked DeFi naming SSOT
        # (codex/02-data/defi-canonical-naming-ssot.md, "instrument_type" row)
        # declares the DeFi canonical values in LOWER case (`pool`, `a_token`,
        # `lst`, `spot_asset`, `perpetual`, `lending`, `solana_lending`, …) while
        # the InstrumentType enum is UPPER, so an exact test false-flagged the
        # SSOT's own canonical spelling. Both cases now badge canonical for defi.
        # (cefi/tradfi keep the exact test — see TestGrainAwareCanonicalCompare.)
        it_map = {e["value"]: e["is_canonical"] for e in axes["instrument_types"]}
        assert it_map["LENDING"] is True
        assert it_map["POOL"] is True
        assert it_map["SOLANA_LENDING"] is True
        assert it_map["lending"] is True
        assert it_map["pool"] is True
        # Raw still NOT collapsed: LENDING and lending survive as distinct entries.
        assert "LENDING" in it_map and "lending" in it_map

        # chains: ETHEREUM/SOLANA canonical, lower-case ethereum drift.
        chain_map = {e["value"]: e["is_canonical"] for e in axes["chains"]}
        assert chain_map["ETHEREUM"] is True
        assert chain_map["SOLANA"] is True
        assert chain_map["ethereum"] is False

        # data_types: dex_pool_state canonical, dex_pools drift.
        dt_map = {e["value"]: e["is_canonical"] for e in axes["data_types"]}
        assert dt_map["dex_pool_state"] is True
        assert dt_map["dex_pools"] is False

        # non_canonical_count is the per-axis drift headline.
        assert non_canonical["instrument_types"] == 0  # defi itype compare is case-insensitive
        assert non_canonical["chains"] == 1  # ethereum
        assert non_canonical["data_types"] == 1  # dex_pools

    def test_blank_sentinels_dropped(self) -> None:
        from deployment_api.routes.data_status import enumerate_distinct_values

        axes, _ = enumerate_distinct_values(_defi_coverage_payload(), "defi")
        assert "" not in {e["value"] for e in axes["venues"]}
        assert "None" not in {e["value"] for e in axes["instrument_types"]}
        assert "nan" not in {e["value"] for e in axes["chains"]}

    def test_missing_section_yields_empty_axis(self) -> None:
        from deployment_api.routes.data_status import enumerate_distinct_values

        # A payload with no by_chain (rollup predating the chain-enum add).
        payload = {"by_venue": {"cefi": {"BYBIT": {"captured": 1}}}}
        axes, non_canonical = enumerate_distinct_values(payload, "cefi")
        assert axes["chains"] == []
        assert non_canonical["chains"] == 0
        # cefi BYBIT is a canonical UAC venue.
        assert axes["venues"] == [{"value": "BYBIT", "is_canonical": True}]


class TestDistinctValuesEndpoint:
    def test_endpoint_shape_and_badges(self, client_distinct_values: TestClient) -> None:
        with patch(_PATCH_READ_ROLLUP, return_value=(_defi_coverage_payload(), "2026-07-18")):
            resp = client_distinct_values.get("/data-status/distinct-values/defi")
        assert resp.status_code == 200
        body = resp.json()
        assert body["asset_group"] == "defi"
        assert body["source"] == "honest-coverage-rollup"
        assert body["source_date"] == "2026-07-18"
        assert body["generated_at"] == "2026-07-18T02:00:00Z"
        assert set(body["axes"].keys()) == {"venues", "instrument_types", "data_types", "chains"}
        # Every entry carries the {value, is_canonical} contract.
        for entries in body["axes"].values():
            for entry in entries:
                assert set(entry.keys()) == {"value", "is_canonical"}
        chain_values = {e["value"] for e in body["axes"]["chains"]}
        assert {"ETHEREUM", "SOLANA", "ethereum"} <= chain_values

    def test_asset_group_case_insensitive(self, client_distinct_values: TestClient) -> None:
        with patch(_PATCH_READ_ROLLUP, return_value=(_defi_coverage_payload(), "2026-07-18")):
            resp = client_distinct_values.get("/data-status/distinct-values/DEFI")
        assert resp.status_code == 200
        assert resp.json()["asset_group"] == "defi"

    def test_503_when_rollup_unavailable(self, client_distinct_values: TestClient) -> None:
        with patch(_PATCH_READ_ROLLUP, return_value=None):
            resp = client_distinct_values.get("/data-status/distinct-values/defi")
        assert resp.status_code == 503


def _payload(ag: str, venues: list[str], itypes: list[str]) -> dict[str, object]:
    """Minimal coverage.json slice carrying just the two axes under test."""
    return {
        "by_venue": {ag: {v: {} for v in venues}},
        "by_venue_instrument_type": {ag: {venues[0] if venues else "X": {t: {} for t in itypes}}},
    }


class TestGrainAwareCanonicalCompare:
    """Audit 2026-07-20 — the canonical set and the manifest column are keyed at
    deliberately DIFFERENT grains for two (axis, asset_group) pairs; comparing
    them raw reported false drift. See ``_comparison_set``."""

    def test_defi_bare_venue_bases_strips_chain_but_keeps_multi_hyphen_base(self) -> None:
        from deployment_api.routes.data_status._distinct_values import _defi_bare_venue_bases

        bases = _defi_bare_venue_bases(
            frozenset({"UNISWAP_V2-ETHEREUM", "AAVE_V3-BASE", "SOLANA-NATIVE-SOLANA", "VENUS-BSC"})
        )
        assert "UNISWAP_V2" in bases
        assert "AAVE_V3" in bases
        assert "VENUS" in bases
        # The base itself contains a hyphen — a naive split("-")[0] would yield
        # "SOLANA" and silently mis-canonicalise a real venue.
        assert "SOLANA-NATIVE" in bases
        assert "SOLANA" not in bases

    def test_defi_bare_venues_canonical_but_real_drift_still_flagged(self) -> None:
        from deployment_api.routes.data_status import enumerate_distinct_values

        payload = _payload("defi", ["UNISWAP_V2", "AAVE_V3", "VENUS", "AAVEV3", "YEARNV3"], [])
        axes, non_canonical = enumerate_distinct_values(payload, "defi")
        badge = {e["value"]: e["is_canonical"] for e in axes["venues"]}
        # bare canonical venues no longer false-flagged
        assert badge["UNISWAP_V2"] is True
        assert badge["AAVE_V3"] is True
        assert badge["VENUS"] is True
        # genuine concatenation/underscore drift (no registry entry under any phase) MUST survive
        assert badge["AAVEV3"] is False
        assert badge["YEARNV3"] is False
        assert non_canonical["venues"] == 2

    def test_defi_bare_pipeline_phase_venue_is_canonical_not_drift(self) -> None:
        """D1b (audit 2026-07-20 RESULT): the vocabulary check compares against
        ``ALL_DEFI_VENUES`` (registered, live+pipeline), NOT the phase=="live"
        capability subset. AAVE-ETHEREUM/COMPOUND-ETHEREUM/UNISWAP-ETHEREUM are
        genuinely registered (pipeline phase, no IS adapter yet) — badging them
        drift would be a false alarm confusing "not yet capturable" with "not a
        real name". ⚠️ CAVEAT: bare UNISWAP additionally has its OWN separate,
        already-tracked issue (Track1 P2, distinct_values_noncanonical_audit
        Progress Log — per-pool V2/V3/V4 version-derivation, 199,397 rows,
        BLOCKED-OPERATOR-DECISION) — this badge going canonical does NOT mean
        that issue is resolved; it only means the drift PANEL (a naming-vocabulary
        detector) is no longer the right place to surface an already-tracked,
        already-gated data-completeness question."""
        from deployment_api.routes.data_status import enumerate_distinct_values

        payload = _payload("defi", ["AAVE", "COMPOUND", "UNISWAP"], [])
        axes, non_canonical = enumerate_distinct_values(payload, "defi")
        badge = {e["value"]: e["is_canonical"] for e in axes["venues"]}
        assert badge["AAVE"] is True
        assert badge["COMPOUND"] is True
        assert badge["UNISWAP"] is True
        assert non_canonical["venues"] == 0

    def test_defi_instrument_types_case_insensitive(self) -> None:
        from deployment_api.routes.data_status import enumerate_distinct_values

        payload = _payload("defi", ["UNISWAP_V2"], ["lending", "a_token", "lst", "liquidation", "restaking"])
        axes, _ = enumerate_distinct_values(payload, "defi")
        badge = {e["value"]: e["is_canonical"] for e in axes["instrument_types"]}
        # defi manifest column is canonically lowercase (operator ruling)
        assert badge["lending"] is True
        assert badge["a_token"] is True
        assert badge["lst"] is True
        # `restaking` became CANONICAL on 2026-07-20: the audit surfaced that the
        # DeFi manifest emits instrument_type=restaking (ezETH/rsETH/pufETH) with
        # no enum member, and the operator approved adding `RESTAKING` to
        # InstrumentType (uac). Under the defi case-insensitive compare it now
        # matches — this assertion is the cross-repo canary for that addition.
        assert badge["restaking"] is True
        # `liquidation` is NOT an instrument_type at all (it is a data concept —
        # the manifest contradicts the same handler's disk write). Still a real
        # defect, must stay flagged.
        assert badge["liquidation"] is False

    def test_cefi_and_tradfi_instrument_types_are_not_case_folded(self) -> None:
        """Regression guard: cefi/tradfi canonical instrument_type is UPPERCASE and
        their lowercase spellings are REAL drift owned by the in-flight uppercase
        migrations — a global case-fold would hide them."""
        from deployment_api.routes.data_status import enumerate_distinct_values

        cefi_axes, _ = enumerate_distinct_values(_payload("cefi", ["OKX"], ["perpetual", "spot"]), "cefi")
        cefi_badge = {e["value"]: e["is_canonical"] for e in cefi_axes["instrument_types"]}
        assert cefi_badge["perpetual"] is False
        assert cefi_badge["spot"] is False

        tradfi_axes, _ = enumerate_distinct_values(
            _payload("tradfi", ["CME"], ["future", "futures", "equity"]), "tradfi"
        )
        tradfi_badge = {e["value"]: e["is_canonical"] for e in tradfi_axes["instrument_types"]}
        assert tradfi_badge["future"] is False
        assert tradfi_badge["futures"] is False
        assert tradfi_badge["equity"] is False

    def test_sports_odds_api_bookmakers_are_accepted_exceptions_not_findings(self) -> None:
        """Operator ruling 2026-07-22 (distinct_values_noncanonical_audit_2026_07_20.md
        § "Operator decisions — RULED 2026-07-22"): "do NOT add them, in fact
        remove them everywhere so they don't come up in audit". The 20 ODDS_API
        fan-out bookmakers must be ABSENT from the sports venues axis entirely
        (not merely badged non-canonical) and must NOT contribute to
        non_canonical_count — while a genuinely unclassified sports venue still
        gets flagged, proving the exclusion is scoped, not a blanket sports pass.
        """
        from unified_api_contracts.registry import (
            SPORTS_ODDS_API_ACCEPTED_NONCANONICAL_BOOKMAKERS,
            VENUES_BY_ASSET_GROUP,
        )

        from deployment_api.routes.data_status import enumerate_distinct_values

        # Not canonical (operator explicitly rejected adding them back).
        assert SPORTS_ODDS_API_ACCEPTED_NONCANONICAL_BOOKMAKERS.isdisjoint(VENUES_BY_ASSET_GROUP["sports"])

        bookmakers = sorted(SPORTS_ODDS_API_ACCEPTED_NONCANONICAL_BOOKMAKERS)
        payload = _payload("sports", [*bookmakers, "DRAFTKINGS", "SOME_GENUINE_DRIFT_VENUE"], [])
        axes, non_canonical = enumerate_distinct_values(payload, "sports")

        values = {e["value"] for e in axes["venues"]}
        # The 20 accepted-exception bookmakers never appear at all.
        assert values.isdisjoint(SPORTS_ODDS_API_ACCEPTED_NONCANONICAL_BOOKMAKERS)
        # A real canonical venue and a real drift venue both still show up.
        assert "DRAFTKINGS" in values
        assert "SOME_GENUINE_DRIFT_VENUE" in values
        badge = {e["value"]: e["is_canonical"] for e in axes["venues"]}
        assert badge["DRAFTKINGS"] is True
        assert badge["SOME_GENUINE_DRIFT_VENUE"] is False
        # non_canonical_count counts ONLY the genuine drift venue — the 20
        # accepted bookmakers contribute zero findings.
        assert non_canonical["venues"] == 1

    def test_tradfi_chain_snapshot_data_types_are_accepted_exceptions_not_findings(self) -> None:
        """Audit follow-up 2026-07-22 (distinct_values_noncanonical_audit_2026_07_20.md
        § "futures_chain tradfi remedy decision"): ``options_chain`` (242,210 real
        captured rows) and ``futures_chain`` (8 real captured rows) are operator-
        preserved chain-snapshot data_type values on tradfi — genuinely non-canonical
        (they are bundle-grain instrument_types, not data_types) but never going to be
        relabelled/purged. Both must be ABSENT from the tradfi data_types axis
        entirely, contributing zero findings, while a genuinely unclassified tradfi
        data_type still gets flagged (the exclusion is scoped, not a blanket pass).
        """
        from unified_api_contracts.registry import (
            DATA_TYPES_BY_ASSET_GROUP,
            TRADFI_CHAIN_SNAPSHOT_ACCEPTED_NONCANONICAL_DATA_TYPES,
        )

        from deployment_api.routes.data_status import enumerate_distinct_values

        # Not canonical (deliberately excluded from DATA_TYPES_BY_ASSET_GROUP).
        assert TRADFI_CHAIN_SNAPSHOT_ACCEPTED_NONCANONICAL_DATA_TYPES.isdisjoint(DATA_TYPES_BY_ASSET_GROUP["tradfi"])

        payload = {
            "by_venue_data_type": {
                "tradfi": {
                    "CME": {
                        "options_chain": {"captured": 242210},
                        "futures_chain": {"captured": 8},
                        "ohlcv_1m": {"captured": 100},
                        "SOME_GENUINE_DRIFT_DATA_TYPE": {"captured": 1},
                    }
                }
            }
        }
        axes, non_canonical = enumerate_distinct_values(payload, "tradfi")

        values = {e["value"] for e in axes["data_types"]}
        # The two accepted-exception chain-snapshot values never appear at all.
        assert values.isdisjoint(TRADFI_CHAIN_SNAPSHOT_ACCEPTED_NONCANONICAL_DATA_TYPES)
        # A real canonical data_type and a real drift data_type both still show up.
        assert "ohlcv_1m" in values
        assert "SOME_GENUINE_DRIFT_DATA_TYPE" in values
        badge = {e["value"]: e["is_canonical"] for e in axes["data_types"]}
        assert badge["ohlcv_1m"] is True
        assert badge["SOME_GENUINE_DRIFT_DATA_TYPE"] is False
        # non_canonical_count counts ONLY the genuine drift data_type — the two
        # accepted chain-snapshot values contribute zero findings.
        assert non_canonical["data_types"] == 1

    def test_cefi_venue_axis_keeps_exact_compare(self) -> None:
        """Only defi venues are bare-based; cefi's suffix IS part of the canonical
        venue identity, so the exact test must be preserved.

        `OKX-FUTURES` was promoted to canonical 2026-07-21 (real, actively-captured
        cefi venue — VENUES_BY_ASSET_GROUP['cefi'] in market_data_categories.py — it
        was previously missing from that list, a false-negative drift alarm, not a
        genuinely non-canonical venue). `OKX-MARGIN` is not a registered cefi venue
        under any name and stands in as the negative exact-compare case instead.
        """
        from deployment_api.routes.data_status import enumerate_distinct_values

        axes, _ = enumerate_distinct_values(_payload("cefi", ["BINANCE-SPOT", "OKX-FUTURES", "OKX-MARGIN"], []), "cefi")
        badge = {e["value"]: e["is_canonical"] for e in axes["venues"]}
        assert badge["BINANCE-SPOT"] is True
        assert badge["OKX-FUTURES"] is True
        assert badge["OKX-MARGIN"] is False
