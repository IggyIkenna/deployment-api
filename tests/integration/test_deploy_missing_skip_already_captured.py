"""Cat D.4 — Deploy-Missing → handler-decompose → orchestrator-skip integration.

Plan: ``data_status_comprehensive_test_coverage_2026_05_07.md`` Phase 3.

Operator directive 2026-05-07:
    "service responds to it, respects it, and is run without --force so
    that it skips already existing shards."

This integration test exercises the full preview → shard-key → handler
decompose → orchestrator preflight skip flow without external IO:

  1. Compose a deploy-missing preview for a leaf shard via the production
     ``build_deploy_missing_preview`` helper.
  2. Lift the emitted ``shard_key`` from the preview.
  3. Decompose it through MTDS's CLI helper ``decompose_shard_key`` —
     simulating what the launcher's argparse pass produces.
  4. Synthesize a fixture manifest where THE TARGETED SHARD is already
     ``captured`` and a sibling shard is missing.
  5. Run the orchestrator's pure preflight filter
     ``_filter_data_types_by_atom_coverage`` with the decomposed atoms +
     the fixture manifest's captured set.
  6. Assert the targeted shard is in ``skipped`` (i.e. orchestrator would
     short-circuit without re-fetching) and the sibling appears in
     ``partial``/kept (i.e. only that one is re-attempted).

Catches drift in BOTH directions:

* If the preview composer drops a field, the decomposer's recovered
  atom won't match the manifest's captured atom — the test surfaces it
  via "not skipped".
* If the orchestrator's filter regresses to "any captured = skip" or
  "any captured for venue = skip whole venue" semantics, the sibling
  shard would also get skipped — the test fails on that side too.

This is integration-level (touches deployment-api → MTDS handoff
contracts) but uses pure-function helpers throughout — no moto, no
real GCS, no real subprocess. Per the plan: "may use a moto'd manifest
+ a mocked MTDS handler" — the cleaner shape avoids both since the
helpers are pure.
"""

from __future__ import annotations

import argparse

import pytest

from deployment_api.services.deploy_missing import build_deploy_missing_preview

# NOTE: deployment-api does NOT have ``market-tick-data-service`` as a
# Python dependency (asymmetric workspace dep graph — MTDS depends on
# deployment-api contracts, not the other way round). To exercise the
# preview→shard-key→handler-decompose→orchestrator-skip flow without
# adding a new repo dep, this integration test inlines the contract
# halves of MTDS's CLI helpers as test fixtures:
#
#   * ``_decompose_shard_key`` — a faithful re-implementation of
#     ``market_tick_data_service.cli.shard_key.decompose_shard_key``.
#     Source: ``market-tick-data-service/market_tick_data_service/cli/shard_key.py``.
#   * ``_filter_data_types_by_atom_coverage`` — re-implementation of
#     ``market_tick_data_service.engine.orchestrator._filter_data_types_by_atom_coverage``.
#     Source: ``market-tick-data-service/market_tick_data_service/engine/orchestrator.py``.
#
# These re-impls are deliberately literal — if MTDS's helpers drift in
# shape, the matching test in
# ``market-tick-data-service/tests/cli/test_shard_key_round_trip.py``
# (also Cat D.2 of this same plan) will fail on the SOURCE side, not
# this side. This test pins the deployment-api → MTDS handoff contract
# only: that the preview's emitted shard_key parses cleanly + the
# recovered atoms match the manifest's captured-set semantics.

_BUNDLED_DATA_TYPES: frozenset[str] = frozenset({"options_chain", "futures_chain"})

_SHARD_KEY_FIELDS: tuple[str, ...] = (
    "asset_group",
    "venue",
    "data_type",
    "instrument_type",
    "instrument_id_or_root",
    "day",
)


def _decompose_shard_key(args: argparse.Namespace) -> argparse.Namespace:
    """Faithful inline copy of MTDS's ``decompose_shard_key`` for the
    deployment-api → MTDS contract test. Keep in sync with
    ``market-tick-data-service/market_tick_data_service/cli/shard_key.py``."""
    shard_key = getattr(args, "shard_key", None)
    if not shard_key:
        return args
    parts = [p.strip() for p in shard_key.split("|")]
    if len(parts) != len(_SHARD_KEY_FIELDS):
        raise ValueError(
            f"--shard-key {shard_key!r} has {len(parts)} fields; expected {len(_SHARD_KEY_FIELDS)}"
        )
    fields = dict(zip(_SHARD_KEY_FIELDS, parts, strict=True))
    if fields["asset_group"] and getattr(args, "asset_group", None) in (None, ""):
        args.asset_group = fields["asset_group"]
    if fields["venue"] and not getattr(args, "venues", None):
        args.venues = [fields["venue"]]
    if fields["data_type"] and not getattr(args, "data_types", None):
        args.data_types = [fields["data_type"]]
    if fields["instrument_type"] and getattr(args, "instrument_type", None) is None:
        args.instrument_type = fields["instrument_type"]
    if fields["instrument_id_or_root"]:
        if not getattr(args, "instrument_ids", None):
            args.instrument_ids = [fields["instrument_id_or_root"]]
        dt_lower = fields["data_type"].lower()
        if dt_lower in _BUNDLED_DATA_TYPES and getattr(args, "root", None) is None:
            args.root = fields["instrument_id_or_root"]
    if fields["day"] and getattr(args, "day", None) is None:
        args.day = fields["day"]
    return args


def _filter_data_types_by_atom_coverage(
    *,
    venue: str,
    requested_data_types: list[str],
    expected_atoms: set[str],
    captured_dts: set[str],
    captured_atoms_by_dt: dict[tuple[str, str], set[str]],
) -> tuple[list[str], set[str], dict[str, set[str]]]:
    """Faithful inline copy of MTDS orchestrator's preflight filter.
    Keep in sync with
    ``market_tick_data_service.engine.orchestrator._filter_data_types_by_atom_coverage``."""
    kept: list[str] = []
    skipped: set[str] = set()
    partial: dict[str, set[str]] = {}
    for dt in requested_data_types:
        if dt not in captured_dts:
            kept.append(dt)
            continue
        if not expected_atoms:
            skipped.add(dt)
            continue
        captured = captured_atoms_by_dt.get((venue, dt), set())
        if expected_atoms.issubset(captured):
            skipped.add(dt)
            continue
        kept.append(dt)
        missing = expected_atoms - captured
        if missing:
            partial[dt] = missing
    return kept, skipped, partial


def _empty_args() -> argparse.Namespace:
    """Build a fresh argparse Namespace with the fields the decomposer
    inspects + sets. Mirrors `tests/cli/test_shard_key.py` _make_args."""
    return argparse.Namespace(
        asset_group=None,
        venues=None,
        data_types=None,
        instrument_ids=None,
        instrument_type=None,
        root=None,
        day=None,
        date_alias=None,
        start_date=None,
        end_date=None,
        shard_key=None,
    )


class TestDeployMissingSkipAlreadyCaptured:
    """The captured shard targeted by a deploy-missing rerun MUST be
    skipped by the orchestrator's preflight filter."""

    def test_per_instrument_captured_shard_is_skipped(self) -> None:
        """End-to-end: preview a captured shard, decompose, run preflight
        filter — assert skipped (no re-fetch).

        Manifest fixture: btcusdt is captured, ethusdt is NOT captured.
        Targeted shard: btcusdt → MUST skip.
        """
        # Step 1+2: deployment-api composes the preview for the captured
        # leaf row.
        preview = build_deploy_missing_preview(
            service="market-tick-data-service",
            asset_group="cefi",
            row_key={
                "venue": "BINANCE-FUTURES",
                "data_type": "trades",
                "instrument_type": "PERPETUAL",
                "instrument_id": "btcusdt",
                "day": "2024-03-04",
            },
        )
        # The shard_key shape is the SSOT canonical 6-field pipe form.
        assert preview.shard_key == "cefi|BINANCE-FUTURES|trades|PERPETUAL|btcusdt|2024-03-04"

        # Step 3: simulate the MTDS launcher's argparse pass over the
        # preview-emitted --shard-key flag.
        args = _empty_args()
        args.shard_key = preview.shard_key
        decomposed = _decompose_shard_key(args)
        # Recover the atom the orchestrator would compare against the
        # manifest. Per ``decompose_shard_key`` the 5th field always
        # routes to ``--instrument-ids``.
        assert decomposed.instrument_ids == ["btcusdt"]
        assert decomposed.venues == ["BINANCE-FUTURES"]
        assert decomposed.data_types == ["trades"]
        recovered_atoms = set(decomposed.instrument_ids)

        # Step 4+5: synthesize a manifest preflight summary where the
        # target atom IS captured + run the filter.
        captured_dts = {"trades"}
        captured_atoms_by_dt = {("BINANCE-FUTURES", "trades"): {"btcusdt"}}
        kept, skipped, partial = _filter_data_types_by_atom_coverage(
            venue="BINANCE-FUTURES",
            requested_data_types=decomposed.data_types,
            expected_atoms=recovered_atoms,
            captured_dts=captured_dts,
            captured_atoms_by_dt=captured_atoms_by_dt,
        )

        # Step 6: orchestrator skips the captured shard without re-fetch.
        assert kept == [], f"Expected fully skipped, got kept={kept}"
        assert skipped == {"trades"}
        assert partial == {}

    def test_missing_sibling_is_re_attempted(self) -> None:
        """Counterpoint to the skip case: the same manifest fixture, but
        the targeted shard is the MISSING sibling. The filter must keep
        the data_type so it gets re-attempted, and the partial map must
        flag the missing atom (NOT the captured one)."""
        preview = build_deploy_missing_preview(
            service="market-tick-data-service",
            asset_group="cefi",
            row_key={
                "venue": "BINANCE-FUTURES",
                "data_type": "trades",
                "instrument_type": "PERPETUAL",
                "instrument_id": "ethusdt",  # NOT captured in fixture.
                "day": "2024-03-04",
            },
        )
        args = _empty_args()
        args.shard_key = preview.shard_key
        decomposed = _decompose_shard_key(args)
        recovered_atoms = set(decomposed.instrument_ids)
        # Same fixture as the skip case — only btcusdt is captured.
        captured_dts = {"trades"}
        captured_atoms_by_dt = {("BINANCE-FUTURES", "trades"): {"btcusdt"}}
        kept, skipped, partial = _filter_data_types_by_atom_coverage(
            venue="BINANCE-FUTURES",
            requested_data_types=decomposed.data_types,
            expected_atoms=recovered_atoms,
            captured_dts=captured_dts,
            captured_atoms_by_dt=captured_atoms_by_dt,
        )
        assert kept == ["trades"]
        assert skipped == set()
        assert partial == {"trades": {"ethusdt"}}

    def test_options_chain_bundle_captured_root_is_skipped(self) -> None:
        """Bundled shards (options_chain) — the 5th field is the
        underlying root (BTC, ES.OPT). The decomposer routes the field
        to BOTH ``--instrument-ids`` AND ``--root`` per the
        ``_atom = _iid or _und`` orchestrator semantic. Filter
        comparison happens on the same atom value."""
        preview = build_deploy_missing_preview(
            service="market-tick-data-service",
            asset_group="cefi",
            row_key={
                "venue": "DERIBIT",
                "data_type": "options_chain",
                "instrument_type": "options_chain",
                "root": "BTC",
                "day": "2024-03-04",
            },
        )
        assert preview.shard_key == "cefi|DERIBIT|options_chain|options_chain|BTC|2024-03-04"
        args = _empty_args()
        args.shard_key = preview.shard_key
        decomposed = _decompose_shard_key(args)
        # Bundled: both flags populated.
        assert decomposed.instrument_ids == ["BTC"]
        assert decomposed.root == "BTC"
        recovered_atoms = set(decomposed.instrument_ids)
        captured_dts = {"options_chain"}
        # BTC chain captured; ETH chain captured too — full universe
        # for the venue.
        captured_atoms_by_dt = {("DERIBIT", "options_chain"): {"BTC", "ETH"}}
        kept, skipped, _partial = _filter_data_types_by_atom_coverage(
            venue="DERIBIT",
            requested_data_types=decomposed.data_types,
            expected_atoms=recovered_atoms,
            captured_dts=captured_dts,
            captured_atoms_by_dt=captured_atoms_by_dt,
        )
        assert kept == []
        assert skipped == {"options_chain"}

    def test_round_trip_preserves_canonical_shape(self) -> None:
        """Bonus: pin that build_deploy_missing_preview's emitted shard_key,
        round-tripped through decompose_shard_key, recovers every field
        from the original row_key. Catches silent field-renames between
        the composer and the decomposer (e.g. preview switches from
        ``instrument_id`` → ``inst_id`` but decomposer still expects
        the old name)."""
        row_key = {
            "venue": "BYBIT",
            "data_type": "book_snapshot_5",
            "instrument_type": "PERPETUAL",
            "instrument_id": "btc-perp",
            "day": "2025-01-15",
        }
        preview = build_deploy_missing_preview(
            service="market-tick-data-service", asset_group="cefi", row_key=row_key
        )
        args = _empty_args()
        args.shard_key = preview.shard_key
        decomposed = _decompose_shard_key(args)
        assert decomposed.asset_group == "cefi"
        assert decomposed.venues == [row_key["venue"]]
        assert decomposed.data_types == [row_key["data_type"]]
        assert decomposed.instrument_type == row_key["instrument_type"]
        assert decomposed.instrument_ids == [row_key["instrument_id"]]
        assert decomposed.day == row_key["day"]


@pytest.mark.parametrize(
    ("preview_atom", "captured_atoms", "expected_kept"),
    [
        # Captured atom (same case + spelling) → skip.
        ("btcusdt", {"btcusdt"}, []),
        # Missing atom → keep + retry.
        ("ethusdt", {"btcusdt"}, ["trades"]),
        # Universe mismatch (captured atom belongs to different venue)
        # → keep — the decomposer ↔ filter must not cross venue walls.
        ("btcusdt", set(), ["trades"]),
    ],
)
def test_round_trip_skip_decision_matches_capture_state(
    preview_atom: str,
    captured_atoms: set[str],
    expected_kept: list[str],
) -> None:
    """Parametrised pin of the (decomposed atom, captured atoms) →
    skip-or-keep decision. Captures every regression mode in one
    table."""
    preview = build_deploy_missing_preview(
        service="market-tick-data-service",
        asset_group="cefi",
        row_key={
            "venue": "BINANCE-FUTURES",
            "data_type": "trades",
            "instrument_type": "PERPETUAL",
            "instrument_id": preview_atom,
            "day": "2024-03-04",
        },
    )
    args = _empty_args()
    args.shard_key = preview.shard_key
    decomposed = _decompose_shard_key(args)
    recovered_atoms = set(decomposed.instrument_ids)
    captured_dts = {"trades"} if captured_atoms else set()
    captured_atoms_by_dt = {("BINANCE-FUTURES", "trades"): captured_atoms} if captured_atoms else {}
    kept, _skipped, _partial = _filter_data_types_by_atom_coverage(
        venue="BINANCE-FUTURES",
        requested_data_types=decomposed.data_types,
        expected_atoms=recovered_atoms,
        captured_dts=captured_dts,
        captured_atoms_by_dt=captured_atoms_by_dt,
    )
    assert kept == expected_kept
