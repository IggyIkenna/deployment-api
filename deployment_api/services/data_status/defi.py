"""DeFi venue canonicalisation / legacy-row filtering for the index.

Split out of the 6,663-line ``data_status_service.py`` god-module
(codex ratchet plan 2026-06-10). The facade module re-exports every
public + legacy-underscore name, so callers keep importing from
``deployment_api.services.data_status_service``.
"""

import logging
import re
from typing import ClassVar, cast

import pandas as pd

import deployment_api.services.data_status_service as _dss
from deployment_api.services.data_status_drilldown import (
    COMMODITY_BUCKET_TEMPLATE,
    PREDICTION_KIND_MAP,
    SERVICE_TO_KIND,
)

logger = logging.getLogger(__name__)

from deployment_api.services.data_status.cli import DataStatusCliMixin
from deployment_api.services.data_status.frame_utils import (
    promote_prediction_cqg_from_instrument_id,
)


class DefiStatusMixin(DataStatusCliMixin):
    """DeFi merged-index reads + canonical venue/chain filtering.

    The data_status mixins form a single linear inheritance chain
    (cli -> defi -> sports -> breakdowns_domain -> breakdowns_core ->
    venue_resolution -> coverage -> missing_shards -> manifest) so that
    every cross-group ``self._method`` reference resolves statically
    under basedpyright strict. ``DataStatusService`` composes the top of
    the chain and is the ONLY public entry point — import it from
    ``deployment_api.services.data_status_service`` (the facade).
    """

    _VENUE_ALIASES: ClassVar[dict[str, str]] = {
        "OKX": "OKX-SPOT",
        "COINBASE": "COINBASE-SPOT",
    }

    # ── Pre-canonicalisation DeFi venue aliases to drop from aggregation ──
    # The DeFi availability indices contain BOTH post-migration canonical rows
    # (``venue=AAVE_V3`` + ``chain=ETHEREUM``) AND pre-canonicalisation rows
    # (``venue=AAVE_V3-ETHEREUM`` + ``chain=""``) where the legacy format
    # combined protocol and chain into a single ``venue`` field. The legacy
    # rows inflate ``venue_dates_expected`` but have no matching shard data
    # under canonical paths, which drives DEFI category completion from ~99%
    # to ~40%.
    #
    # Deterministic filter, DEFI-scoped only:
    #   (category == 'defi') AND (venue contains '-') AND (chain is NaN/'')
    # → drop these rows from the aggregate.
    #
    # CeFi hyphenated venues (``BINANCE-FUTURES``, ``OKX-SWAP``, ...) live in
    # category=='cefi' and are therefore untouched by this filter.
    _DEFI_LEGACY_PROTOCOL_PREFIXES: ClassVar[tuple[str, ...]] = (
        "AAVE",
        "UNISWAP",
        "CURVE",
        "LIDO",
        "BALANCER",
        "COMPOUND",
        "SUSHISWAP",
        "PANCAKESWAP",
        "RAYDIUM",
        "MAKER",
        "YEARN",
        "CONVEX",
        "ROCKETPOOL",
        # Additional DeFi protocols observed in the DeFi availability index
        # with empty chain column (pre-canonicalisation alias rows).
        "CAMELOT",
        "GMX",
        "JITO",
        "ORCA",
        "MARINADE",
        "KAMINO",
        "MORPHO",
        "FLUID",
        "ETHENA",
        "ETHERFI",
        "EIGENLAYER",
        # Added 2026-04-19 after Playwright audit flagged AERODROME_V3-BASE
        # leaking into DeFi venue summary. Canonical form is
        # `AERODROME-BASE` + `chain='base'`; legacy is `AERODROME_V3-BASE` +
        # `chain=''`. Filter keys on empty chain so canonical rows survive.
        "AERODROME",
        # Velodrome mirrors Aerodrome (same Solidly v3 fork on Optimism).
        "VELODROME",
        # Drift perps — recent addition to UAC KNOWN_VENUE_TOKENS 2026-04-19.
        "DRIFT",
    )

    @classmethod
    def _is_legacy_defi_venue_row(cls, venue: object, chain: object) -> bool:
        """Detect pre-canonicalisation DeFi venue alias row.

        Legacy rows look like ``venue='AAVE_V3-ETHEREUM' chain=''`` — the
        protocol+chain pair was squashed into a single field before the
        canonical migration. New canonical rows are
        ``venue='AAVE_V3' chain='ETHEREUM'``.
        """
        if not isinstance(venue, str):
            return False
        if "-" not in venue:
            return False
        # Legacy format is uppercase letters/digits with an optional 'V<N>'
        # segment, then a hyphen, then the chain name. We treat rows as
        # legacy only when chain is empty/NaN AND the venue name starts
        # with a known DeFi protocol prefix. This avoids matching
        # fictional or CeFi-shaped hyphenated venues.
        chain_str = "" if chain is None else str(chain).strip()
        # ``pd.isna`` returns True for NaN but is not safe on arbitrary
        # objects; guard with the string coercion above, then also allow
        # the literal 'nan' produced by ``str(float('nan'))``.
        if chain_str and chain_str.lower() != "nan":
            return False
        head = venue.split("-", 1)[0]
        # Strip optional underscore + ``V<digits>`` tail so both canonical
        # (``AAVE_V3`` → ``AAVE``) and ghost (``AAVEV3`` → ``AAVE``) forms
        # normalise to the same root before the prefix lookup.
        root = re.sub(r"_?V\d+$", "", head)
        return root in cls._DEFI_LEGACY_PROTOCOL_PREFIXES

    # Per-service category scope (SSOT: deployment-ui-playwright-audit-checklist
    # Appendix A — Service x category matrix).  Services listed here ONLY apply
    # to the categories in the frozenset — categories outside this set are
    # omitted from the /turbo response (rather than rendered as misleading 0/0).
    # Services NOT listed (e.g. instruments-service, market-tick-data-service,
    # market-tick-data-handler, features-calendar-service) apply to ALL 5
    # categories (CEFI / TRADFI / DEFI / SPORTS / PREDICTION).
    _SERVICE_CATEGORY_RESTRICTIONS: ClassVar[dict[str, frozenset[str]]] = {
        "market-data-processing-service": frozenset({"CEFI", "TRADFI", "DEFI"}),
        "features-delta-one-service": frozenset({"CEFI", "TRADFI", "DEFI"}),
        "features-volatility-service": frozenset({"CEFI", "TRADFI", "DEFI"}),
        "features-multi-timeframe-service": frozenset({"CEFI", "TRADFI", "DEFI"}),
        "features-cross-instrument-service": frozenset({"CEFI", "TRADFI", "DEFI", "PREDICTION"}),
        "features-onchain-service": frozenset({"DEFI"}),
        "features-sports-service": frozenset({"SPORTS"}),
        "features-commodity-service": frozenset({"TRADFI"}),
        # features-calendar is intentionally NOT restricted: it serves all
        # asset_groups via a single shared bucket (``features-calendar-{pid}``),
        # surfaced under the ``SHARED`` pseudo-asset_group below rather than
        # duplicating identical numbers across every CEFI/TRADFI/DEFI/...
        "strategy-service": frozenset({"CEFI", "TRADFI", "DEFI"}),
        "execution-service": frozenset({"CEFI", "TRADFI", "DEFI"}),
    }

    # Services whose data is cross-asset by design (single shared bucket, no
    # per-asset-group sharding). The coverage-summary surfaces these under a
    # single ``SHARED`` pseudo-asset_group instead of duplicating the same
    # totals under every CEFI/TRADFI/DEFI/SPORTS/PREDICTION key. This matches
    # the user-facing taxonomy: calendar events are consumed by every
    # asset_group's strategies, so attributing them to one is misleading.
    _SHARED_BUCKET_SERVICES: ClassVar[frozenset[str]] = frozenset(
        {
            "features-calendar-service",
            # ml-service pools all asset groups under shared ml-*-artifacts buckets
            # (model checkpoints, training metrics, inference results). Consolidated
            # from ml-training-service + ml-inference-service (2026-05-21).
            "ml-service",
        }
    )

    # Per-service display axis for the ``latest_day_instruments`` breakdown.
    # The pricing→features pipeline uses ``venue`` (or ``data_type`` for sports;
    # see SPORTS axis swap in `_get_coverage_summary_sync`); experiment-based
    # services use the manifest column that identifies the experiment unit.
    # Falls through to the default (venue/data_type) if the named column is
    # missing or empty across all rows.
    _SERVICE_GROUP_AXIS_OVERRIDE: ClassVar[dict[str, str]] = {
        # ml-training-service + ml-inference-service consolidated into ml-service (2026-05-21)
        "ml-service": "model_family",
        "strategy-service": "strategy_id",
        "execution-service": "instruction_type",
    }

    # Categories whose bucket name doesn't follow the template pattern.
    # Only Phase-1 DeFi data types live in dedicated per-data-type buckets via
    # ``get_write_bucket_name(data_type)``. Phase-2 event-typed handlers
    # (liquidation_events, flash_loan_events, staking_yields, position_data,
    # token_transfers, bridge_events, governance_events, mev_events) and
    # eigenlayer_rewards write into the main ``market-data-tick-defi-{pid}``
    # bucket (via ``get_tick_data_bucket(asset_group="defi")``) so they're
    # picked up by the default ``SERVICE_TO_KIND`` → ``resolve_bucket_name`` path — no override needed.
    #
    # 2026-07-14: dropped `gas-fees`/`evm-defi`/`solana-defi`/`dex-pools`/`dex-swaps`/`liquidations`/
    # `lst-rates`/`oracle-prices`/`perp-funding`/`lending-indices` — all 10 dedicated buckets these
    # templates resolved to are now DELETED or mid-deletion (confirmed via `gcloud storage buckets
    # list`; deletions landed across PM plan gcs-bucket-estate-cleanup (2026-07-10) §5i/§5k,
    # defi-dedicated-bucket-shared-migration (2026-07-13), and bucket_estate_consolidation_to_sub100
    # (2026-07-13) item C for lending-indices specifically — all 10 kinds' data now lives in the
    # shared bucket, same as the already-migrated Phase-2 types above). This file was never updated
    # during any of those rounds, so `_read_defi_merged_index` was silently swallowing failed reads
    # against dead buckets (`except Exception: logger.debug(...)`) — not crashing, but the drilldown's
    # per-sub-dimension breakdown was permanently showing these as empty/no-data. `lending-indices`
    # was believed to be "the only survivor" as of the prior commit on this file (b5641cf, same day) —
    # that was wrong: its dedicated bucket's write path was already broken (a bare `"lending-indices"`
    # domain string falls through UTL's legacy fallback resolver to the flat, pre-shared-bucket-migration
    # name — see bucket_estate_consolidation_to_sub100_2026_07_13.md item C), the actual live data has
    # been in the shared bucket all along, and both dedicated buckets are now being retired.
    _BUCKET_CATEGORY_OVERRIDES: ClassVar[dict[tuple[str, str], str]] = {}

    # DeFi sub-dimension bucket keys for MTDS — merged into DEFI category.
    # Limited to Phase-1 sub-buckets; Phase-2 + eigenlayer-rewards already
    # land in the main DEFI bucket so they appear without enumeration here.
    # 2026-07-14: see `_BUCKET_CATEGORY_OVERRIDES` above — all Phase-1 sub-buckets (incl.
    # lending-indices) are gone; their data now merges in via the main DEFI bucket read like
    # every Phase-2 type.
    _MTDS_DEFI_SUB_DIMENSIONS: ClassVar[list[str]] = []

    def _read_defi_merged_index(self, service: str, cat: str, cloud: str = "gcp") -> pd.DataFrame:
        """Read availability index, merging sub-dimension buckets for MTDS DEFI.

        For market-tick-data-service + DEFI category, reads the main DEFI bucket
        AND all sub-dimension buckets (gas-fees, dex-swaps, etc.), concatenating
        them so venues from sub-dimensions appear under DEFI in the UI.

        Each row is tagged with ``_defi_source`` so the category builder can
        produce a per-sub-dimension breakdown.

        Phase 3 (data-status multi-axis drilldown) — after concatenation,
        filters merged rows to only those whose ``(venue, chain)`` pair is
        a canonical DeFi protocol per the UAC ``ALL_DEFI_VENUES``
        registry. Without this filter, sub-dimension buckets like
        ``oracle-prices-{pid}`` and ``perp-funding-{pid}`` (which carry
        oracle-price and perp-funding rows for CeFi venues that DEFI
        feeds reference, e.g. COINBASE-SPOT-as-oracle-source) silently
        leaked into the DEFI cell-grid as if they were DeFi protocols.
        """
        override = self._BUCKET_CATEGORY_OVERRIDES.get((service, cat.lower()))
        if override:
            main_bucket = override.format(pid=self.project_id, env=self.deployment_env_short)
        elif service == "features-commodity-service":
            main_bucket = COMMODITY_BUCKET_TEMPLATE.format(pid=self.project_id)
        else:
            kind = SERVICE_TO_KIND.get(service)
            if kind is None:
                return pd.DataFrame()
            ag = cat.lower() or None
            if ag == "shared":
                # SHARED pseudo-key (cross-asset services — features-calendar / ml-service)
                # has no per-asset-group bucket. Absent a (service, 'shared') override there
                # is nothing to read via the per-AG path → return empty (honest skip) rather
                # than calling resolve_bucket_name(asset_group='shared'), which raises
                # BucketNamingError and previously crashed the ENTIRE rollup sweep (phase-1
                # aborted before phase-2 → beta rollup blobs silently froze, 2026-06-16→17).
                return pd.DataFrame()
            if ag == "prediction" and PREDICTION_KIND_MAP.get(kind):
                # Prediction-SPECIAL single-bucket kind (its own KIND, resolved kind-only).
                main_bucket = _dss.resolve_bucket_name(cloud=cast(object, cloud), kind=PREDICTION_KIND_MAP[kind])  # pyright: ignore[reportArgumentType]
            else:
                # Normal per-asset_group kind — incl. a per-AG kind that merely serves
                # prediction as one of its asset_groups (e.g. features-cross-instrument, which
                # has NO PREDICTION_KIND_MAP entry). Resolve WITH asset_group; resolving such a
                # per-AG kind kind-only raised "asset_group= is required" (rollup SERVICE_FAILED).
                main_bucket = _dss.resolve_bucket_name(
                    cloud=cast(object, cloud),  # pyright: ignore[reportArgumentType]
                    kind=kind,
                    asset_group=cast(object, ag),  # pyright: ignore[reportArgumentType]
                )

        frames: list[pd.DataFrame] = []
        try:
            idx = _dss._read_index_cached(main_bucket)  # pyright: ignore[reportPrivateUsage]  # facade patch-point (late-bound)
            if not idx.empty:
                idx = idx.copy()
                idx["_defi_source"] = ""
                frames.append(idx)
        except Exception:
            logger.debug("No manifest index in %s", main_bucket)

        # Merge sub-dimension buckets for MTDS DEFI
        if service == "market-tick-data-service" and cat.lower() == "defi":
            for sub_dim in self._MTDS_DEFI_SUB_DIMENSIONS:
                sub_override = self._BUCKET_CATEGORY_OVERRIDES.get((service, sub_dim))
                if not sub_override:
                    continue
                sub_bucket = sub_override.format(pid=self.project_id, env=self.deployment_env_short)
                try:
                    sub_idx = _dss._read_index_cached(sub_bucket)  # pyright: ignore[reportPrivateUsage]  # facade patch-point (late-bound)
                    if not sub_idx.empty:
                        sub_idx = sub_idx.copy()
                        sub_idx["_defi_source"] = sub_dim
                        frames.append(sub_idx)
                except Exception:
                    logger.debug("No sub-dimension index in %s", sub_bucket)

        if not frames:
            return pd.DataFrame()
        merged = pd.concat(frames, ignore_index=True)

        # DeFi-venue whitelist filter — only applies to the MTDS-DEFI merge
        # path. Drops rows whose ``(venue, chain)`` pair is NOT in the UAC
        # canonical DeFi venue registry. Catches the COINBASE-SPOT-under-
        # ETHEREUM leak from oracle-prices / perp-funding sub-buckets that
        # legitimately carry CeFi venues but shouldn't appear in the DEFI
        # cell-grid.
        if service == "market-tick-data-service" and cat.lower() == "defi" and not merged.empty:
            merged = self._filter_to_canonical_defi_venues(merged)
            # Reconstruct the canonical ``PROTOCOL-CHAIN`` venue id from the
            # stored bare-protocol ``venue`` + ``chain`` columns. MUST run
            # AFTER the whitelist (which matches the bare ``(venue, chain)``
            # pair). Without this the honest-coverage drilldown matches raw
            # ``UNISWAP_V3`` against the UAC-canonical expected universe
            # (``UNISWAP_V3-ETHEREUM``) and finds nothing — the 3.2%-vs-97.6%
            # surface split. Also lets the per-chain genesis / protocol-launch
            # clip parse the chain out of the venue id.
            merged = self._canonicalise_defi_venue_column(merged)

        # v9 prediction: promote ``instrument_id`` → ``canonical_question_group`` so
        # the ``_apply_row_filters(canonical_question_group=...)`` and the turbo
        # breakdowns axis both work correctly against v9-canonical manifest rows.
        # Pre-v9 rows that already carry a non-empty ``canonical_question_group``
        # column are left untouched (the fill is conditioned on emptiness).
        if cat.lower() == "prediction" and not merged.empty:
            merged = promote_prediction_cqg_from_instrument_id(merged)
        return merged

    @classmethod
    def _allowed_defi_venue_chain_pairs(cls) -> frozenset[tuple[str, str]]:
        """Return the canonical ``(venue_upper, chain_upper)`` set for DeFi.

        Sourced from UAC ``ALL_DEFI_VENUES`` (canonical hyphenated form
        ``PROTOCOL-CHAIN``) AND ``LEGACY_DEFI_VENUE_ALIASES`` (raw
        underscore forms like ``AAVE_V3`` that pre-2026-04 manifests
        carry). Adding both shapes means the filter accepts canonical
        rows AND legacy-underscore rows that haven't yet been migrated.
        """
        from unified_api_contracts.registry import (
            ALL_DEFI_VENUES,
            LEGACY_DEFI_VENUE_ALIASES,
        )

        pairs: set[tuple[str, str]] = set()
        for entry in ALL_DEFI_VENUES:
            if "-" not in entry:
                continue
            protocol, chain = entry.rsplit("-", 1)
            pairs.add((protocol.upper(), chain.upper()))
        # Legacy: every entry in LEGACY_DEFI_VENUE_ALIASES maps an old
        # underscore form to its canonical no-underscore form. Accept the
        # legacy key alongside its canonical pair so unmigrated rows still
        # pass the whitelist.
        for legacy, canonical in LEGACY_DEFI_VENUE_ALIASES.items():
            if "-" not in canonical:
                continue
            _, chain = canonical.rsplit("-", 1)
            pairs.add((legacy.upper(), chain.upper()))
        return frozenset(pairs)

    def _filter_to_canonical_defi_venues(self, index: pd.DataFrame) -> pd.DataFrame:
        """Keep rows whose ``(venue, chain)`` is a canonical DeFi pair.

        Rows with a truly empty *venue* are kept (they cannot be classified
        here and are handled downstream).  Rows with a non-empty DeFi protocol
        venue but a blank *chain* are intentionally dropped: they are
        sub-bucket phantom rows (e.g. from oracle-prices / perp-funding shards
        that pre-date the chain split) that are NOT in the canonical
        ``(venue, chain)`` whitelist.  If kept, they reach
        :meth:`_canonicalise_defi_venue_column` where
        ``normalize_defi_venue(venue, None)`` returns the bare protocol string
        (e.g. ``TRADER_JOE_V2``), producing a duplicate bare-protocol entry
        alongside the canonical ``TRADER_JOE_V2-AVALANCHE`` entry.
        ``_filter_legacy_defi_rows`` only catches *hyphenated* venue names like
        ``AAVE_V3-ETHEREUM`` and does not close this gap.
        """
        if "venue" not in index.columns or "chain" not in index.columns:
            return index
        allowed = self._allowed_defi_venue_chain_pairs()
        venues = index["venue"].fillna("").astype(str).str.upper()
        chains = index["chain"].fillna("").astype(str).str.upper()
        # Keep rows with genuinely empty venue (no venue axis) — truly
        # ambiguous rows we cannot classify here.  Rows with a blank *chain*
        # but a real DeFi protocol venue are NOT passed through: they are not
        # in the whitelist and would produce bare-protocol duplicate entries.
        empty_venue = venues == ""
        in_whitelist = pd.Series(
            [(v, c) in allowed for v, c in zip(venues.tolist(), chains.tolist(), strict=True)],
            index=index.index,
        )
        keep = empty_venue | in_whitelist
        dropped = int((~keep).sum())
        if dropped > 0:
            logger.debug("DEFI venue whitelist dropped %d non-DeFi rows from merged index", dropped)
        return index.loc[keep].copy()

    def _canonicalise_defi_venue_column(self, index: pd.DataFrame) -> pd.DataFrame:
        """Rewrite the DeFi ``venue`` column to canonical ``PROTOCOL-CHAIN`` form.

        The manifest stores the venue split as a bare protocol id (``venue``)
        plus a separate ``chain`` column; the canonical UAC venue identity is
        the reconstructed pair (``UNISWAP_V3`` + ``ETHEREUM`` →
        ``UNISWAP_V3-ETHEREUM``). The coverage card reconstructs this implicitly
        via its ``(venue, chain)`` whitelist, but the honest-coverage drilldown
        matcher joins the raw ``venue`` string against the canonical expected
        universe — so without canonicalisation it finds zero overlap (the
        3.2%-vs-97.6% surface split for DeFi; 99.5% of captured DeFi rows carry
        a bare-protocol venue). Runs after :meth:`_filter_to_canonical_defi_venues`
        so the bare-pair whitelist is unaffected. Already-canonical rows pass
        through unchanged (``normalize_defi_venue`` is idempotent). Vectorised
        over the unique ``(venue, chain)`` pairs (~70 for DeFi).
        """
        if "venue" not in index.columns or "chain" not in index.columns or index.empty:
            return index
        from deployment_api.services.data_status.mtds import shared_venue_mapping  # noqa: qg-inside-import

        vm = shared_venue_mapping()
        pairs = index[["venue", "chain"]].drop_duplicates()
        mapping: dict[tuple[str, str], str] = {}
        for v, c in zip(pairs["venue"].astype(str), pairs["chain"].astype(str), strict=True):
            try:
                mapping[(v, c)] = str(vm.normalize_defi_venue(v, c if c.strip() else None))
            except Exception:
                mapping[(v, c)] = v
        index = index.copy()
        index["venue"] = [
            mapping.get((str(v), str(c)), str(v))
            for v, c in zip(index["venue"].tolist(), index["chain"].tolist(), strict=True)  # pyright: ignore[reportUnknownArgumentType,reportUnknownMemberType]
        ]
        return index

    def _filter_legacy_defi_rows(self, index: pd.DataFrame, cat: str) -> pd.DataFrame:
        """Drop pre-canonicalisation DeFi venue-alias rows.

        Rows like ``venue='AAVE_V3-ETHEREUM' chain=''`` predate the venue/chain
        split; same filter the per-shard rollup already applies.
        """
        if cat.lower() != "defi" or "venue" not in index.columns or index.empty:
            return index
        chain_series = index["chain"] if "chain" in index.columns else pd.Series([""] * len(index), index=index.index)
        legacy_mask = [
            self._is_legacy_defi_venue_row(v, c)  # pyright: ignore[reportAny]
            for v, c in zip(index["venue"].tolist(), chain_series.tolist(), strict=True)  # pyright: ignore[reportAny]
        ]
        if not any(legacy_mask):
            return index
        dropped = int(sum(legacy_mask))
        logger.debug(
            "coverage-summary: filtered %d legacy DeFi venue-alias rows from %s",
            dropped,
            cat,
        )
        return index.loc[[not m for m in legacy_mask]].copy()
