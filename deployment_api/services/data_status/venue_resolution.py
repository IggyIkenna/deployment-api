"""Venue start/expected-date resolution + MTDS honest-coverage apply.

Split out of the 6,663-line ``data_status_service.py`` god-module
(codex ratchet plan 2026-06-10). The facade module re-exports every
public + legacy-underscore name, so callers keep importing from
``deployment_api.services.data_status_service``.
"""

import logging
from collections.abc import Callable
from typing import cast

import pandas as pd
from unified_api_contracts import (
    VenueMapping,
)
from unified_api_contracts.sports import (
    get_transfer_windows_for_year,
)

logger = logging.getLogger(__name__)

from deployment_api.services.data_status.breakdowns_core import CoreBreakdownsMixin
from deployment_api.services.data_status.frame_utils import TRANSFER_COUNTRIES
from deployment_api.services.data_status.instrument_coverage import (
    build_cefi_is_instruments_provider,
)
from deployment_api.services.data_status.mtds import (
    MTDS_CATEGORY_META,
    canonicalise_defi_data_types,
    mtds_expected_venues,
    mtds_honest_coverage_for_bookmaker,
    mtds_honest_coverage_for_venue,
)


class VenueResolutionMixin(CoreBreakdownsMixin):
    """Venue starts, expected dates + per-venue breakdown assembly.

    The data_status mixins form a single linear inheritance chain
    (cli -> defi -> sports -> breakdowns_domain -> breakdowns_core ->
    venue_resolution -> coverage -> missing_shards -> manifest_category_builder ->
    manifest) so that
    every cross-group ``self._method`` reference resolves statically
    under basedpyright strict. ``DataStatusService`` composes the top of
    the chain and is the ONLY public entry point — import it from
    ``deployment_api.services.data_status_service`` (the facade).
    """

    @staticmethod
    def _resolve_venue_start(
        venue: str,
        venue_mapping: VenueMapping,
        is_instruments_service: bool,
    ) -> str | None:
        """Resolve the per-venue start date for the breakdown denominator.

        For instruments-service, prefer the per-venue instrument-discovery
        start (UAC SSOT, added 2026-05-06 in unified-api-contracts@89db18f)
        so HYPERLIQUID returns 2023-11-01 (when instruments first existed)
        instead of 2023-04-15 (when book_snapshot_5 archive starts). Other
        services keep using venue_start_date as before. Falls back through
        the ``venue:variant`` split for both lookups.
        """
        if is_instruments_service:
            vs = venue_mapping.get_instrument_discovery_start(venue)
            if not vs and ":" in venue:
                vs = venue_mapping.get_instrument_discovery_start(venue.split(":")[0])
            if vs:
                return vs
        vs = venue_mapping.get_venue_start_date(venue)
        if not vs and ":" in venue:
            vs = venue_mapping.get_venue_start_date(venue.split(":")[0])
        return vs

    @staticmethod
    def _has_data_type_column(filtered: pd.DataFrame) -> bool:
        """Whether ``filtered`` carries a non-empty ``data_type`` column (per-data-type manifest
        entries, e.g. market-tick-data-service)."""
        return bool(
            "data_type" in filtered.columns
            and not filtered["data_type"].isna().all()
            and (filtered["data_type"].str.len() > 0).any()
        )

    def _maybe_reference_expected_dates(
        self,
        filtered: pd.DataFrame,
        service: str,
        category: str,
        start_date: str,
        end_date: str,
        cloud: str,
    ) -> dict[str, set[str]]:
        """Reference-data-driven expected dates from the upstream service, for services in
        ``_REFERENCE_DRIVEN_SERVICES``; ``{}`` otherwise."""
        if service in self._REFERENCE_DRIVEN_SERVICES and category:
            return self._get_reference_expected_dates(category, start_date, end_date, service=service, cloud=cloud)
        return {}

    def _build_venue_breakdown(
        self,
        filtered: pd.DataFrame,
        start_date: str,
        end_date: str,
        venue_mapping: VenueMapping,
        cat_found: int,
        total_days: int,
        service: str = "",
        category: str = "",
        cloud: str = "gcp",
    ) -> tuple[dict[str, object], int, int]:
        """Build per-venue stats from filtered index data, incl. the data_type sub-dimension for
        services that write per-data-type manifest entries (e.g. market-tick-data-service).
        **Reference-data-driven denominator**: for market-data/feature services, the expected-date
        set comes from instruments-service ("dates where reference data exists for this venue"),
        NOT calendar trading days — coverage = "missing market data given available
        instruments/fixtures". For sports reference venues the denominator is the fixture calendar.
        """
        if "venue" not in filtered.columns or filtered.empty:
            return {}, cat_found, total_days

        has_data_type = self._has_data_type_column(filtered)
        ref_dates = self._maybe_reference_expected_dates(filtered, service, category, start_date, end_date, cloud)
        all_venues = [str(x) for x in filtered["venue"].unique() if str(x).strip()]  # pyright: ignore[reportAny]
        fixture_calendar = self._sports_reference_fixture_calendar(filtered, all_venues)

        venues_dict: dict[str, object] = {}
        venue_found_total = 0
        venue_expected_total = 0
        is_instruments_service = service == "instruments-service"
        for v in sorted(all_venues):
            venue_entry = self._build_one_venue_entry(
                filtered,
                v,
                start_date,
                end_date,
                fixture_calendar,
                ref_dates,
                venue_mapping,
                has_data_type,
                is_instruments_service,
                category,
                service,
            )
            venues_dict[v] = venue_entry
            venue_found_total += int(venue_entry["dates_found"])  # pyright: ignore[reportArgumentType]
            venue_expected_total += int(venue_entry["dates_expected"])  # pyright: ignore[reportArgumentType]
        return venues_dict, venue_found_total, venue_expected_total

    def _sports_reference_fixture_calendar(self, filtered: pd.DataFrame, all_venues: list[str]) -> set[str] | None:
        """Union of dates across all sports reference venues in this category — used as the
        expected-date set for each individual sports reference venue instead of all calendar days."""
        sports_ref_venues = [v for v in all_venues if self._is_sports_reference_venue(v)]
        if not sports_ref_venues:
            return None
        sports_mask = filtered["venue"].isin(sports_ref_venues)
        return {str(d) for d in filtered.loc[sports_mask, "date"].unique()}  # pyright: ignore[reportAny]

    def _build_one_venue_entry(
        self,
        filtered: pd.DataFrame,
        v: str,
        start_date: str,
        end_date: str,
        fixture_calendar: set[str] | None,
        ref_dates: dict[str, set[str]],
        venue_mapping: VenueMapping,
        has_data_type: bool,
        is_instruments_service: bool,
        category: str,
        service: str,
    ) -> dict[str, object]:
        """One venue's stats entry, extracted from ``_build_venue_breakdown``."""
        v_mask = filtered["venue"] == v
        v_df = filtered[v_mask]
        v_dates_all = {str(d) for d in v_df["date"].unique()}  # pyright: ignore[reportAny]
        vs = self._resolve_venue_start(v, venue_mapping, is_instruments_service)
        if not vs and v_dates_all:
            vs = min(v_dates_all)
        eff_start = max(start_date, vs) if vs else start_date

        v_all_dates = self._resolve_expected_dates(v, eff_start, end_date, fixture_calendar, ref_dates, venue_mapping)
        # Sparse sports entities return None → use found dates as expected (shows raw count, not
        # misleading % against full fixture calendar). Note: v_all_dates is always set[str], so no
        # None check is needed here.
        return self._build_single_venue_entry(
            v_df,
            v,
            vs,
            eff_start,
            end_date,
            v_dates_all,
            v_all_dates,
            has_data_type,
            venue_mapping,
            category=category,
            service=service,
        )

    def _apply_mtds_honest_coverage(
        self,
        venues_dict: dict[str, object],
        filtered: pd.DataFrame,
        category: str,
        start_date: str,
        end_date: str,
        venue_mapping: VenueMapping,
        cloud: str = "gcp",
        scope: str = "could_exist",
        service: str = "market-tick-data-service",
    ) -> tuple[dict[str, object], int, int]:
        """Override per-venue denominator with UAC-driven honest-coverage (SSOT:
        ``codex/02-data/mtds-data-source-coverage-matrix.md`` §7) — see
        :func:`_fold_honest_coverage_into_venue` for the per-venue fields rebuilt, and
        :func:`_mtds_honest_coverage_for_one_venue`/:func:`mtds_honest_coverage_for_venue` for the
        UAC ``(venue, data_type, date)`` shard-space math (``scope="mvp"``, CEFI
        instruments-provider). Also injects UAC-declared venues with ZERO manifest rows (else
        invisible — e.g. UPBIT shipped nothing for a quarter). ``service`` defaults to
        ``"market-tick-data-service"`` since production always passes one explicitly.
        """
        expected_venues = mtds_expected_venues(category, venue_mapping)
        # DEFI hyphenated-data_type canonicalisation (Phase 6e.1b residual; venue canon. done via UTL write-hook).
        if category.upper() == "DEFI" and not filtered.empty:
            filtered = canonicalise_defi_data_types(filtered)
        cefi_provider, cefi_windows, cefi_types = self._cefi_instruments_provider_for_category(category, cloud)

        # Start from the (possibly remapped) dict — preserves instrument_types/chains/
        # capture_status_counts sub-structures built by _build_venue_breakdown.
        new_venues: dict[str, object] = dict(venues_dict)
        # Union of manifest venues (post-normalisation for DEFI) + UAC-declared venues; a
        # UAC-declared venue absent from the manifest gets a zero-row entry.
        union_venues = set(new_venues.keys()) | set(expected_venues)
        is_sports = category.upper() == "SPORTS"
        total_found, total_expected = self._apply_honest_coverage_to_all_venues(
            new_venues,
            union_venues,
            filtered,
            category,
            start_date,
            end_date,
            venue_mapping,
            scope,
            service,
            is_sports,
            cefi_provider,
            cefi_windows,
            cefi_types,
        )
        return new_venues, total_found, total_expected

    def _apply_honest_coverage_to_all_venues(
        self,
        new_venues: dict[str, object],
        union_venues: set[str],
        filtered: pd.DataFrame,
        category: str,
        start_date: str,
        end_date: str,
        venue_mapping: VenueMapping,
        scope: str,
        service: str,
        is_sports: bool,
        cefi_provider: Callable[[str, str], list[str] | None] | None,
        cefi_windows: dict[str, tuple[str | None, str | None]],
        cefi_types: dict[str, str],
    ) -> tuple[int, int]:
        """Fold every venue's honest-coverage result into ``new_venues`` (mutated in place);
        returns the summed (found_shards, expected_shards) totals."""
        total_found = 0
        total_expected = 0
        for venue in sorted(union_venues):
            honest = self._mtds_honest_coverage_for_one_venue(
                filtered,
                venue,
                category,
                start_date,
                end_date,
                venue_mapping,
                scope,
                service,
                is_sports,
                cefi_provider,
                cefi_windows,
                cefi_types,
            )
            found_delta, expected_delta = self._fold_honest_coverage_into_venue(
                new_venues, venue, honest, category, venue_mapping
            )
            total_found += found_delta
            total_expected += expected_delta
        return total_found, total_expected

    @staticmethod
    def _cefi_instruments_provider_for_category(
        category: str, cloud: str
    ) -> tuple[Callable[[str, str], list[str] | None] | None, dict[str, tuple[str | None, str | None]], dict[str, str]]:
        """Live IS-catalog instruments_provider for CEFI, built ONCE so all per-instrument
        denominator lookups within this category call use the real universe (not the UAC MVP
        seed of 21 spot / 10 perp instruments). Fail-open: if IS is unavailable
        ``build_cefi_is_instruments_provider`` returns a provider that always yields None → UAC
        MVP seed is used. For all other asset_groups the provider stays ``None``."""
        if category.upper() != "CEFI":
            return None, {}, {}
        return build_cefi_is_instruments_provider(cloud)

    @staticmethod
    def _mtds_honest_coverage_for_one_venue(
        filtered: pd.DataFrame,
        venue: str,
        category: str,
        start_date: str,
        end_date: str,
        venue_mapping: VenueMapping,
        scope: str,
        service: str,
        is_sports: bool,
        cefi_provider: Callable[[str, str], list[str] | None] | None,
        cefi_windows: dict[str, tuple[str | None, str | None]],
        cefi_types: dict[str, str],
    ) -> dict[str, object]:
        """Dispatch to the bookmaker axis (SPORTS) or the generic per-(venue, data_type, date)
        honest-coverage function. Phase 6d: SPORTS uses a bookmaker x league x fixture-date axis
        — the generic function doesn't apply (no league dimension, no trading calendar); see
        :func:`mtds_honest_coverage_for_bookmaker`'s docstring."""
        if is_sports:
            return mtds_honest_coverage_for_bookmaker(filtered, venue, start_date, end_date)
        return mtds_honest_coverage_for_venue(
            filtered,
            venue,
            category,
            start_date,
            end_date,
            venue_mapping,
            instruments_provider=cefi_provider,
            instrument_windows=cefi_windows,
            scope=scope,
            instrument_types=cefi_types,
            service=service,
        )

    def _fold_honest_coverage_into_venue(
        self,
        new_venues: dict[str, object],
        venue: str,
        honest: dict[str, object],
        category: str,
        venue_mapping: VenueMapping,
    ) -> tuple[int, int]:
        """Merge one venue's honest-coverage result into ``new_venues`` (mutated in place, keyed
        by ``venue``); returns the (found_shards, expected_shards) delta for the category-level
        totals."""
        expected_shards = int(cast(int, honest["expected_shards"]))
        found_shards = int(cast(int, honest["found_shards"]))
        if expected_shards == 0:
            return self._fold_legacy_only_venue(new_venues, venue)

        venue_entry = self._base_venue_entry(new_venues.get(venue), venue, venue_mapping)
        # Override top-level found/expected with honest-coverage values.
        venue_entry["dates_found"] = found_shards
        venue_entry["dates_expected"] = expected_shards
        venue_entry["dates_expected_venue"] = expected_shards
        venue_entry["dates_missing"] = max(0, expected_shards - found_shards)
        venue_entry["completion_pct"] = min(round(found_shards / max(1, expected_shards) * 100, 2), 100.0)
        # Honest-coverage annotations — UI surfaces these as a second tab/tooltip alongside the
        # legacy ``data_types`` block.
        venue_entry["expected_data_types"] = honest["expected_data_types"]
        venue_entry["missing_data_types"] = honest["missing_data_types"]
        venue_entry["honest_data_types"] = honest["data_types"]
        venue_entry["honest_axis"] = str(MTDS_CATEGORY_META[category.upper()]["axis"])
        if "historical_coverage_gap" in honest:
            # MDPS-only annotation (see mtds_honest_coverage_for_venue's docstring) — propagated
            # onto the per-venue entry the UI actually reads. Absent entirely for MTDS.
            venue_entry["historical_coverage_gap"] = honest["historical_coverage_gap"]

        new_venues[venue] = venue_entry
        return found_shards, expected_shards

    @staticmethod
    def _fold_legacy_only_venue(new_venues: dict[str, object], venue: str) -> tuple[int, int]:
        """Venue not UAC-declared for MTDS (e.g. legacy DeFi names) — keep the existing entry
        untouched. Sum its legacy per-venue totals into the category header WITHOUT letting it
        pollute UAC-declared totals otherwise."""
        entry = new_venues.get(venue)
        if not isinstance(entry, dict):
            return 0, 0
        return (
            int(cast(int, entry.get("dates_found", 0))),  # pyright: ignore[reportUnknownMemberType]
            int(cast(int, entry.get("dates_expected", 0))),  # pyright: ignore[reportUnknownMemberType]
        )

    @staticmethod
    def _base_venue_entry(existing_entry: object, venue: str, venue_mapping: VenueMapping) -> dict[str, object]:
        """The existing per-venue dict (preserves instrument_types/chains/capture_status_counts
        sub-structures), or a zero-row placeholder for a UAC-declared venue with zero manifest
        rows — surfaced under ``missing_venues`` with 0% completion."""
        if isinstance(existing_entry, dict):
            return dict(existing_entry)  # pyright: ignore[reportUnknownArgumentType]
        _zero_counts = {
            "captured": 0,
            "empty_confirmed": 0,
            "attempted_failed": 0,
            "expected_unattempted_known_empty": 0,
            "expected_unattempted_pending_fetch": 0,
        }
        return {
            "dates_found": 0,
            "dates_expected": 0,
            "completion_pct": 0.0,
            "venue_start_date": venue_mapping.get_venue_start_date(venue),
            "capture_status_counts": _zero_counts,
            "counts": _zero_counts,
            "coverage": 0.0,
            "missing_dates": [],
            "dates_found_list": [],
            "dates_missing_list": [],
        }

    def _resolve_expected_dates(
        self,
        venue: str,
        eff_start: str,
        end_date: str,
        fixture_calendar: set[str] | None,
        ref_dates: dict[str, set[str]],
        venue_mapping: VenueMapping,
    ) -> set[str]:
        """Resolve the expected-date set for a venue.

        Priority:
        1. Transfermarkt → transfer-window dates only (open/close +/- grace)
        2. Understat XG → fixture calendar filtered to 6 covered leagues
        3. Sports reference venues → full fixture calendar
        4. Reference-driven services → instruments-service dates
        5. Fallback → calendar trading days
        """
        # Sparse sports entities: expected = found (show raw count, not
        # misleading % against full fixture calendar). Returns None to signal
        # "use found dates as denominator" to the caller.
        if self._is_sparse_sports_entity(venue):
            return None  # pyright: ignore[reportReturnType]
        if self._is_sports_reference_venue(venue) and fixture_calendar is not None:
            return {d for d in fixture_calendar if d >= eff_start}
        if venue in ref_dates:
            return {d for d in ref_dates[venue] if d >= eff_start}
        return set(venue_mapping.get_expected_trading_dates(venue, eff_start, end_date))

    def _resolve_understat_fixture_dates(
        self,
        start: str,
        end: str,
    ) -> set[str]:
        """Return expected dates for Understat XG.

        Understat XG covers only 6 leagues. We don't have a league-filtered
        fixture calendar available in this service, so we return None to signal
        that the expected dates should equal the found dates (self-referencing
        denominator). The completion shows as the raw date count rather than
        a misleading percentage against the full 38-league calendar.
        """
        # Return None — caller handles this as "use found dates as expected"
        return None  # pyright: ignore[reportReturnType]

    @staticmethod
    def _resolve_transfer_window_dates(start: str, end: str) -> set[str]:
        """Return dates where Transfermarkt reference data is expected.

        Uses the UAC transfer window calendar across all tracked countries.
        Only dates that fall within an open transfer window (or within a
        7-day grace period after close) are considered expected.

        This prevents the denominator from including mid-season dates
        where no transfer activity occurs.
        """
        from datetime import date as dt_date
        from datetime import timedelta

        start_d = dt_date.fromisoformat(start)
        end_d = dt_date.fromisoformat(end)

        expected: set[str] = set()
        grace_days = 7

        for year in range(start_d.year, end_d.year + 1):
            for country in TRANSFER_COUNTRIES:
                for window in get_transfer_windows_for_year(country, year):
                    # Include all dates within the window
                    w_start = max(window.open_date, start_d)
                    # Extend close by grace period for post-window data lag
                    w_end = min(window.close_date + timedelta(days=grace_days), end_d)
                    if w_start > w_end:
                        continue
                    d = w_start
                    while d <= w_end:
                        expected.add(d.isoformat())
                        d += timedelta(days=1)

        return expected
